import pkg_resources
import os
import json
import socket
import socketserver
from threading import Thread
import py4j
import pyspark

from hail.utils.java import *
from hail.expr.types import dtype
from hail.expr.table_type import *
from hail.expr.matrix_type import *
from hail.expr.blockmatrix_type import *
from hail.ir.renderer import CSERenderer, Renderer
from hail.table import Table
from hail.matrixtable import MatrixTable

from .backend import Backend


def handle_java_exception(f):
    def deco(*args, **kwargs):
        import pyspark
        try:
            return f(*args, **kwargs)
        except py4j.protocol.Py4JJavaError as e:
            s = e.java_exception.toString()

            # py4j catches NoSuchElementExceptions to stop array iteration
            if s.startswith('java.util.NoSuchElementException'):
                raise

            tpl = Env.jutils().handleForPython(e.java_exception)
            deepest, full = tpl._1(), tpl._2()
            raise FatalError('%s\n\nJava stack trace:\n%s\n'
                             'Hail version: %s\n'
                             'Error summary: %s' % (deepest, full, hail.__version__, deepest)) from None
        except pyspark.sql.utils.CapturedException as e:
            raise FatalError('%s\n\nJava stack trace:\n%s\n'
                             'Hail version: %s\n'
                             'Error summary: %s' % (e.desc, e.stackTrace, hail.__version__, e.desc)) from None

    return deco


_installed = False
_original = None


def install_exception_handler():
    global _installed
    global _original
    if not _installed:
        _original = py4j.protocol.get_return_value
        _installed = True
        # The original `get_return_value` is not patched, it's idempotent.
        patched = handle_java_exception(_original)
        # only patch the one used in py4j.java_gateway (call Java API)
        py4j.java_gateway.get_return_value = patched


def uninstall_exception_handler():
    global _installed
    global _original
    if _installed:
        _installed = False
        py4j.protocol.get_return_value = _original


class LoggingTCPHandler(socketserver.StreamRequestHandler):
    def handle(self):
        for line in self.rfile:
            sys.stderr.write(line.decode("ISO-8859-1"))


class SimpleServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address, handler_class):
        socketserver.TCPServer.__init__(self, server_address, handler_class)


def connect_logger(utils_package_object, host, port):
    """
    This method starts a simple server which listens on a port for a
    client to connect and start writing messages. Whenever a message
    is received, it is written to sys.stderr. The server is run in
    a daemon thread from the caller, which is killed when the caller
    thread dies.

    If the socket is in use, then the server tries to listen on the
    next port (port + 1). After 25 tries, it gives up.

    :param str host: Hostname for server.
    :param int port: Port to listen on.
    """
    server = None
    tries = 0
    max_tries = 25
    while not server:
        try:
            server = SimpleServer((host, port), LoggingTCPHandler)
        except socket.error:
            port += 1
            tries += 1

            if tries >= max_tries:
                sys.stderr.write(
                    'WARNING: Could not find a free port for logger, maximum retries {} exceeded.'.format(max_tries))
                return

    t = Thread(target=server.serve_forever, args=())

    # The thread should be a daemon so that it shuts down when the parent thread is killed
    t.daemon = True

    t.start()
    utils_package_object.addSocketAppender(host, port)


class SparkBackend(Backend):
    def __init__(self, idempotent, sc, spark_conf, app_name, master,
                 local, log, quiet, append, min_block_size,
                 branching_factor, tmpdir, local_tmpdir, skip_logging_configuration, optimizer_iterations):
        if pkg_resources.resource_exists(__name__, "hail-all-spark.jar"):
            hail_jar_path = pkg_resources.resource_filename(__name__, "hail-all-spark.jar")
            assert os.path.exists(hail_jar_path), f'{hail_jar_path} does not exist'
            conf = pyspark.SparkConf()

            base_conf = spark_conf or {}
            for k, v in base_conf.items():
                conf.set(k, v)

            jars = [hail_jar_path]

            if os.environ.get('HAIL_SPARK_MONITOR'):
                import sparkmonitor
                jars.append(os.path.join(os.path.dirname(sparkmonitor.__file__), 'listener.jar'))
                conf.set("spark.extraListeners", "sparkmonitor.listener.JupyterSparkMonitorListener")

            conf.set('spark.jars', ','.join(jars))
            conf.set('spark.driver.extraClassPath', ','.join(jars))
            conf.set('spark.executor.extraClassPath', './hail-all-spark.jar')
            if sc is None:
                pyspark.SparkContext._ensure_initialized(conf=conf)
            elif not quiet:
                sys.stderr.write(
                    'pip-installed Hail requires additional configuration options in Spark referring\n'
                    '  to the path to the Hail Python module directory HAIL_DIR,\n'
                    '  e.g. /path/to/python/site-packages/hail:\n'
                    '    spark.jars=HAIL_DIR/hail-all-spark.jar\n'
                    '    spark.driver.extraClassPath=HAIL_DIR/hail-all-spark.jar\n'
                    '    spark.executor.extraClassPath=./hail-all-spark.jar')
        else:
            pyspark.SparkContext._ensure_initialized()

        self._gateway = pyspark.SparkContext._gateway
        self._jvm = pyspark.SparkContext._jvm

        hail_package = getattr(self._jvm, 'is').hail

        self._hail_package = hail_package
        self._utils_package_object = scala_package_object(hail_package.utils)

        jsc = sc._jsc.sc() if sc else None

        if idempotent:
            self._jbackend = hail_package.backend.spark.SparkBackend.getOrCreate(
                jsc, app_name, master, local, True, min_block_size, tmpdir, local_tmpdir)
            self._jhc = hail_package.HailContext.getOrCreate(
                self._jbackend, log, True, append, branching_factor, skip_logging_configuration, optimizer_iterations)
        else:
            self._jbackend = hail_package.backend.spark.SparkBackend.apply(
                jsc, app_name, master, local, True, min_block_size, tmpdir, local_tmpdir)
            self._jhc = hail_package.HailContext.apply(
                self._jbackend, log, True, append, branching_factor, skip_logging_configuration, optimizer_iterations)

        self._jsc = self._jbackend.sc()
        if sc:
            self.sc = sc
        else:
            self.sc = pyspark.SparkContext(gateway=self._gateway, jsc=self._jvm.JavaSparkContext(self._jsc))
        self._jspark_session = self._jbackend.sparkSession()
        self._spark_session = pyspark.sql.SparkSession(self.sc, self._jspark_session)

        # This has to go after creating the SparkSession. Unclear why.
        # Maybe it does its own patch?
        install_exception_handler()

        from hail.context import version

        py_version = version()
        jar_version = self._jhc.version()
        if jar_version != py_version:
            raise RuntimeError(f"Hail version mismatch between JAR and Python library\n"
                               f"  JAR:    {jar_version}\n"
                               f"  Python: {py_version}")

        self._fs = None

        if not quiet:
            sys.stderr.write('Running on Apache Spark version {}\n'.format(self.sc.version))
            if self._jsc.uiWebUrl().isDefined():
                sys.stderr.write('SparkUI available at {}\n'.format(self._jsc.uiWebUrl().get()))

            connect_logger(self._utils_package_object, 'localhost', 12888)

            self._jbackend.startProgressBar()

    def jvm(self):
        return self._jvm

    def hail_package(self):
        return self._hail_package

    def utils_package_object(self):
        return self._utils_package_object

    def stop(self):
        self._jhc.stop()
        self._jhc = None
        self.sc.stop()
        self.sc = None
        uninstall_exception_handler()

    def _parse_value_ir(self, code, ref_map={}, ir_map={}):
        return self._jbackend.parse_value_ir(
            code,
            {k: t._parsable_string() for k, t in ref_map.items()},
            ir_map)

    def _parse_table_ir(self, code, ref_map={}, ir_map={}):
        return self._jbackend.parse_table_ir(code, ref_map, ir_map)

    def _parse_matrix_ir(self, code, ref_map={}, ir_map={}):
        return self._jbackend.parse_matrix_ir(code, ref_map, ir_map)

    def _parse_blockmatrix_ir(self, code, ref_map={}, ir_map={}):
        return self._jbackend.parse_blockmatrix_ir(code, ref_map, ir_map)

    @property
    def fs(self):
        if self._fs is None:
            from hail.fs.hadoop_fs import HadoopFS
            self._fs = HadoopFS(self._utils_package_object, self._jbackend.fs())
        return self._fs

    def _to_java_ir(self, ir, parse):
        if not hasattr(ir, '_jir'):
            r = CSERenderer(stop_at_jir=True)
            # FIXME parse should be static
            ir._jir = parse(r(ir), ir_map=r.jirs)
        return ir._jir

    def _to_java_value_ir(self, ir):
        return self._to_java_ir(ir, self._parse_value_ir)

    def _to_java_table_ir(self, ir):
        return self._to_java_ir(ir, self._parse_table_ir)

    def _to_java_matrix_ir(self, ir):
        return self._to_java_ir(ir, self._parse_matrix_ir)

    def _to_java_blockmatrix_ir(self, ir):
        return self._to_java_ir(ir, self._parse_blockmatrix_ir)

    def execute(self, ir, timed=False):
        jir = self._to_java_value_ir(ir)
        result = json.loads(self._jhc.backend().executeJSON(jir))
        value = ir.typ._from_json(result['value'])
        timings = result['timings']

        return (value, timings) if timed else value

    def value_type(self, ir):
        jir = self._to_java_value_ir(ir)
        return dtype(jir.typ().toString())

    def table_type(self, tir):
        jir = self._to_java_table_ir(tir)
        return ttable._from_java(jir.typ())

    def matrix_type(self, mir):
        jir = self._to_java_matrix_ir(mir)
        return tmatrix._from_java(jir.typ())

    def persist_table(self, t, storage_level):
        return Table._from_java(self._jbackend.pyPersistTable(storage_level, self._to_java_table_ir(t._tir)))

    def unpersist_table(self, t):
        return Table._from_java(self._to_java_table_ir(t._tir).pyUnpersist())

    def persist_matrix_table(self, mt, storage_level):
        return MatrixTable._from_java(self._jbackend.pyPersistMatrix(storage_level, self._to_java_matrix_ir(mt._mir)))

    def unpersist_matrix_table(self, mt):
        return MatrixTable._from_java(self._to_java_matrix_ir(mt._mir).pyUnpersist())

    def blockmatrix_type(self, bmir):
        jir = self._to_java_blockmatrix_ir(bmir)
        return tblockmatrix._from_java(jir.typ())

    def from_spark(self, df, key):
        return Table._from_java(self._jbackend.pyFromDF(df._jdf, key))

    def to_spark(self, t, flatten):
        t = t.expand_types()
        if flatten:
            t = t.flatten()
        return pyspark.sql.DataFrame(self._jbackend.pyToDF(self._to_java_table_ir(t._tir)), Env.spark_session()._wrapped)

    def to_pandas(self, t, flatten):
        return self.to_spark(t, flatten).toPandas()

    def from_pandas(self, df, key):
        return Table.from_spark(Env.spark_session().createDataFrame(df), key)

    def add_reference(self, config):
        Env.hail().variant.ReferenceGenome.fromJSON(json.dumps(config))

    def load_references_from_dataset(self, path):
        return json.loads(Env.hail().variant.ReferenceGenome.fromHailDataset(self.fs._jfs, path))

    def from_fasta_file(self, name, fasta_file, index_file, x_contigs, y_contigs, mt_contigs, par):
        self._jbackend.pyFromFASTAFile(
            name, fasta_file, index_file, x_contigs, y_contigs, mt_contigs, par)

    def remove_reference(self, name):
        Env.hail().variant.ReferenceGenome.removeReference(name)

    def get_reference(self, name):
        return json.loads(Env.hail().variant.ReferenceGenome.getReference(name).toJSONString())

    def add_sequence(self, name, fasta_file, index_file):
        self._jbackend.pyAddSequence(name, fasta_file, index_file)

    def remove_sequence(self, name):
        scala_object(Env.hail().variant, 'ReferenceGenome').removeSequence(name)

    def add_liftover(self, name, chain_file, dest_reference_genome):
        self._jbackend.pyReferenceAddLiftover(name, chain_file, dest_reference_genome)

    def remove_liftover(self, name, dest_reference_genome):
        scala_object(Env.hail().variant, 'ReferenceGenome').referenceRemoveLiftover(
            name, dest_reference_genome)

    def parse_vcf_metadata(self, path):
        return json.loads(self._jhc.pyParseVCFMetadataJSON(self.fs._jfs, path))

    def index_bgen(self, files, index_file_map, rg, contig_recoding, skip_invalid_loci):
        self._jbackend.pyIndexBgen(files, index_file_map, rg, contig_recoding, skip_invalid_loci)
