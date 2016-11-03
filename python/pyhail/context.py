import pyspark

from pyhail.dataset import VariantDataset
from pyhail.java import jarray, scala_object

class HailContext:
    def __init__(self, sc):
        self.sc = sc
        
        self.gateway = sc._gateway
        self.jvm = sc._jvm
        
        # sc._jsc is JavaObject JavaSparkContext
        self.jsc = sc._jsc.sc()
        
        self.jsqlContext = sc._jvm.SQLContext(self.jsc)
        
        self.sqlContext = pyspark.sql.SQLContext(sc, self.jsqlContext)
        
        self.jsc.hadoopConfiguration().set(
            'io.compression.codecs',
            'org.apache.hadoop.io.compress.DefaultCodec,org.broadinstitute.hail.io.compress.BGzipCodec,org.apache.hadoop.io.compress.GzipCodec')
        
        logger = sc._jvm.org.apache.log4j
        logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
        logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
        
    def to_jarray(self, jtype, plist):
        n = len(plist)
        jarr = self.gateway.new_array(jtype, n)
        for i, s in enumerate(plist):
            jarr[i] = s
        return jarr
    
    def vds_state(self, vds):
        return VariantDataset(self, self.jvm.org.broadinstitute.hail.driver.State(self.jsc, self.jsqlContext, vds))
    
    def initial_state(self):
        return VariantDataset(
            self,
            self.jvm.org.broadinstitute.hail.driver.State(self.jsc,
                                                          self.jsqlContext,
                                                          None,
                                                          scala_object(self.jvm.scala.collection.immutable, 'Map').empty()))
    
    def _run_command(self, state, args):
        jargs = jarray(self.gateway, self.jvm.java.lang.String, args)
        cmdargs = self.jvm.org.broadinstitute.hail.driver.ToplevelCommands.lookup(jargs)
        cmd = cmdargs._1()
        cargs = cmdargs._2()
        options = cmd.parseArgs(cargs)
        result = cmd.run(state.jstate, options)
        return VariantDataset(self, result)
    
    def fam_summary(input, output):
        pargs = ["famsummary", "-f", input, "-o", output]
        return _run_command(self, pargs)

    def grep(*args, **kwargs):
        pargs = ["grep"]
        
        max_count = kwargs.pop('max_count', False)
        if max_count:
            pargs.append('--max-count')
            pargs.append(str(max_count))
        
        return _run_command(self, pargs)
    
    def import_annotations_table(self, *args, **kwargs):
        pargs = ["importannotationstable"]
        
        variant_expr = kwargs.pop('variant_expr')
        pargs.append('--variant-expr')
        pargs.append(variant_expr)

        code = kwargs.pop('code', False)
        if code:
            pargs.append('--code')
            pargs.append(code)

        npartition = kwargs.pop('npartition', None)
        if npartition:
            pargs.append('--npartition')
            pargs.append(npartition)
        
        for arg in args:
            pargs.append(arg)
        return self._run_command(self.initial_state(), pargs)

    def import_bgen(self, *args, **kwargs):
        pargs = ["importbgen"]

        no_compress = kwargs.pop('no_compress', False)
        if no_compress:
            pargs.append('--no-compress')
        
        samplefile = kwargs.pop('samplefile', None)
        if samplefile:
            pargs.append('--samplefile')
            pargs.append(samplefile)
        
        npartition = kwargs.pop('npartition', None)
        if npartition:
            pargs.append('--npartition')
            pargs.append(npartition)

        tolerance = kwargs.pop('tolerance', None)
        if tolerance:
            pargs.append('--tolerance')
            pargs.append(npartition)
            
        return self._run_command(self.initial_state(), args)

    def import_gen(self, *args, **kwargs):
        pargs = ["importgen"]

        no_compress = kwargs.pop('no_compress', False)
        if no_compress:
            pargs.append('--no-compress')
        
        samplefile = kwargs.pop('samplefile', None)
        if samplefile:
            pargs.append('--samplefile')
            pargs.append(samplefile)
        
        chromosome = kwargs.pop('chromosome', None)
        if chromosome:
            pargs.append('--chromosome')
            pargs.append(npartition)

        npartition = kwargs.pop('npartition', None)
        if npartition:
            pargs.append('--npartition')
            pargs.append(npartition)
        
        tolerance = kwargs.pop('tolerance', None)
        if tolerance:
            pargs.append('--tolerance')
            pargs.append(npartition)
            
        return self._run_command(self.initial_state(), pargs)

    def import_plink(self, *args, **kwargs):
        pargs = ["importplink"]

        bfile = kwargs.pop('bfile', False)
        if bfile:
            pargs.append('--bfile')
            pargs.append(bfile)

        bed = kwargs.pop('bed', False)
        if bed:
            pargs.append('--bed')
            pargs.append(bed)

        bim = kwargs.pop('bim', False)
        if bim:
            pargs.append('--bim')
            pargs.append(bim)

        fam = kwargs.pop('fam', False)
        if fam:
            pargs.append('--fam')
            pargs.append(fam)
            
        npartition = kwargs.pop('npartition', None)
        if npartition:
            pargs.append('--npartition')
            pargs.append(npartition)
        
        tolerance = kwargs.pop('tolerance', None)
        if tolerance:
            pargs.append('--tolerance')
            pargs.append(npartition)

        quantpheno = kwargs.pop('quantpheno', False)
        if quantpheno:
            pargs.append('--quantpheno')

        missing = kwargs.pop('missing', None)
        if missing:
            pargs.append('--missing')
            pargs.append(missing)

        delimiter = kwargs.pop('delimiter', None)
        if delimiter:
            pargs.append('--delimiter')
            pargs.append(delimiter)
        
        return self._run_command(self.initial_state(), pargs)
    
    def read(self, vds_path, skip_genotypes = False):
        pargs = ["read"]
        if (skip_genotypes):
            pargs.append("--skip-genotypes")
        pargs.append("-i")
        pargs.append(vds_path)
        return self._run_command(self.initial_state(), pargs)

    def import_vcf(self, *args, **kwargs):
        pargs = ["importvcf"]
        no_compress = kwargs.pop('no_compress', False)
        if no_compress:
            pargs.append('--no-compress')
        
        force = kwargs.pop('force', False)
        if force:
            pargs.append('--force')
            
        force_bgz = kwargs.pop('force_bgz', False)
        if force_bgz:
            pargs.append('--force-bgz')
            
        header_file = kwargs.pop('header_file', None)
        if header_file:
            pargs.append('--header-file')
            pargs.append(header_file)
            
        npartition = kwargs.pop('npartition', None)
        if npartition:
            pargs.append('--npartition')
            pargs.append(str(n_partitions))
            
        pp_as_pl = kwargs.pop('pp_as_pl', False)
        if pp_as_pl:
            pargs.append('--pp-as-pl')
            
        skip_bad_ad = kwargs.pop('skip_bad_ad', False)
        if skip_bad_ad:
            pargs.append('--skip-bad-ad')
            
        skip_genotypes = kwargs.pop('skip_genotypes', False)
        if skip_genotypes:
            pargs.append('--skip-genotypes')

        store_gq = kwargs.pop('skip_genotypes', False)
        if store_gq:
            pargs.append('--store-gq')
            
        for arg in args:
            pargs.append(arg)

        return self._run_command(self.initial_state(), pargs)

    def index_bgen(*args):
        pargs = ["indexbgen"]
        for arg in args:
            pargs.append(arg)
        return self._run_command(self.initial_state(), pargs)
