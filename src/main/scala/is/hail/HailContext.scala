package is.hail

import java.io.InputStream
import java.util.Properties

import is.hail.annotations._
import is.hail.expr.types._
import is.hail.expr.{EvalContext, Parser, ir}
import is.hail.io.{CodecSpec, LoadMatrix}
import is.hail.io.bgen.LoadBgen
import is.hail.io.gen.LoadGen
import is.hail.io.plink.{FamFileConfig, LoadPlink}
import is.hail.io.vcf._
import is.hail.rvd.RVDContext
import is.hail.table.Table
import is.hail.sparkextras.ContextRDD
import is.hail.stats.{BaldingNicholsModel, Distribution, UniformDist}
import is.hail.utils.{log, _}
import is.hail.variant.{MatrixTable, ReferenceGenome, VSMSubgen}
import org.apache.hadoop
import org.apache.log4j.{ConsoleAppender, LogManager, PatternLayout, PropertyConfigurator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark._
import org.apache.spark.executor.InputMetrics

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.reflect.ClassTag

case class FilePartition(index: Int, file: String) extends Partition

object HailContext {

  val tera: Long = 1024L * 1024L * 1024L * 1024L

  val logFormat: String = "%d{yyyy-MM-dd HH:mm:ss} %c{1}: %p: %m%n"

  def configureAndCreateSparkContext(appName: String, master: Option[String],
    local: String, blockSize: Long): SparkContext = {
    require(blockSize >= 0)
    require(is.hail.HAIL_SPARK_VERSION == org.apache.spark.SPARK_VERSION,
      s"""This Hail JAR was compiled for Spark ${ is.hail.HAIL_SPARK_VERSION },
         |  but the version of Spark available at runtime is ${ org.apache.spark.SPARK_VERSION }.""".stripMargin)

    val conf = new SparkConf().setAppName(appName)

    master match {
      case Some(m) =>
        conf.setMaster(m)
      case None =>
        if (!conf.contains("spark.master"))
          conf.setMaster(local)
    }

    conf.set("spark.logConf", "true")
    conf.set("spark.ui.showConsoleProgress", "false")

    conf.set(
      "spark.hadoop.io.compression.codecs",
      "org.apache.hadoop.io.compress.DefaultCodec," +
        "is.hail.io.compress.BGzipCodec," +
        "org.apache.hadoop.io.compress.GzipCodec")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator")

    conf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", (blockSize * 1024L * 1024L).toString)

    // load additional Spark properties from HAIL_SPARK_PROPERTIES
    val hailSparkProperties = System.getenv("HAIL_SPARK_PROPERTIES")
    if (hailSparkProperties != null) {
      hailSparkProperties
        .split(",")
        .foreach { p =>
          p.split("=") match {
            case Array(k, v) =>
              log.info(s"set Spark property from HAIL_SPARK_PROPERTIES: $k=$v")
              conf.set(k, v)
            case _ =>
              warn(s"invalid key-value property pair in HAIL_SPARK_PROPERTIES: $p")
          }
        }
    }

    val sc = new SparkContext(conf)
    sc
  }

  def checkSparkConfiguration(sc: SparkContext) {
    val conf = sc.getConf

    val problems = new ArrayBuffer[String]

    val serializer = conf.get("spark.serializer")
    val kryoSerializer = "org.apache.spark.serializer.KryoSerializer"
    if (serializer != kryoSerializer)
      problems += s"Invalid configuration property spark.serializer: required $kryoSerializer.  Found: $serializer."

    if (!conf.getOption("spark.kryo.registrator").exists(_.split(",").contains("is.hail.kryo.HailKryoRegistrator")))
      problems += s"Invalid config parameter: spark.kryo.registrator must include is.hail.kryo.HailKryoRegistrator." +
        s"Found ${ conf.getOption("spark.kryo.registrator").getOrElse("empty parameter.") }"

    if (problems.nonEmpty)
      fatal(
        s"""Found problems with SparkContext configuration:
           |  ${ problems.mkString("\n  ") }""".stripMargin)
  }

  def configureLogging(logFile: String, quiet: Boolean, append: Boolean) {
    val logProps = new Properties()

    logProps.put("log4j.rootLogger", "INFO, logfile")
    logProps.put("log4j.appender.logfile", "org.apache.log4j.FileAppender")
    logProps.put("log4j.appender.logfile.append", append.toString)
    logProps.put("log4j.appender.logfile.file", logFile)
    logProps.put("log4j.appender.logfile.threshold", "INFO")
    logProps.put("log4j.appender.logfile.layout", "org.apache.log4j.PatternLayout")
    logProps.put("log4j.appender.logfile.layout.ConversionPattern", HailContext.logFormat)

    LogManager.resetConfiguration()
    PropertyConfigurator.configure(logProps)

    if (!quiet)
      consoleLog.addAppender(new ConsoleAppender(new PatternLayout(HailContext.logFormat), "System.err"))
  }

  def apply(sc: SparkContext = null,
    appName: String = "Hail",
    master: Option[String] = None,
    local: String = "local[*]",
    logFile: String = "hail.log",
    quiet: Boolean = false,
    append: Boolean = false,
    minBlockSize: Long = 1L,
    branchingFactor: Int = 50,
    tmpDir: String = "/tmp"): HailContext = {

    val javaVersion = System.getProperty("java.version")
    if (!javaVersion.startsWith("1.8"))
      fatal(s"Hail requires Java 1.8, found version $javaVersion")

    {
      import breeze.linalg._
      import breeze.linalg.operators.{BinaryRegistry, OpMulMatrix}

      implicitly[BinaryRegistry[DenseMatrix[Double], Vector[Double], OpMulMatrix.type, DenseVector[Double]]].register(
        DenseMatrix.implOpMulMatrix_DMD_DVD_eq_DVD)
    }

    configureLogging(logFile, quiet, append)

    val sparkContext = if (sc == null)
      configureAndCreateSparkContext(appName, master, local, minBlockSize)
    else {
      checkSparkConfiguration(sc)
      sc
    }

    sparkContext.hadoopConfiguration.set("io.compression.codecs",
      "org.apache.hadoop.io.compress.DefaultCodec," +
        "is.hail.io.compress.BGzipCodec," +
        "org.apache.hadoop.io.compress.GzipCodec"
    )

    if (!quiet)
      ProgressBarBuilder.build(sparkContext)

    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    val hailTempDir = TempDir.createTempDir(tmpDir, sparkContext.hadoopConfiguration)
    val hc = new HailContext(sparkContext, sqlContext, hailTempDir, branchingFactor)
    sparkContext.uiWebUrl.foreach(ui => info(s"SparkUI: $ui"))

    info(s"Running Hail version ${ hc.version }")
    hc
  }

  def startProgressBar(sc: SparkContext): Unit = {
    ProgressBarBuilder.build(sc)
  }

  def readRowsPartition(
    t: TStruct,
    codecSpec: CodecSpec
  )(ctx: RVDContext,
    in: InputStream,
    metrics: InputMetrics = null
  ): Iterator[RegionValue] = new Iterator[RegionValue] {
    private val region = ctx.region()
    private val rv = RegionValue(region)

    private val trackedIn = new ByteTrackingInputStream(in)
    private val dec =
      try {
        codecSpec.buildDecoder(trackedIn)
      } catch {
        case e: Exception =>
          in.close()
          throw e
      }

    private var cont: Byte = dec.readByte()
    if (cont == 0)
      dec.close()

    // can't throw
    def hasNext: Boolean = cont != 0

    def next(): RegionValue = {
      // !hasNext => cont == 0 => dec has been closed
      if (!hasNext)
        throw new NoSuchElementException("next on empty iterator")

      try {
        region.clear()
        rv.setOffset(dec.readRegionValue(t, region))
        if (metrics != null) {
          ExposedMetrics.incrementRecord(metrics)
          ExposedMetrics.setBytes(metrics, trackedIn.bytesRead)
        }

        cont = dec.readByte()
        if (cont == 0)
          dec.close()

        rv
      } catch {
        case e: Exception =>
          dec.close()
          throw e
      }
    }

    override def finalize(): Unit = {
      dec.close()
    }
  }
}

class HailContext private(val sc: SparkContext,
  val sqlContext: SQLContext,
  val tmpDir: String,
  val branchingFactor: Int) {
  val hadoopConf: hadoop.conf.Configuration = sc.hadoopConfiguration

  def version: String = is.hail.HAIL_PRETTY_VERSION

  def grep(regex: String, files: Seq[String], maxLines: Int = 100) {
    val regexp = regex.r
    sc.textFilesLines(hadoopConf.globAll(files))
      .filter(line => regexp.findFirstIn(line.value).isDefined)
      .take(maxLines)
      .groupBy(_.source.asInstanceOf[Context].file)
      .foreach { case (file, lines) =>
        info(s"$file: ${ lines.length } ${ plural(lines.length, "match", "matches") }:")
        lines.map(_.value).foreach { line =>
          val (screen, logged) = line.truncatable().strings
          log.info("\t" + logged)
          println(s"\t$screen")
        }
      }
  }

  def getTemporaryFile(nChar: Int = 10, prefix: Option[String] = None, suffix: Option[String] = None): String =
    sc.hadoopConfiguration.getTemporaryFile(tmpDir, nChar, prefix, suffix)

  def importBgen(file: String,
    sampleFile: Option[String] = None,
    includeGT: Boolean,
    includeGP: Boolean,
    includeDosage: Boolean,
    nPartitions: Option[Int] = None,
    rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    contigRecoding: Option[Map[String, String]] = None,
    tolerance: Double = 0.2): MatrixTable = {
    importBgens(List(file), sampleFile, includeGT, includeGP, includeDosage, nPartitions, rg, contigRecoding, tolerance)
  }

  def importBgens(files: Seq[String],
    sampleFile: Option[String] = None,
    includeGT: Boolean = true,
    includeGP: Boolean = true,
    includeDosage: Boolean = false,
    nPartitions: Option[Int] = None,
    rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    contigRecoding: Option[Map[String, String]] = None,
    tolerance: Double = 0.2): MatrixTable = {

    val inputs = hadoopConf.globAll(files).flatMap { file =>
      if (!file.endsWith(".bgen"))
        warn(s"Input file does not have .bgen extension: $file")

      if (hadoopConf.isDir(file))
        hadoopConf.listStatus(file)
          .map(_.getPath.toString)
          .filter(p => ".*part-[0-9]+".r.matches(p))
      else
        Array(file)
    }

    if (inputs.isEmpty)
      fatal(s"arguments refer to no files: '${ files.mkString(",") }'")

    rg.foreach(ref => contigRecoding.foreach(ref.validateContigRemap))

    LoadBgen.load(this, inputs, sampleFile, includeGT: Boolean, includeGP: Boolean, includeDosage: Boolean,
      nPartitions, rg, contigRecoding.getOrElse(Map.empty[String, String]), tolerance)
  }

  def importGen(file: String,
    sampleFile: String,
    chromosome: Option[String] = None,
    nPartitions: Option[Int] = None,
    tolerance: Double = 0.2,
    rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    contigRecoding: Option[Map[String, String]] = None): MatrixTable = {
    importGens(List(file), sampleFile, chromosome, nPartitions, tolerance, rg, contigRecoding)
  }

  def importGens(files: Seq[String],
    sampleFile: String,
    chromosome: Option[String] = None,
    nPartitions: Option[Int] = None,
    tolerance: Double = 0.2,
    rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    contigRecoding: Option[Map[String, String]] = None): MatrixTable = {
    val inputs = hadoopConf.globAll(files)

    inputs.foreach { input =>
      if (!hadoopConf.stripCodec(input).endsWith(".gen"))
        fatal(s"gen inputs must end in .gen[.bgz], found $input")
    }

    if (inputs.isEmpty)
      fatal(s"arguments refer to no files: ${ files.mkString(",") }")

    rg.foreach(ref => contigRecoding.foreach(ref.validateContigRemap))

    val samples = LoadBgen.readSampleFile(sc.hadoopConfiguration, sampleFile)
    val nSamples = samples.length

    //FIXME: can't specify multiple chromosomes
    val results = inputs.map(f => LoadGen(f, sampleFile, sc, rg, nPartitions,
      tolerance, chromosome, contigRecoding.getOrElse(Map.empty[String, String])))

    val unequalSamples = results.filter(_.nSamples != nSamples).map(x => (x.file, x.nSamples))
    if (unequalSamples.length > 0)
      fatal(
        s"""The following GEN files did not contain the expected number of samples $nSamples:
           |  ${ unequalSamples.map(x => s"""(${ x._2 } ${ x._1 }""").mkString("\n  ") }""".stripMargin)

    val noVariants = results.filter(_.nVariants == 0).map(_.file)
    if (noVariants.length > 0)
      fatal(
        s"""The following GEN files did not contain at least 1 variant:
           |  ${ noVariants.mkString("\n  ") })""".stripMargin)

    val nVariants = results.map(_.nVariants).sum

    info(s"Number of GEN files parsed: ${ results.length }")
    info(s"Number of variants in all GEN files: $nVariants")
    info(s"Number of samples in GEN files: $nSamples")

    val signature = TStruct(
      "locus" -> TLocus.schemaFromRG(rg),
      "alleles" -> TArray(TString()),
      "rsid" -> TString(), "varid" -> TString())

    val rdd = sc.union(results.map(_.rdd))

    MatrixTable.fromLegacy(this,
      MatrixType.fromParts(
        globalType = TStruct.empty(),
        colKey = Array("s"),
        colType = TStruct("s" -> TString()),
        rowPartitionKey = Array("locus"), rowKey = Array("locus", "alleles"),
        rowType = signature,
        entryType = TStruct("GT" -> TCall(),
          "GP" -> TArray(TFloat64()))),
      Annotation.empty,
      samples.map(Annotation(_)),
      rdd)
  }

  def importTable(inputs: java.util.ArrayList[String],
    keyNames: java.util.ArrayList[String],
    nPartitions: java.lang.Integer,
    types: java.util.HashMap[String, Type],
    commentChar: String,
    separator: String,
    missing: String,
    noHeader: Boolean,
    impute: Boolean,
    quote: java.lang.Character): Table = importTables(inputs.asScala, keyNames.asScala.toArray, if (nPartitions == null) None else Some(nPartitions),
    types.asScala.toMap, Option(commentChar), separator, missing, noHeader, impute, quote)

  def importTable(input: String,
    keyNames: Array[String] = Array.empty[String],
    nPartitions: Option[Int] = None,
    types: Map[String, Type] = Map.empty[String, Type],
    commentChar: Option[String] = None,
    separator: String = "\t",
    missing: String = "NA",
    noHeader: Boolean = false,
    impute: Boolean = false,
    quote: java.lang.Character = null): Table = {
    importTables(List(input), keyNames, nPartitions, types, commentChar, separator, missing, noHeader, impute, quote)
  }

  def importTables(inputs: Seq[String],
    keyNames: Array[String] = Array.empty[String],
    nPartitions: Option[Int] = None,
    types: Map[String, Type] = Map.empty[String, Type],
    commentChar: Option[String] = None,
    separator: String = "\t",
    missing: String = "NA",
    noHeader: Boolean = false,
    impute: Boolean = false,
    quote: java.lang.Character = null): Table = {
    require(nPartitions.forall(_ > 0), "nPartitions argument must be positive")

    val files = hadoopConf.globAll(inputs)
    if (files.isEmpty)
      fatal(s"Arguments referred to no files: '${ inputs.mkString(",") }'")

    val (struct, rdd) =
      TextTableReader.read(sc)(files, types, commentChar, separator, missing,
        noHeader, impute, nPartitions.getOrElse(sc.defaultMinPartitions), quote)

    Table(this, rdd.map(_.value), struct, keyNames)
  }

  def importPlink(bed: String, bim: String, fam: String,
    nPartitions: Option[Int] = None,
    delimiter: String = "\\\\s+",
    missing: String = "NA",
    quantPheno: Boolean = false,
    a2Reference: Boolean = true,
    rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    contigRecoding: Option[Map[String, String]] = None): MatrixTable = {

    rg.foreach(ref => contigRecoding.foreach(ref.validateContigRemap))

    val ffConfig = FamFileConfig(quantPheno, delimiter, missing)

    LoadPlink(this, bed, bim, fam,
      ffConfig, nPartitions, a2Reference, rg, contigRecoding.getOrElse(Map.empty[String, String]))
  }

  def importPlinkBFile(bfileRoot: String,
    nPartitions: Option[Int] = None,
    delimiter: String = "\\\\s+",
    missing: String = "NA",
    quantPheno: Boolean = false,
    a2Reference: Boolean = true,
    rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    contigRecoding: Option[Map[String, String]] = None): MatrixTable = {
    importPlink(bfileRoot + ".bed", bfileRoot + ".bim", bfileRoot + ".fam",
      nPartitions, delimiter, missing, quantPheno, a2Reference, rg, contigRecoding)
  }

  def read(file: String, dropCols: Boolean = false, dropRows: Boolean = false): MatrixTable = {
    MatrixTable.read(this, file, dropCols = dropCols, dropRows = dropRows)
  }

  def readVDS(file: String, dropSamples: Boolean = false, dropVariants: Boolean = false): MatrixTable =
    read(file, dropSamples, dropVariants)

  def readGDS(file: String, dropSamples: Boolean = false, dropVariants: Boolean = false): MatrixTable =
    read(file, dropSamples, dropVariants)

  def readTable(path: String): Table =
    Table.read(this, path)

  def readPartitions[T: ClassTag](
    path: String,
    partFiles: Array[String],
    read: (Int, InputStream, InputMetrics) => Iterator[T],
    optPartitioner: Option[Partitioner] = None
  ): RDD[T] = {
    val nPartitions = partFiles.length

    val sHadoopConfBc = sc.broadcast(new SerializableHadoopConfiguration(sc.hadoopConfiguration))

    new RDD[T](sc, Nil) {
      def getPartitions: Array[Partition] =
        Array.tabulate(nPartitions)(i => FilePartition(i, partFiles(i)))

      override def compute(split: Partition, context: TaskContext): Iterator[T] = {
        val p = split.asInstanceOf[FilePartition]
        val filename = path + "/parts/" + p.file
        val in = sHadoopConfBc.value.value.unsafeReader(filename)
        read(p.index, in, context.taskMetrics().inputMetrics)
      }

      @transient override val partitioner: Option[Partitioner] = optPartitioner
    }
  }

  def readRows(
    path: String,
    t: TStruct,
    codecSpec: CodecSpec,
    partFiles: Array[String]
  ): ContextRDD[RVDContext, RegionValue] =
    ContextRDD.weaken[RVDContext](readPartitions(path, partFiles, (_, is) => Iterator.single(is)))
      .cmapPartitions { (ctx, it) =>
        assert(it.hasNext)
        val is = it.next
        assert(!it.hasNext)
        HailContext.readRowsPartition(t, codecSpec)(ctx, is)
      }

  def parseVCFMetadata(file: String): Map[String, Map[String, Map[String, String]]] = {
    val reader = new HtsjdkRecordReader(Set.empty)
    LoadVCF.parseHeaderMetadata(this, reader, file)
  }

  private[this] val codecsKey = "io.compression.codecs"
  private[this] val hadoopGzipCodec = "org.apache.hadoop.io.compress.GzipCodec"
  private[this] val hailGzipAsBGZipCodec = "is.hail.io.compress.BGzipCodecGZ"

  private[this] def forceBGZip[T](force: Boolean)(body: => T): T = {
    val defaultCodecs = hadoopConf.get(codecsKey)
    if (force)
      hadoopConf.set(codecsKey, defaultCodecs.replaceAllLiterally(hadoopGzipCodec, hailGzipAsBGZipCodec))
    try {
      body
    } finally {
      hadoopConf.set(codecsKey, defaultCodecs)
    }
  }

  def importVCF(file: String, force: Boolean = false,
    forceBGZ: Boolean = false,
    headerFile: Option[String] = None,
    nPartitions: Option[Int] = None,
    dropSamples: Boolean = false,
    callFields: Set[String] = Set.empty[String],
    rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    contigRecoding: Option[Map[String, String]] = None): MatrixTable = {
    importVCFs(List(file), force, forceBGZ, headerFile, nPartitions, dropSamples, callFields, rg, contigRecoding)
  }

  def importVCFs(files: Seq[String], force: Boolean = false,
    forceBGZ: Boolean = false,
    headerFile: Option[String] = None,
    nPartitions: Option[Int] = None,
    dropSamples: Boolean = false,
    callFields: Set[String] = Set.empty[String],
    rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    contigRecoding: Option[Map[String, String]] = None): MatrixTable = {

    rg.foreach(ref => contigRecoding.foreach(ref.validateContigRemap))

    val inputs = LoadVCF.globAllVCFs(hadoopConf.globAll(files), hadoopConf, force || forceBGZ)

    forceBGZip(forceBGZ) {
      val reader = new HtsjdkRecordReader(callFields)
      LoadVCF(this, reader, headerFile, inputs, nPartitions, dropSamples, rg,
        contigRecoding.getOrElse(Map.empty[String, String]))
    }
  }

  def importMatrix(files: java.util.ArrayList[String],
    rowFields: java.util.HashMap[String, Type],
    keyNames: java.util.ArrayList[String],
    cellType: Type,
    missingVal: String,
    minPartitions: Option[Int],
    noHeader: Boolean,
    forceBGZ: Boolean): MatrixTable =
    importMatrices(files.asScala, rowFields.asScala.toMap, keyNames.asScala.toArray,
      cellType, missingVal, minPartitions, noHeader, forceBGZ)

  def importMatrices(files: Seq[String],
    rowFields: Map[String, Type],
    keyNames: Array[String],
    cellType: Type,
    missingVal: String = "NA",
    nPartitions: Option[Int],
    noHeader: Boolean,
    forceBGZ: Boolean): MatrixTable = {

    val inputs = hadoopConf.globAll(files)

    forceBGZip(forceBGZ) {
      LoadMatrix(this, inputs, rowFields, keyNames, cellType = TStruct("x" -> cellType), missingVal, nPartitions, noHeader)
    }
  }

  def indexBgen(file: String) {
    indexBgen(List(file))
  }

  def indexBgen(files: Seq[String]) {
    val inputs = hadoopConf.globAll(files).flatMap { file =>
      if (!file.endsWith(".bgen"))
        warn(s"Input file does not have .bgen extension: $file")

      if (hadoopConf.isDir(file))
        hadoopConf.listStatus(file)
          .map(_.getPath.toString)
          .filter(p => ".*part-[0-9]+".r.matches(p))
      else
        Array(file)
    }

    if (inputs.isEmpty)
      fatal(s"arguments refer to no files: '${ files.mkString(",") }'")

    val conf = new SerializableHadoopConfiguration(hadoopConf)

    sc.parallelize(inputs, numSlices = inputs.length).foreach { in =>
      LoadBgen.index(conf.value, in)
    }

    info(s"Number of BGEN files indexed: ${ inputs.length }")
  }

  def baldingNicholsModel(populations: Int,
    samples: Int,
    variants: Int,
    nPartitions: Option[Int] = None,
    popDist: Option[Array[Double]] = None,
    fst: Option[Array[Double]] = None,
    afDist: Distribution = UniformDist(0.1, 0.9),
    seed: Int = 0,
    rg: ReferenceGenome = ReferenceGenome.defaultReference,
    mixture: Boolean = false): MatrixTable =
    BaldingNicholsModel(this, populations, samples, variants, popDist, fst, seed, nPartitions, afDist, rg, mixture)

  def genDataset(): MatrixTable = VSMSubgen.realistic.gen(this).sample()

  def eval(expr: String): (Annotation, Type) = {
    val ec = EvalContext()
    val ast = Parser.parseToAST(expr, ec)
    ast.toIR() match {
      case Some(body) =>
        Region.scoped { region =>
          val t = ast.`type`
          t match {
            case _: TBoolean =>
              val (_, f) = ir.Compile[Boolean](body)
              (f()(region), t)
            case _: TInt32 =>
              val (_, f) = ir.Compile[Int](body)
              (f()(region), t)
            case _: TInt64 =>
              val (_, f) = ir.Compile[Long](body)
              (f()(region), t)
            case _: TFloat32 =>
              val (_, f) = ir.Compile[Float](body)
              (f()(region), t)
            case _: TFloat64 =>
              val (_, f) = ir.Compile[Double](body)
              (f()(region), t)
            case _ =>
              val (_, f) = ir.Compile[Long](body)
              val off = f()(region)
              val v2 = Annotation.safeFromRegionValue(t, region, off)
              (v2, t)
          }
        }
      case None =>
        val (t, f) = Parser.eval(ast, ec)
        (f(), t)
    }
  }

  def stop() {
    sc.stop()
  }
}
