package is.hail.io.bgen

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr.ir.{MatrixRead, MatrixReader, MatrixValue}
import is.hail.expr.types._
import is.hail.io._
import is.hail.io.index.{IndexReader, IndexWriter}
import is.hail.io.vcf.LoadVCF
import is.hail.rvd.{OrderedRVD, RVD}
import is.hail.table.Table
import is.hail.utils._
import is.hail.variant._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row

import scala.io.Source

case class BgenHeader(
  compressed: Boolean,
  nSamples: Int,
  nVariants: Int,
  headerLength: Int,
  dataStart: Int,
  hasIds: Boolean,
  version: Int,
  fileByteSize: Long,
  path: String
)

object LoadBgen {
  def readSamples(hConf: org.apache.hadoop.conf.Configuration, file: String, createIndex: Boolean): Array[String] = {
    val bState = readState(hConf, file)
    if (bState.hasIds) {
      hConf.readFile(file) { is =>
        val reader = new HadoopFSDataBinaryReader(is)

        reader.seek(bState.headerLength + 4)
        val sampleIdSize = reader.readInt()
        val nSamples = reader.readInt()

        if (nSamples != bState.nSamples)
          fatal("BGEN file is malformed -- number of sample IDs in header does not equal number in file")

        if (sampleIdSize + bState.headerLength > bState.dataStart - 4)
          fatal("BGEN file is malformed -- offset is smaller than length of header")

        (0 until nSamples).map { i =>
          reader.readLengthAndString(2)
        }.toArray
      }
    } else {
      if (!createIndex)
        warn(s"BGEN file `$file' contains no sample ID block and no sample ID file given.\n" +
          s"  Using _0, _1, ..., _N as sample IDs.")
      (0 until bState.nSamples).map(i => s"_$i").toArray
    }
  }

  def readSampleFile(hConf: org.apache.hadoop.conf.Configuration, file: String): Array[String] = {
    hConf.readFile(file) { s =>
      Source.fromInputStream(s)
        .getLines()
        .drop(2)
        .filter(line => !line.isEmpty)
        .map { line =>
          val arr = line.split("\\s+")
          arr(0)
        }
        .toArray
    }
  }

  def readState(hConf: org.apache.hadoop.conf.Configuration, file: String): BgenHeader = {
    hConf.readFile(file) { is =>
      val reader = new HadoopFSDataBinaryReader(is)
      readState(reader, file, hConf.getFileSize(file))
    }
  }

  def readState(reader: HadoopFSDataBinaryReader, path: String, byteSize: Long): BgenHeader = {
    reader.seek(0)
    val allInfoLength = reader.readInt()
    val headerLength = reader.readInt()
    val dataStart = allInfoLength + 4

    assert(headerLength <= allInfoLength)
    val nVariants = reader.readInt()
    val nSamples = reader.readInt()

    val magicNumber = reader.readBytes(4)
      .map(_.toInt)
      .toSeq

    if (magicNumber != FastSeq(0, 0, 0, 0) && magicNumber != FastSeq(98, 103, 101, 110))
      fatal(s"expected magic number [0000] or [bgen], got [${ magicNumber.mkString }]")

    if (headerLength > 20)
      reader.skipBytes(headerLength.toInt - 20)

    val flags = reader.readInt()
    val compressType = flags & 3

    if (compressType != 0 && compressType != 1)
      fatal(s"Hail only supports zlib compression.")

    val isCompressed = compressType != 0

    val version = (flags >>> 2) & 0xf
    if (version != 2)
      fatal(s"Hail supports BGEN version 1.2, got version 1.$version")

    val hasIds = (flags >> 31 & 1) != 0
    BgenHeader(
      isCompressed,
      nSamples,
      nVariants,
      headerLength,
      dataStart,
      hasIds,
      version,
      byteSize,
      path
    )
  }

  def checkVersionTwo(headers: Array[BgenHeader]) {
    val notVersionTwo = headers.filter(_.version != 2).map(x => x.path -> x.version)
    if (notVersionTwo.length > 0)
      fatal(
        s"""The following BGEN files are not BGENv2:
            |  ${ notVersionTwo.mkString("\n  ") }""".stripMargin)
  }

  def getFileHeaders(
    hConf: Configuration,
    files: Seq[String]): Array[BgenHeader] = {

    var statuses = hConf.globAllStatuses(files)
    statuses = statuses.flatMap { status =>
      val file = status.getPath.toString
      if (!file.endsWith(".bgen"))
        warn(s"input file does not have .bgen extension: $file")

      if (hConf.isDir(file))
        hConf.listStatus(file)
          .filter(status => ".*part-[0-9]+".r.matches(status.getPath.toString))
      else
        Array(status)
    }

    if (statuses.isEmpty)
      fatal(s"arguments refer to no files: '${ files.mkString(",") }'")

    val inputs = statuses.map(_.getPath.toString)
    inputs.map(LoadBgen.readState(hConf, _))
  }
}

case class MatrixBGENReader(
  files: Seq[String],
  sampleFile: Option[String],
  nPartitions: Option[Int],
  blockSizeInMB: Option[Int],
  rg: Option[String],
  contigRecoding: Map[String, String],
  skipInvalidLoci: Boolean,
  includedVariants: Option[Table]) extends MatrixReader {
  private val hc = HailContext.get
  private val sc = hc.sc
  private val hConf = sc.hadoopConfiguration

  private val referenceGenome = rg.map(ReferenceGenome.getReference)
  referenceGenome.foreach(_.validateContigRemap(contigRecoding))

  val headers = LoadBgen.getFileHeaders(hConf, files)

  private val totalSize = headers.map(_.fileByteSize).sum

  private val inputNPartitions = (blockSizeInMB, nPartitions) match {
    case (Some(blockSizeInMB), _) =>
      val blockSizeInB = blockSizeInMB * 1024 * 1024
      headers.map { header =>
        val size = header.fileByteSize
        ((size + blockSizeInB - 1) / blockSizeInB).toInt
      }
    case (_, Some(nParts)) =>
      headers.map { header =>
        val size = header.fileByteSize
        ((size * nParts + totalSize - 1) / totalSize).toInt
      }
    case (None, None) => fatal(s"Must specify either of 'blockSizeInMB' or 'nPartitions'.")
  }

  private val sampleIds = sampleFile.map(file => LoadBgen.readSampleFile(hConf, file))
    .getOrElse(LoadBgen.readSamples(hConf, headers.head.path, createIndex = false))

  LoadVCF.warnDuplicates(sampleIds)

  private val nSamples = sampleIds.length

  val unequalSamples = headers.filter(_.nSamples != nSamples).map(x => (x.path, x.nSamples))
  if (unequalSamples.length > 0) {
    val unequalSamplesString =
      unequalSamples.map(x => s"""(${ x._2 } ${ x._1 }""").mkString("\n  ")
    fatal(
      s"""The following BGEN files did not contain the expected number of samples $nSamples:
          |  $unequalSamplesString""".stripMargin)
  }

  val noVariants = headers.filter(_.nVariants == 0).map(_.path)
  if (noVariants.length > 0)
    fatal(
      s"""The following BGEN files did not contain at least 1 variant:
            |  ${ noVariants.mkString("\n  ") })""".stripMargin)

  LoadBgen.checkVersionTwo(headers)

  val nVariants = headers.map(_.nVariants).sum

  info(s"Number of BGEN files parsed: ${ headers.length }")
  info(s"Number of samples in BGEN files: $nSamples")
  info(s"Number of variants across all BGEN files: $nVariants")

  private val paths = headers.map(_.path)

  val outdatedIdxFiles = paths.filter(f => !hConf.exists(f + ".idx2") && hConf.exists(f + ".idx") && hConf.isFile(f + ".idx"))
  if (outdatedIdxFiles.length > 0)
    fatal(
      s"""The following BGEN files have index files that are no longer supported. Use 'index_bgen' to recreate the index file once before calling 'import_bgen':
          |  ${ outdatedIdxFiles.mkString("\n  ") })""".stripMargin)

  val missingIdxFiles = paths.filterNot(f => hConf.exists(f + ".idx2"))
  if (missingIdxFiles.length > 0)
    fatal(
      s"""The following BGEN files have missing index files. Use 'index_bgen' to create the index file once before calling 'import_bgen':
          |  ${ missingIdxFiles.mkString("\n  ") })""".stripMargin)

  val mismatchIdxFiles = paths.filter(f => hConf.exists(f + ".idx2") && hConf.isDir(f + ".idx2")).flatMap { f =>
    val ir = new IndexReader(hConf, f + ".idx2")
    val idxAttr = ir.attributes
    val attr = Map("reference_genome" -> rg.orNull, "contig_recoding" -> contigRecoding, "skip_invalid_loci" -> skipInvalidLoci)
    if (idxAttr != attr) {
      val msg = new StringBuilder()
      msg ++= s"The index file for BGEN file '$f' was created with different parameters than called with 'import_bgen':\n"
      Array("reference_genome", "contig_recoding", "skip_invalid_loci").foreach { k =>
        if (idxAttr(k) != attr(k))
          msg ++= s"parameter: '$k'\texpected: ${ idxAttr(k) }\tfound: ${ attr(k) }\n"
      }
      Some(msg.result())
    } else
      None
  }

  if (mismatchIdxFiles.length > 0)
    fatal(mismatchIdxFiles.mkString(""))

  def absolutePath(rel: String): String = {
    val matches = hConf.glob(rel)
    if (matches.length != 1)
      fatal(s"""found more than one match for variant filter path: $rel:
                 |${ matches.mkString(",") }""".stripMargin)
    val abs = matches(0).getPath.toString
    abs
  }

  private val includedOffsetsPerFile = includedVariants match {
    case Some(variantsTable) =>
      val variants = variantsTable.collect()
      // I tried to use sc.parallelize to do this in parallel, but couldn't fix the serialization error
      files.map { f =>
        val index = new IndexReader(hConf, f + ".idx2")
        val offsets = variants.flatMap(index.queryByKey(_).map(_.recordOffset))
        index.close()
        (absolutePath(f), offsets)
      }.toMap
    case _ => Map.empty[String, Array[Long]]
  }

  val fullType: MatrixType = MatrixType.fromParts(
    globalType = TStruct.empty(),
    colKey = Array("s"),
    colType = TStruct("s" -> TString()),
    rowType = TStruct(
      "locus" -> TLocus.schemaFromRG(referenceGenome),
      "alleles" -> TArray(TString()),
      "rsid" -> TString(),
      "varid" -> TString(),
      "file_row_idx" -> TInt64(),
      "offset" -> TInt64()),
    rowKey = Array("locus", "alleles"),
    rowPartitionKey = Array("locus"),
    entryType = TStruct(
      "GT" -> TCall(),
      "GP" -> +TArray(+TFloat64()),
      "dosage" -> +TFloat64()))

  def columnCount: Option[Int] = Some(nSamples)

  def partitionCounts: Option[IndexedSeq[Long]] = None

  lazy val fastKeys = BgenRDD(
    sc, headers, inputNPartitions, includedOffsetsPerFile,
    BgenSettings(
      nSamples,
      NoEntries,
      RowFields(false, false, false),
      referenceGenome,
      contigRecoding,
      skipInvalidLoci))

  private lazy val coercer = OrderedRVD.makeCoercer(fullType.orvdType, fullType.rowPartitionKey.length, fastKeys)

  def apply(mr: MatrixRead): MatrixValue = {
    require(headers.nonEmpty)

    val requestedType = mr.typ
    val requestedEntryType = requestedType.entryType

    val includeGT = requestedEntryType.hasField("GT")
    val includeGP = requestedEntryType.hasField("GP")
    val includeDosage = requestedEntryType.hasField("dosage")

    val requestedRowType = requestedType.rowType
    val includeLid = requestedRowType.hasField("varid")
    val includeRsid = requestedRowType.hasField("rsid")
    val includeOffset = requestedRowType.hasField("offset")

    val settings = BgenSettings(
      nSamples,
      EntriesWithFields(includeGT, includeGP, includeDosage),
      RowFields(includeLid, includeRsid, includeOffset),
      referenceGenome,
      contigRecoding,
      skipInvalidLoci)
    assert(mr.typ == settings.matrixType)

    val rvd = if (mr.dropRows)
      OrderedRVD.empty(sc, requestedType.orvdType)
    else
      coercer.coerce(requestedType.orvdType,
        BgenRDD(sc, headers, inputNPartitions, includedOffsetsPerFile, settings))

    MatrixValue(mr.typ,
      BroadcastRow(Row.empty, mr.typ.globalType, sc),
      BroadcastIndexedSeq(
        if (mr.dropCols)
          IndexedSeq.empty[Annotation]
        else
          sampleIds.map(x => Annotation(x)),
        TArray(requestedType.colType), sc),
      rvd)
  }
}
