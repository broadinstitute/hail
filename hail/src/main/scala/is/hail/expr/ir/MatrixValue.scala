package is.hail.expr.ir

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr.types.physical.{PArray, PStruct, PType}
import is.hail.expr.types.virtual._
import is.hail.expr.types.{MatrixType, TableType}
import is.hail.io.CodecSpec
import is.hail.linalg.RowMatrix
import is.hail.rvd.{AbstractRVDSpec, RVD, RVDType, _}
import is.hail.sparkextras.ContextRDD
import is.hail.table.TableSpec
import is.hail.utils._
import is.hail.variant._
import is.hail.io.fs.{FS, HadoopFS}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods.parse

case class MatrixValue(
  typ: MatrixType,
  globals: BroadcastRow,
  colValues: BroadcastIndexedSeq,
  rvd: RVD) {

  lazy val rvRowPType: PStruct = rvd.typ.rowType
  lazy val rvRowType: TStruct = rvRowPType.virtualType
  lazy val entriesIdx: Int = rvRowPType.fieldIdx(MatrixType.entriesIdentifier)
  lazy val entryArrayPType: PArray = rvRowPType.types(entriesIdx).asInstanceOf[PArray]
  lazy val entryArrayType: TArray = rvRowType.types(entriesIdx).asInstanceOf[TArray]
  lazy val entryPType: PStruct = entryArrayPType.elementType.asInstanceOf[PStruct]
  lazy val entryType: TStruct = entryArrayType.elementType.asInstanceOf[TStruct]

  lazy val entriesRVType: TStruct = TStruct(
    MatrixType.entriesIdentifier -> TArray(entryType))

  require(rvd.typ.key.startsWith(typ.rowKey), s"\nmat row key: ${ typ.rowKey }\nrvd key: ${ rvd.typ.key }")

  def sparkContext: SparkContext = rvd.sparkContext

  def nPartitions: Int = rvd.getNumPartitions

  def nCols: Int = colValues.value.length

  def stringSampleIds: IndexedSeq[String] = {
    val colKeyTypes = typ.colKeyStruct.types
    assert(colKeyTypes.length == 1 && colKeyTypes(0).isInstanceOf[TString], colKeyTypes.toSeq)
    val querier = typ.colType.query(typ.colKey(0))
    colValues.value.map(querier(_).asInstanceOf[String])
  }

  def requireUniqueSamples(method: String) {
    val dups = stringSampleIds.counter().filter(_._2 > 1).toArray
    if (dups.nonEmpty)
      fatal(s"Method '$method' does not support duplicate column keys. Duplicates:" +
        s"\n  @1", dups.sortBy(-_._2).map { case (id, count) => s"""($count) "$id"""" }.truncatable("\n  "))
  }

  def referenceGenome: ReferenceGenome = typ.referenceGenome

  def colsTableValue: TableValue = TableValue(typ.colsTableType, globals, colsRVD())

  private def writeCols(fs: FS, path: String, codecSpec: CodecSpec) {
    val partitionCounts = AbstractRVDSpec.writeSingle(fs, path + "/rows", typ.colType.physicalType, codecSpec, colValues.value)

    val colsSpec = TableSpec(
      FileFormat.version.rep,
      is.hail.HAIL_PRETTY_VERSION,
      "../references",
      typ.colsTableType,
      Map("globals" -> RVDComponentSpec("../globals/rows"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    colsSpec.write(fs, path)

    fs.writeTextFile(path + "/_SUCCESS")(out => ())
  }

  private def writeGlobals(fs: FS, path: String, codecSpec: CodecSpec) {
    val partitionCounts = AbstractRVDSpec.writeSingle(fs, path + "/rows", typ.globalType.physicalType, codecSpec, Array(globals.value))

    AbstractRVDSpec.writeSingle(fs, path + "/globals", TStruct.empty().physicalType, codecSpec, Array[Annotation](Row()))

    val globalsSpec = TableSpec(
      FileFormat.version.rep,
      is.hail.HAIL_PRETTY_VERSION,
      "../references",
      TableType(typ.globalType, FastIndexedSeq(), TStruct.empty()),
      Map("globals" -> RVDComponentSpec("globals"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    globalsSpec.write(fs, path)

    fs.writeTextFile(path + "/_SUCCESS")(out => ())
  }

  private def finalizeWrite(
    fs: FS,
    path: String,
    codecSpec: CodecSpec,
    partitionCounts: Array[Long]
  ) = {
    val globalsPath = path + "/globals"
    fs.mkDir(globalsPath)
    writeGlobals(fs, globalsPath, codecSpec)

    val rowsSpec = TableSpec(
      FileFormat.version.rep,
      is.hail.HAIL_PRETTY_VERSION,
      "../references",
      typ.rowsTableType,
      Map("globals" -> RVDComponentSpec("../globals/rows"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    rowsSpec.write(fs, path + "/rows")

    fs.writeTextFile(path + "/rows/_SUCCESS")(out => ())

    val entriesSpec = TableSpec(
      FileFormat.version.rep,
      is.hail.HAIL_PRETTY_VERSION,
      "../references",
      TableType(entriesRVType, FastIndexedSeq(), typ.globalType),
      Map("globals" -> RVDComponentSpec("../globals/rows"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    entriesSpec.write(fs, path + "/entries")

    fs.writeTextFile(path + "/entries/_SUCCESS")(out => ())

    fs.mkDir(path + "/cols")
    writeCols(fs, path + "/cols", codecSpec)

    val refPath = path + "/references"
    fs.mkDir(refPath)
    Array(typ.colType, typ.rowType, entryType, typ.globalType).foreach { t =>
      ReferenceGenome.exportReferences(fs, refPath, t)
    }

    val spec = MatrixTableSpec(
      FileFormat.version.rep,
      is.hail.HAIL_PRETTY_VERSION,
      "references",
      typ,
      Map("globals" -> RVDComponentSpec("globals/rows"),
        "cols" -> RVDComponentSpec("cols/rows"),
        "rows" -> RVDComponentSpec("rows/rows"),
        "entries" -> RVDComponentSpec("entries/rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    spec.write(fs, path)

    writeNativeFileReadMe(path)

    fs.writeTextFile(path + "/_SUCCESS")(out => ())

    val nRows = partitionCounts.sum
    val nCols = colValues.value.length
    info(s"wrote matrix table with $nRows ${ plural(nRows, "row") } " +
      s"and $nCols ${ plural(nCols, "column") } " +
      s"in ${ partitionCounts.length } ${ plural(partitionCounts.length, "partition") } " +
      s"to $path")
  }

  def write(path: String, overwrite: Boolean = false, stageLocally: Boolean = false, codecSpecJSONStr: String = null) = {
    val hc = HailContext.get
    val fs = hc.sFS

    val codecSpec =
      if (codecSpecJSONStr != null) {
        implicit val formats = AbstractRVDSpec.formats
        val codecSpecJSON = parse(codecSpecJSONStr)
        codecSpecJSON.extract[CodecSpec]
      } else
        CodecSpec.default

    if (overwrite)
      fs.delete(path, recursive = true)
    else if (fs.exists(path))
      fatal(s"file already exists: $path")

    fs.mkDir(path)

    val partitionCounts = rvd.writeRowsSplit(path, codecSpec, stageLocally)

    finalizeWrite(fs, path, codecSpec, partitionCounts)
  }

  lazy val (sortedColValues, sortedColsToOldIdx): (BroadcastIndexedSeq, BroadcastIndexedSeq) = {
    val (_, keyF) = typ.colType.select(typ.colKey)
    if (typ.colKey.isEmpty)
      (colValues,
        BroadcastIndexedSeq(
          IndexedSeq.range(0, colValues.safeValue.length),
          TArray(TInt32()),
          colValues.backend))
    else {
      val sortedValsWithIdx = colValues.safeValue
        .zipWithIndex
        .map(colIdx => (keyF(colIdx._1.asInstanceOf[Row]), colIdx))
        .sortBy(_._1)(typ.colKeyStruct.ordering.toOrdering.asInstanceOf[Ordering[Row]])
        .map(_._2)

      (colValues.copy(value = sortedValsWithIdx.map(_._1)),
        BroadcastIndexedSeq(
          sortedValsWithIdx.map(_._2),
          TArray(TInt32()),
          colValues.backend))
    }
  }

  def colsRVD(): RVD = {
    val hc = HailContext.get
    val colPType = typ.colType.physicalType

    RVD.coerce(
      typ.colsTableType.canonicalRVDType,
      ContextRDD.parallelize(hc.sc, sortedColValues.safeValue.asInstanceOf[IndexedSeq[Row]])
        .cmapPartitions { (ctx, it) => it.toRegionValueIterator(ctx.region, colPType) }
    )
  }

  def toRowMatrix(entryField: String): RowMatrix = {
    val partCounts: Array[Long] = rvd.countPerPartition()
    val partStarts = partCounts.scanLeft(0L)(_ + _)
    assert(partStarts.length == rvd.getNumPartitions + 1)
    val partStartsBc = HailContext.backend.broadcast(partStarts)

    val localRvRowPType = rvRowPType
    val localEntryArrayPType = entryArrayPType
    val localEntryPType = entryPType
    val fieldType = entryPType.field(entryField).typ

    assert(fieldType.virtualType.isOfType(TFloat64()))

    val localEntryArrayIdx = entriesIdx
    val fieldIdx = entryType.fieldIdx(entryField)
    val numColsLocal = nCols

    val rows = rvd.mapPartitionsWithIndex { (pi, it) =>
      var i = partStartsBc.value(pi)
      it.map { rv =>
        val region = rv.region
        val data = new Array[Double](numColsLocal)
        val entryArrayOffset = localRvRowPType.loadField(rv, localEntryArrayIdx)
        var j = 0
        while (j < numColsLocal) {
          if (localEntryArrayPType.isElementDefined(region, entryArrayOffset, j)) {
            val entryOffset = localEntryArrayPType.loadElement(region, entryArrayOffset, j)
            if (localEntryPType.isFieldDefined(region, entryOffset, fieldIdx)) {
              val fieldOffset = localEntryPType.loadField(region, entryOffset, fieldIdx)
              data(j) = region.loadDouble(fieldOffset)
            } else
              fatal(s"Cannot create RowMatrix: missing value at row $i and col $j")
          } else
            fatal(s"Cannot create RowMatrix: filtered entry at row $i and col $j")
          j += 1
        }
        val row = (i, data)
        i += 1
        row
      }
    }

    new RowMatrix(HailContext.get, rows, nCols, Some(partStarts.last), Some(partCounts))
  }

  def typeCheck(): Unit = {
    assert(typ.globalType.typeCheck(globals.value))
    assert(TArray(typ.colType).typeCheck(colValues.value))
    val localRVRowType = rvRowType
    assert(rvd.toRows.forall(r => localRVRowType.typeCheck(r)))
  }

  def persist(storageLevel: StorageLevel): MatrixValue = copy(rvd = rvd.persist(storageLevel))

  def unpersist(): MatrixValue = copy(rvd = rvd.unpersist())

  def toTableValue: TableValue = {
    val tt: TableType = typ.canonicalTableType
    val newGlobals = BroadcastRow(
      Row.merge(globals.safeValue, Row(colValues.safeValue)),
      tt.globalType,
      HailContext.backend)

    TableValue(tt, newGlobals, rvd.cast(tt.rowType.physicalType))
  }
}

object MatrixValue {
  def writeMultiple(
    mvs: IndexedSeq[MatrixValue],
    prefix: String,
    overwrite: Boolean,
    stageLocally: Boolean
  ): Unit = {
    val first = mvs.head
    require(mvs.forall(_.typ == first.typ))
    val hc = HailContext.get
    val fs = hc.sFS
    val codecSpec = CodecSpec.default

    val d = digitsNeeded(mvs.length)
    val paths = (0 until mvs.length).map { i => prefix + StringUtils.leftPad(i.toString, d, '0') + ".mt" }
    paths.foreach { path =>
      if (overwrite)
        fs.delete(path, recursive = true)
      else if (fs.exists(path))
        fatal(s"file already exists: $path")
      fs.mkDir(path)
    }

    val partitionCounts = RVD.writeRowsSplitFiles(mvs.map(_.rvd), prefix, codecSpec, stageLocally)
    for ((mv, path, partCounts) <- (mvs, paths, partitionCounts).zipped) {
      mv.finalizeWrite(fs, path, codecSpec, partCounts)
    }
  }
}
