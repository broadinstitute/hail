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
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.JsonMethods.parse

case class MatrixValue(
  typ: MatrixType,
  globals: BroadcastRow,
  colValues: BroadcastIndexedSeq,
  rvd: RVD) {

  require(typ.rvRowType == rvd.rowType, s"\nmat rowType: ${ typ.rowType }\nrvd rowType: ${ rvd.rowType }")
  require(rvd.typ.key.startsWith(typ.rowKey), s"\nmat row key: ${ typ.rowKey }\nrvd key: ${ rvd.typ.key }")

  def sparkContext: SparkContext = rvd.sparkContext

  def nPartitions: Int = rvd.getNumPartitions

  def nCols: Int = colValues.value.length

  def sampleIds: IndexedSeq[Row] = {
    val queriers = typ.colKey.map(field => typ.colType.query(field))
    colValues.value.map(a => Row.fromSeq(queriers.map(_ (a))))
  }

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

  def rowsTableValue: TableValue = TableValue(typ.rowsTableType, globals, rowsRVD())

  def entriesTableValue: TableValue = TableValue(typ.entriesTableType, globals, entriesRVD())

  private def writeCols(path: String, codecSpec: CodecSpec) {
    val hc = HailContext.get
    val hadoopConf = hc.hadoopConf

    val partitionCounts = AbstractRVDSpec.writeLocal(hc, path + "/rows", typ.colType.physicalType, codecSpec, colValues.value)

    val colsSpec = TableSpec(
      FileFormat.version.rep,
      hc.version,
      "../references",
      typ.colsTableType,
      Map("globals" -> RVDComponentSpec("../globals/rows"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    colsSpec.write(hc, path)

    hadoopConf.writeTextFile(path + "/_SUCCESS")(out => ())
  }

  private def writeGlobals(path: String, codecSpec: CodecSpec) {
    val hc = HailContext.get
    val hadoopConf = hc.hadoopConf

    val partitionCounts = AbstractRVDSpec.writeLocal(hc, path + "/rows", typ.globalType.physicalType, codecSpec, Array(globals.value))

    AbstractRVDSpec.writeLocal(hc, path + "/globals", TStruct.empty().physicalType, codecSpec, Array[Annotation](Row()))

    val globalsSpec = TableSpec(
      FileFormat.version.rep,
      hc.version,
      "../references",
      TableType(typ.globalType, FastIndexedSeq(), TStruct.empty()),
      Map("globals" -> RVDComponentSpec("globals"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    globalsSpec.write(hc, path)

    hadoopConf.writeTextFile(path + "/_SUCCESS")(out => ())
  }

  private def finalizeWrite(path: String, codecSpec: CodecSpec, partitionCounts: Array[Long]) = {
    val hc = HailContext.get
    val hadoopConf = hc.hadoopConf

    val globalsPath = path + "/globals"
    hadoopConf.mkDir(globalsPath)
    writeGlobals(globalsPath, codecSpec)

    val rowsSpec = TableSpec(
      FileFormat.version.rep,
      hc.version,
      "../references",
      typ.rowsTableType,
      Map("globals" -> RVDComponentSpec("../globals/rows"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    rowsSpec.write(hc, path + "/rows")

    hadoopConf.writeTextFile(path + "/rows/_SUCCESS")(out => ())

    val entriesSpec = TableSpec(
      FileFormat.version.rep,
      hc.version,
      "../references",
      TableType(typ.entriesRVType, FastIndexedSeq(), typ.globalType),
      Map("globals" -> RVDComponentSpec("../globals/rows"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    entriesSpec.write(hc, path + "/entries")

    hadoopConf.writeTextFile(path + "/entries/_SUCCESS")(out => ())

    hadoopConf.mkDir(path + "/cols")
    writeCols(path + "/cols", codecSpec)

    val refPath = path + "/references"
    hc.hadoopConf.mkDir(refPath)
    Array(typ.colType, typ.rowType, typ.entryType, typ.globalType).foreach { t =>
      ReferenceGenome.exportReferences(hc, refPath, t)
    }

    val spec = MatrixTableSpec(
      FileFormat.version.rep,
      hc.version,
      "references",
      typ,
      Map("globals" -> RVDComponentSpec("globals/rows"),
        "cols" -> RVDComponentSpec("cols/rows"),
        "rows" -> RVDComponentSpec("rows/rows"),
        "entries" -> RVDComponentSpec("entries/rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    spec.write(hc, path)

    writeNativeFileReadMe(path)

    hadoopConf.writeTextFile(path + "/_SUCCESS")(out => ())

    val nRows = partitionCounts.sum
    val nCols = colValues.value.length
    info(s"wrote matrix table with $nRows ${ plural(nRows, "row") } " +
      s"and $nCols ${ plural(nCols, "column") } " +
      s"in ${ partitionCounts.length } ${ plural(partitionCounts.length, "partition") } " +
      s"to $path")
  }

  def write(path: String, overwrite: Boolean = false, stageLocally: Boolean = false, codecSpecJSONStr: String = null) = {
    val hc = HailContext.get
    val hadoopConf = hc.hadoopConf

    val codecSpec =
      if (codecSpecJSONStr != null) {
        implicit val formats = AbstractRVDSpec.formats
        val codecSpecJSON = parse(codecSpecJSONStr)
        codecSpecJSON.extract[CodecSpec]
      } else
        CodecSpec.default

    if (overwrite)
      hadoopConf.delete(path, recursive = true)
    else if (hadoopConf.exists(path))
      fatal(s"file already exists: $path")

    hc.hadoopConf.mkDir(path)

    val partitionCounts = rvd.writeRowsSplit(path, codecSpec, stageLocally)

    finalizeWrite(path, codecSpec, partitionCounts)
  }

  lazy val (sortedColValues, sortedColsToOldIdx): (BroadcastIndexedSeq, BroadcastIndexedSeq) = {
    val (_, keyF) = typ.colType.select(typ.colKey)
    if (typ.colKey.isEmpty)
      (colValues,
        BroadcastIndexedSeq(
          IndexedSeq.range(0, colValues.safeValue.length),
          TArray(TInt32()),
          colValues.sc))
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
          colValues.sc))
    }
  }

  def rowsRVD(): RVD = {
    val localRowType = typ.rowType
    val fullRowType = typ.rvRowType
    val localEntriesIndex = typ.entriesIdx
    rvd.mapPartitions(
      RVDType(typ.rowType.physicalType, typ.rowKey)
    ) { it =>
      val rv2b = new RegionValueBuilder()
      val rv2 = RegionValue()
      it.map { rv =>
        rv2b.set(rv.region)
        rv2b.start(localRowType.physicalType)
        rv2b.startStruct()
        var i = 0
        while (i < fullRowType.size) {
          if (i != localEntriesIndex)
            rv2b.addField(fullRowType.physicalType, rv, i)
          i += 1
        }
        rv2b.endStruct()
        rv2.set(rv.region, rv2b.end())
        rv2
      }
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

  def entriesRVD(): RVD = {
    val resultStruct = typ.entriesTableType.rowType
    val fullRowType = typ.rvRowType.physicalType
    val localEntriesIndex = typ.entriesIdx
    val localEntriesType = typ.entryArrayType.physicalType
    val localColType = typ.colType
    val localEntryType = typ.entryType
    val localRVDType = typ.canonicalRVDType
    val localNCols = nCols

    val localSortedColValues = sortedColValues.broadcast
    val localSortedColsToOldIdx = sortedColsToOldIdx.broadcast

    rvd.repartition(rvd.partitioner.strictify).boundary
      .mapPartitions(typ.entriesTableType.canonicalRVDType.copy(key = typ.rowKey), { (ctx, it) =>
        val rv2b = ctx.rvb
        val rv2 = RegionValue(ctx.region)

        val colRegion = ctx.freshRegion
        val colRVB = new RegionValueBuilder(colRegion)
        val colsType = TArray(localColType, required = true).physicalType
        colRVB.start(colsType)
        colRVB.addAnnotation(colsType.virtualType, localSortedColValues.value)
        val colsOffset = colRVB.end()

        val rowBuffer = new RegionValueArrayBuffer(fullRowType, ctx.freshRegion)

        val colsNewToOldIdx = localSortedColsToOldIdx.value.asInstanceOf[IndexedSeq[Int]]

        OrderedRVIterator(localRVDType, it, ctx).staircase.flatMap { step =>
          rowBuffer.clear()
          rowBuffer ++= step
          (0 until localNCols).iterator.flatMap { i =>
            rowBuffer.iterator
              .filter { row =>
                localEntriesType.isElementDefined(row.region, fullRowType.loadField(row, localEntriesIndex), colsNewToOldIdx(i))
              }.map { row =>
              rv2b.clear()
              rv2b.start(resultStruct.physicalType)
              rv2b.startStruct()

              var j = 0
              while (j < fullRowType.size) {
                if (j != localEntriesIndex)
                  rv2b.addField(fullRowType, row, j)
                j += 1
              }

              rv2b.addAllFields(localColType.physicalType, colRegion, colsType.loadElement(colRegion, colsOffset, i))
              rv2b.addAllFields(
                localEntryType.physicalType,
                row.region,
                localEntriesType.loadElement(row.region, fullRowType.loadField(row, localEntriesIndex), colsNewToOldIdx(i)))
              rv2b.endStruct()
              rv2.setOffset(rv2b.end())
              rv2
            }
          }
        }
      }).extendKeyPreservesPartitioning(typ.entriesTableType.key)
  }

  def insertEntries[PC](makePartitionContext: () => PC, newColType: TStruct = typ.colType,
    newColKey: IndexedSeq[String] = typ.colKey,
    newColValues: BroadcastIndexedSeq = colValues,
    newGlobalType: TStruct = typ.globalType,
    newGlobals: BroadcastRow = globals)(newEntryType: PStruct,
    inserter: (PC, RegionValue, RegionValueBuilder) => Unit): MatrixValue = {
    insertIntoRow(makePartitionContext, newColType, newColKey, newColValues, newGlobalType, newGlobals)(
      PArray(newEntryType), MatrixType.entriesIdentifier, inserter)
  }

  def insertIntoRow[PC](makePartitionContext: () => PC, newColType: TStruct = typ.colType,
    newColKey: IndexedSeq[String] = typ.colKey,
    newColValues: BroadcastIndexedSeq = colValues,
    newGlobalType: TStruct = typ.globalType,
    newGlobals: BroadcastRow = globals)(typeToInsert: PType, path: String,
    inserter: (PC, RegionValue, RegionValueBuilder) => Unit): MatrixValue = {
    assert(!typ.rowKey.contains(path))

    val fullRowType = rvd.rowPType
    val localEntriesIndex = MatrixType.getEntriesIndex(fullRowType)

    val (newRVPType, ins) = fullRowType.unsafeStructInsert(typeToInsert, List(path))


    val newMatrixType = typ.copy(rvRowType = newRVPType.virtualType, colType = newColType,
      colKey = newColKey, globalType = newGlobalType)

    MatrixValue(
      newMatrixType,
      newGlobals,
      newColValues,
      rvd.mapPartitions(newMatrixType.canonicalRVDType) { it =>

        val pc = makePartitionContext()

        val rv2 = RegionValue()
        val rvb = new RegionValueBuilder()
        it.map { rv =>
          rvb.set(rv.region)
          rvb.start(newRVPType)

          ins(rv.region, rv.offset, rvb,
            () => inserter(pc, rv, rvb)
          )

          rv2.set(rv.region, rvb.end())
          rv2
        }
      })
  }

  def toRowMatrix(entryField: String): RowMatrix = {
    val partCounts: Array[Long] = rvd.countPerPartition()
    val partStarts = partCounts.scanLeft(0L)(_ + _)
    assert(partStarts.length == rvd.getNumPartitions + 1)
    val partStartsBc = sparkContext.broadcast(partStarts)

    val rvRowType = typ.rvRowType.physicalType
    val entryArrayType = typ.entryArrayType.physicalType
    val entryType = typ.entryType.physicalType
    val fieldType = entryType.field(entryField).typ

    assert(fieldType.virtualType.isOfType(TFloat64()))

    val entryArrayIdx = typ.entriesIdx
    val fieldIdx = entryType.fieldIdx(entryField)
    val numColsLocal = nCols

    val rows = rvd.mapPartitionsWithIndex { (pi, it) =>
      var i = partStartsBc.value(pi)
      it.map { rv =>
        val region = rv.region
        val data = new Array[Double](numColsLocal)
        val entryArrayOffset = rvRowType.loadField(rv, entryArrayIdx)
        var j = 0
        while (j < numColsLocal) {
          if (entryArrayType.isElementDefined(region, entryArrayOffset, j)) {
            val entryOffset = entryArrayType.loadElement(region, entryArrayOffset, j)
            if (entryType.isFieldDefined(region, entryOffset, fieldIdx)) {
              val fieldOffset = entryType.loadField(region, entryOffset, fieldIdx)
              data(j) = region.loadDouble(fieldOffset)
            } else
              fatal(s"Cannot create RowMatrix: missing value at row $i and col $j")
          } else
            fatal(s"Cannot create RowMatrix: missing entry at row $i and col $j")
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
    val localRVRowType = typ.rvRowType
    assert(rvd.toRows.forall(r => localRVRowType.typeCheck(r)))
  }

  def persist(storageLevel: StorageLevel): MatrixValue = copy(rvd = rvd.persist(storageLevel))

  def unpersist(): MatrixValue = copy(rvd = rvd.unpersist())

  def filterCols(p: (Annotation, Int) => Boolean): MatrixValue = {
    val (_, filterF) = MatrixIR.filterCols(typ)
    Interpret(MatrixLiteral(filterF(this, p)))
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
    val hadoopConf = hc.hadoopConf
    val codecSpec = CodecSpec.default

    val d = digitsNeeded(mvs.length)
    val paths = (0 until mvs.length).map { i => prefix + StringUtils.leftPad(i.toString, d, '0') + ".mt" }
    paths.foreach { path =>
      if (overwrite)
        hadoopConf.delete(path, recursive = true)
      else if (hadoopConf.exists(path))
        fatal(s"file already exists: $path")
      hadoopConf.mkDir(path)
    }

    val partitionCounts = RVD.writeRowsSplitFiles(mvs.map(_.rvd), prefix, codecSpec, stageLocally)
    for ((mv, path, partCounts) <- (mvs, paths, partitionCounts).zipped) {
      mv.finalizeWrite(path, codecSpec, partCounts)
    }
  }
}
