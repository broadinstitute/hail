package is.hail.io

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr.types._
import is.hail.rvd.{OrderedRVD, OrderedRVDPartitioner}
import is.hail.utils._
import is.hail.variant._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Row

import scala.language.implicitConversions
import scala.io.Source

class LoadMatrixParser(rvb: RegionValueBuilder, fieldTypes: Array[Type], entryType: TStruct, nCols: Int, missingValue: String, file: String) {

  assert(entryType.size == 1)

  val sep = '\t'
  val nFields: Int = fieldTypes.length
  val cellf: (String, Long, Int, Int) => Int = addType(entryType.fieldType(0))

  def parseLine(line: String, rowNum: Long): Unit = {
    var ii = 0
    var off = 0
    while (ii < fieldTypes.length) {
      off = addType(fieldTypes(ii))(line, rowNum, ii, off)
      ii += 1
      if (off > line.length) {
        fatal(
          s"""Error parsing row fields in row $rowNum:
             |    expected $nFields fields but only $ii found.
             |    File: $file
             |    Line:
             |        ${ line.truncate }""".stripMargin
        )
      }
    }

    ii = 0
    rvb.startArray(nCols)
    while (ii < nCols) {
      if (off > line.length) {
        fatal(
          s"""Incorrect number of entries in row $rowNum:
             |    expected $nCols entries but only $ii entries found.
             |    File: $file
             |    Line:
             |        ${ line.truncate }""".stripMargin
        )
      }
      rvb.startStruct()
      off = cellf(line, rowNum, ii, off)
      rvb.endStruct()
      ii += 1
    }
    if (off < line.length) {
      fatal(
        s"""Incorrect number of entries in row $rowNum:
           |    expected $nCols entries but more data found.
           |    in file $file""".stripMargin
      )
    }
    rvb.endArray()
  }

  def addType(t: Type): (String, Long, Int, Int) => Int = t match {
    case TInt32(_) => addInt
    case TInt64(_) => addLong
    case TFloat32(_) => addFloat
    case TFloat64(_) => addDouble
    case TString(_) => addString
  }

  def addString(line: String, rowNum: Long, colNum: Int, off: Int): Int = {
    var newoff = line.indexOf(sep, off)
    if (newoff == -1) {
      newoff = line.length
    }
    val v = line.substring(off, newoff)
    if (v == missingValue){
      rvb.setMissing()
    } else rvb.addString(v)
    newoff + 1
  }

  def addInt(line: String, rowNum: Long, colNum: Int, off: Int): Int = {
    var newoff = off
    var v = 0
    var isNegative = false
    if (line(off) == sep) {
      fatal(s"Error parsing matrix. Invalid Int32 at column: $colNum, row: $rowNum in file: $file")
    }
    if (line(off) == '-' || line(off) == '+') {
      isNegative = line(off) == '-'
      newoff += 1
    }
    while (newoff < line.length && line(newoff) >= '0' && line(newoff) <= '9') {
      v *= 10
      v += (line(newoff) - '0')
      newoff += 1
    }
    if (newoff == off) {
      while (newoff - off < missingValue.length && missingValue(newoff - off) == line(newoff)) {
        newoff += 1
      }

      if (newoff - off == missingValue.length && (line.length == newoff || line(newoff) == sep)) {
        rvb.setMissing()
      } else {
        fatal(s"Error parsing matrix. Invalid Int32 at column: $colNum, row: $rowNum in file: $file")
      }
    } else if (line.length == newoff || line(newoff) == sep) {
      if (isNegative) rvb.addInt(-v) else rvb.addInt(v)
    } else {
      fatal(s"Error parsing matrix. $v Invalid Int32 at column: $colNum, row: $rowNum in file: $file")
    }
    newoff + 1
  }

  def addLong(line: String, rowNum: Long, colNum: Int, off: Int): Int = {
    var newoff = off
    var v = 0L
    var isNegative = false
    if (line(off) == sep) {
      fatal(s"Error parsing matrix. Invalid Int64 at column: $colNum, row: $rowNum in file: $file")
    }
    if (line(off) == '-' || line(off) == '+') {
      isNegative = line(off) == '-'
      newoff += 1
    }
    while (newoff < line.length && line(newoff) >= '0' && line(newoff) <= '9') {
      v *= 10
      v += line(newoff) - '0'
      newoff += 1
    }
    if (newoff == off) {
      while (newoff - off < missingValue.length && missingValue(newoff - off) == line(newoff)) {
        newoff += 1
      }

      if (newoff - off == missingValue.length && (line.length == newoff || line(newoff) == sep)) {
        rvb.setMissing()
      } else {
        fatal(s"Error parsing matrix. Invalid Int64 at column: $colNum, row: $rowNum in file: $file")
      }
    } else if (line.length == newoff || line(newoff) == sep) {
      if (isNegative) rvb.addLong(-v) else rvb.addLong(v)
    } else {
      fatal(s"Error parsing matrix. Invalid Int64 at column: $colNum, row: $rowNum in file: $file")
    }
    newoff + 1
  }

  def addFloat(line: String, rowNum: Long, colNum: Int, off: Int): Int = {
    var newoff = line.indexOf(sep, off)
    if (newoff == -1)
      newoff = line.length
    val v = line.substring(off, newoff)
    if (v == missingValue) {
      rvb.setMissing()
    } else {
      try {
        rvb.addFloat(v.toFloat)
      } catch {
        case _: NumberFormatException => fatal(s"Error parsing matrix: $v is not a Float32. column: $colNum, row: $rowNum in file: $file")
      }
    }
    newoff + 1
  }

  def addDouble(line: String, rowNum: Long, colNum: Int, off: Int): Int = {
    var newoff = line.indexOf(sep, off)
    if (newoff == -1)
      newoff = line.length
    val v = line.substring(off, newoff)
    if (v == missingValue) {
      rvb.setMissing()
    } else {
      try {
        rvb.addDouble(v.toDouble)
      } catch {
        case _: NumberFormatException => fatal(s"Error parsing matrix: $v is not a Float64. column: $colNum, row: $rowNum in file: $file")
      }
    }
    newoff + 1
  }
}

object LoadMatrix {

  def warnDuplicates(ids: Array[String]) {
    val duplicates = ids.counter().filter(_._2 > 1)
    if (duplicates.nonEmpty) {
      warn(s"Found ${ duplicates.size } duplicate ${ plural(duplicates.size, "sample ID") }:\n  @1",
        duplicates.toArray.sortBy(-_._2).map { case (id, count) => s"""($count) "$id"""" }.truncatable("\n  "))
    }
  }

  def parseHeader(hConf: Configuration, file: String, sep: Char, nRowFields: Int, noHeader: Boolean): (Array[String], Int) = {
    if (noHeader) {
      val nCols = hConf.readFile(file) { s => Source.fromInputStream(s).getLines().next() }.count(_ == sep) + 1
      (Array(), nCols - nRowFields)
    } else {
      val lines = hConf.readFile(file) { s => Source.fromInputStream(s).getLines().take(2).toArray }
      lines match {
        case Array(header, first) =>
          val nCols = first.split(sep).length - nRowFields
          if (nCols <= 0)
            fatal(s"More row fields ($nRowFields) than columns (${ nRowFields + nCols }) in file: $file")
          (header.split(sep), nCols)
        case _ =>
          fatal(s"file in import_matrix contains no data: $file")
      }
    }
  }

  def splitHeader(cols: Array[String], nRowFields: Int, nColIDs: Int): (Array[String], Array[String]) = {
    if (cols.length == nColIDs) {
      (Array.tabulate(nRowFields)(i => s"f$i"), cols)
    } else if (cols.length == nColIDs + nRowFields) {
      (cols.take(nRowFields), cols.drop(nRowFields))
    } else if (cols.isEmpty) {
      (Array.tabulate(nRowFields)(i => s"f$i"), Array.tabulate(nColIDs)(i => s"col$i"))
    } else
      fatal(
        s"""Expected file header to contain all $nColIDs column IDs and
            | optionally all $nRowFields row field names: found ${ cols.length } header elements.
           """.stripMargin)
  }

  def makePartitionerFromCounts(partitionCounts: Array[Long], pkType: TStruct): (OrderedRVDPartitioner, Array[Int]) = {
    var includeStart = true
    val keepPartitions = new ArrayBuilder[Int]()
    val rangeBoundIntervals = partitionCounts.zip(partitionCounts.tail).zipWithIndex.flatMap { case ((s, e), i) =>
      val interval = Interval(Row(if (includeStart) s else s - 1), Row(e - 1), includeStart, true)
      includeStart = false
      if (interval.isEmpty(pkType.ordering))
        None
      else {
        keepPartitions += i
        Some(interval)
      }
    }
    val ranges = UnsafeIndexedSeq(TArray(TInterval(pkType)), rangeBoundIntervals)
    (new OrderedRVDPartitioner(Array(pkType.fieldNames(0)), pkType, ranges), keepPartitions.result())
  }

  def verifyRowFields(fieldNames: Array[String], fieldTypes: Map[String, Type]): TStruct = {
    val headerDups = fieldNames.duplicates()
    if (headerDups.nonEmpty)
      fatal(s"Found following duplicate row fields in header: \n    ${ headerDups.mkString("\n    ") }")

    val fields: Array[(String, Type)] = fieldNames.map { name =>
      fieldTypes.get(name) match {
        case Some(t) => (name, t)
        case None => fatal(
          s"""row field $name not found in provided row_fields dictionary.
             |    expected fields:
             |      ${ fieldNames.mkString("\n      ") }
             |    found fields:
             |      ${ fieldTypes.keys.mkString("\n      ") }
           """.stripMargin)
      }
    }
    TStruct(fields: _*)
  }

  def apply(hc: HailContext,
    files: Array[String],
    rowFields: Map[String, Type],
    keyFields: Array[String],
    cellType: TStruct = TStruct("x" -> TInt64()),
    missingValue: String = "NA",
    nPartitions: Option[Int] = None,
    noHeader: Boolean = false): MatrixTable = {

    require(cellType.size == 1, "cellType can only have 1 field")

    val sep = '\t'
    val nAnnotations = rowFields.size

      assert(rowFields.values.forall { t =>
        t.isOfType(TString()) ||
          t.isOfType(TInt32()) ||
          t.isOfType(TInt64()) ||
          t.isOfType(TFloat32()) ||
          t.isOfType(TFloat64())
      })
    val sc = hc.sc
    val hConf = hc.hadoopConf

    val (header1, nCols) = parseHeader(hConf, files.head, sep, nAnnotations, noHeader)
    val (rowFieldNames, colIDs) = splitHeader(header1, nAnnotations, nCols)

    val rowFieldType: TStruct = verifyRowFields(rowFieldNames, rowFields)

    val header1Bc = sc.broadcast(header1)

    LoadMatrix.warnDuplicates(colIDs)

    val lines = sc.textFilesLines(files, nPartitions.getOrElse(sc.defaultMinPartitions))

    val fileByPartition = lines.partitions.map(p => partitionPath(p))
    val firstPartitions = fileByPartition.scanLeft(0) { (i, file) => if (fileByPartition(i) == file) i else i + 1 }.tail

    val partitionCounts = lines.filter(l => l.value.nonEmpty)
      .mapPartitionsWithIndex { (i, it) =>
        if (firstPartitions(i) == i) {
          if (!noHeader) {
            val hd1 = header1Bc.value
            val hd = it.next().value.split(sep)
            if (!hd1.sameElements(hd)) {
              if (hd1.length != hd.length) {
                fatal(
                  s"""invalid header: lengths of headers differ.
                     |    ${ hd1.length } elements in ${ files(0) }
                     |    ${ hd.length } elements in ${ fileByPartition(i) }
               """.stripMargin
                )
              }
              hd1.zip(hd).zipWithIndex.foreach { case ((s1, s2), j) =>
                if (s1 != s2) {
                  fatal(
                    s"""invalid header: expected elements to be identical for all input files. Found different elements at position $j.
                       |    ${ files(0) }: $s1
                       |    ${ fileByPartition(i) }: $s2""".
                      stripMargin)
                }
              }
            }
          }
        }
        it
      }.countPerPartition().scanLeft(0L)(_ + _)

    val useIndex = keyFields.isEmpty
    val (rowKey, rowType) =
      if (useIndex)
        (Array("row_id"),TStruct("row_id" -> TInt64()) ++ rowFieldType)
      else (keyFields, rowFieldType)

    val matrixType = MatrixType.fromParts(
      TStruct.empty(),
      colType = TStruct("col_id" -> TString()),
      colKey = Array("col_id"),
      rowType = rowType,
      rowKey = rowKey.toFastIndexedSeq,
      rowPartitionKey = rowKey.toFastIndexedSeq,
      entryType = cellType)

    val rdd = lines.filter(l => l.value.nonEmpty)
      .mapPartitionsWithIndex { (i, it) =>
        val region = Region()
        val rvb = new RegionValueBuilder(region)
        val rv = RegionValue(region)

        if (firstPartitions(i) == i && !noHeader) { it.next() }

        val partitionStartInFile = partitionCounts(i) - partitionCounts(firstPartitions(i))
        val parser = new LoadMatrixParser(rvb, rowFieldType.fieldType, cellType, nCols, missingValue, fileByPartition(i))

        it.zipWithIndex.map { case (v, row) =>
          val fileRowNum = partitionStartInFile + row
          val line = v.value

          region.clear()
          rvb.start(matrixType.rvRowType)
          rvb.startStruct()
          if (useIndex) {
            rvb.addLong(partitionCounts(i) + row)
          }
          parser.parseLine(line, fileRowNum)
          rvb.endStruct()
          rv.setOffset(rvb.end())
          rv
        }
      }

    val orderedRVD = if (useIndex) {
      val (partitioner, keepPartitions) = makePartitionerFromCounts(partitionCounts, matrixType.orvdType.pkType)
      OrderedRVD(matrixType.orvdType, partitioner, rdd.subsetPartitions(keepPartitions))
    } else
      OrderedRVD(matrixType.orvdType, rdd, None, None)

    new MatrixTable(hc,
      matrixType,
      Annotation.empty,
      colIDs.map(x => Annotation(x)),
      orderedRVD)
  }
}
