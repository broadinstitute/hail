package is.hail.methods

import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.ir.{IRParser, MatrixValue, TableValue}
import is.hail.expr.ir.functions.{MatrixToMatrixFunction, TableToTableFunction}
import is.hail.expr.types.virtual.{TArray, TInterval, Type}
import is.hail.expr.types.{MatrixType, TableType}
import is.hail.rvd.{RVDPartitioner, RVDType}
import is.hail.utils.Interval
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JBool, JObject, JString}

class MatrixFilterItervalsSerializer extends CustomSerializer[MatrixFilterIntervals](format => (
  { case JObject(fields) =>
      val fieldMap = fields.toMap

      val keyType = IRParser.parseType(fieldMap("keyType").asInstanceOf[JString].s)
      val intervals = JSONAnnotationImpex.importAnnotation(fieldMap("intervals"), TArray(TInterval(keyType)))
      MatrixFilterIntervals(
        keyType,
        intervals.asInstanceOf[IndexedSeq[Interval]].toArray,
        fieldMap("keep").asInstanceOf[JBool].value)
  },
  { case fi: MatrixFilterIntervals =>
      JObject("keyType" -> JString(fi.keyType.toString),
        "intervals" -> JSONAnnotationImpex.exportAnnotation(fi.intervals.toFastIndexedSeq, TArray(TInterval(fi.keyType))),
        "keep" -> JBool(fi.keep))
  }))

case class MatrixFilterIntervals(
  keyType: Type,
  intervals: Array[Interval],
  keep: Boolean) extends MatrixToMatrixFunction {
  def preservesPartitionCounts: Boolean = false

  def typeInfo(childType: MatrixType, childRVDType: RVDType): (MatrixType, RVDType) = {
    (childType, childRVDType)
  }

  def execute(mv: MatrixValue): MatrixValue = {
    val partitioner = RVDPartitioner.union(
      mv.rvd.typ.kType.virtualType,
      intervals,
      mv.rvd.typ.key.length - 1)
    MatrixValue(mv.typ, mv.globals, mv.colValues, mv.rvd.filterIntervals(partitioner, keep))
  }
}

class TableFilterItervalsSerializer extends CustomSerializer[TableFilterIntervals](format => (
  { case JObject(fields) =>
    val fieldMap = fields.toMap

    val keyType = IRParser.parseType(fieldMap("keyType").asInstanceOf[JString].s)
    val intervals = JSONAnnotationImpex.importAnnotation(fieldMap("intervals"), TArray(TInterval(keyType)))
    TableFilterIntervals(
      keyType,
      intervals.asInstanceOf[IndexedSeq[Interval]].toArray,
      fieldMap("keep").asInstanceOf[JBool].value)
  },
  { case fi: TableFilterIntervals =>
    JObject("keyType" -> JString(fi.keyType.toString),
      "intervals" -> JSONAnnotationImpex.exportAnnotation(fi.intervals.toFastIndexedSeq, TArray(TInterval(fi.keyType))),
      "keep" -> JBool(fi.keep))
  }))

case class TableFilterIntervals(
  keyType: Type,
  intervals: Array[Interval],
  keep: Boolean) extends TableToTableFunction {
  def preservesPartitionCounts: Boolean = false

  def typeInfo(childType: TableType, childRVDType: RVDType): (TableType, RVDType) = {
    (childType, childRVDType)
  }

  def execute(tv: TableValue): TableValue = {
    val partitioner = RVDPartitioner.union(
      tv.typ.keyType,
      intervals,
      tv.rvd.typ.key.length - 1)
    TableValue(tv.typ, tv.globals, tv.rvd.filterIntervals(partitioner, keep))
  }
}
