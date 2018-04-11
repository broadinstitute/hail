package is.hail.annotations

import is.hail.expr._
import is.hail.expr.types._
import is.hail.utils.{ArrayBuilder, Interval}
import is.hail.variant._
import is.hail.utils._
import org.apache.spark.sql.Row

object Annotation {

  final val COL_HEAD = "sa"

  final val ROW_HEAD = "va"

  final val GLOBAL_HEAD = "global"

  final val ENTRY_HEAD = "g"

  val empty: Annotation = Row()

  def emptyIndexedSeq(n: Int): IndexedSeq[Annotation] = Array.fill[Annotation](n)(Annotation.empty)

  def printAnnotation(a: Any, nSpace: Int = 0): String = {
    val spaces = " " * nSpace
    a match {
      case null => "Null"
      case r: Row =>
        "Struct:\n" +
          r.toSeq.zipWithIndex.map { case (elem, index) =>
            s"""$spaces[$index] ${ printAnnotation(elem, nSpace + 4) }"""
          }
            .mkString("\n")
      case a => a.toString + ": " + a.getClass.getSimpleName
    }
  }

  def apply(args: Any*): Annotation = Row.fromSeq(args)

  def fromSeq(values: Seq[Any]): Annotation = Row.fromSeq(values)

  def buildInserter(code: String, t: TStruct, ec: EvalContext, expectedHead: String): (TStruct, Inserter) = {
    val (paths, types, f) = Parser.parseAnnotationExprs(code, ec, Some(expectedHead))

    val inserterBuilder = new ArrayBuilder[Inserter]()
    val finalType = (paths, types).zipped.foldLeft(t) { case (t, (ids, signature)) =>
      val (s, i) = t.structInsert(signature, ids)
      inserterBuilder += i
      s
    }

    val inserters = inserterBuilder.result()

    val insF = (left: Annotation, right: Annotation) => {
      ec.setAll(left, right)

      var newAnnotation = left
      val queries = f()
      queries.indices.foreach { i =>
        newAnnotation = inserters(i)(newAnnotation, queries(i))
      }
      newAnnotation
    }

    (finalType, insF)
  }

  def copy(t: Type, a: Annotation): Annotation = {
    if (a == null)
      return null

    t match {
      case t: TBaseStruct =>
        val r = a.asInstanceOf[Row]
        Row(Array.tabulate(r.size)(i => Annotation.copy(t.types(i), r(i))): _*)

      case t: TArray =>
        a.asInstanceOf[IndexedSeq[Annotation]].map(Annotation.copy(t.elementType, _))

      case t: TSet =>
        a.asInstanceOf[Set[Annotation]].map(Annotation.copy(t.elementType, _))

      case t: TDict =>
        a.asInstanceOf[Map[Annotation, Annotation]]
          .map { case (k, v) => (Annotation.copy(t.keyType, k), Annotation.copy(t.valueType, v)) }

      case t: TInterval =>
        val i = a.asInstanceOf[Interval]
        i.copy(start = Annotation.copy(t.pointType, i.start), end = Annotation.copy(t.pointType, i.end))

      case _ => a
    }
  }

  def isSafe(typ: Type, a: Annotation): Boolean = {
    a == null || (typ match {
      case t: TBaseStruct =>
        val r = a.asInstanceOf[Row]
        !r.isInstanceOf[UnsafeRow] && Array.range(0, t.size).forall(i => Annotation.isSafe(t.types(i), r(i)))

      case t: TArray =>
        !a.isInstanceOf[UnsafeIndexedSeq] && a.asInstanceOf[IndexedSeq[Annotation]].forall(Annotation.isSafe(t.elementType, _))

      case t: TSet =>
        a.asInstanceOf[Set[Annotation]].forall(Annotation.isSafe(t.elementType, _))

      case t: TDict =>
        a.asInstanceOf[Map[Annotation, Annotation]]
          .forall { case (k, v) => Annotation.isSafe(t.keyType, k) && Annotation.isSafe(t.valueType, v) }

      case t: TInterval =>
        val i = a.asInstanceOf[Interval]
        Annotation.isSafe(t.pointType, i.start) && Annotation.isSafe(t.pointType, i.end)

      case _ => true
    })
  }

  def safeFromRegionValue(t: Type, rv: RegionValue): Annotation =
    safeFromRegionValue(t, rv.region, rv.offset)

  def safeFromRegionValue(t: Type, region: Region, offset: Long): Annotation =
    Annotation.copy(t, UnsafeRow.read(t, region, offset))

  def safeFromArrayRegionValue(
    t: TContainer,
    region: Region,
    offset: Long
  ): IndexedSeq[Annotation] =
    Annotation.copy(t, UnsafeRow.readArray(t, region, offset))
      .asInstanceOf[IndexedSeq[Annotation]]

  def safeFromBaseStructRegionValue(
    t: TBaseStruct,
    region: Region,
    offset: Long
  ): Row =
    Annotation.copy(t, UnsafeRow.readBaseStruct(t, region, offset))
      .asInstanceOf[Row]
}
