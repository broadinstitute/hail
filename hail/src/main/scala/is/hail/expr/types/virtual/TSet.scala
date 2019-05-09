package is.hail.expr.types.virtual

import is.hail.annotations.{Annotation, ExtendedOrdering}
import is.hail.check.Gen
import is.hail.expr.types.physical.PSet
import org.json4s.jackson.JsonMethods

import scala.collection.immutable.TreeSet
import scala.reflect.{ClassTag, classTag}

final case class TSet(elementType: Type, override val required: Boolean = false) extends TContainer {
  lazy val physicalType: PSet = PSet(elementType.physicalType, required)

  override lazy val fundamentalType: TArray = TArray(elementType.fundamentalType, required)

  def _toPretty = s"Set[$elementType]"

  override def pyString(sb: StringBuilder): Unit = {
    sb.append("set<")
    elementType.pyString(sb)
    sb.append('>')
  }

  override def canCompare(other: Type): Boolean = other match {
    case TSet(otherType, _) => elementType.canCompare(otherType)
    case _ => false
  }

  override def unify(concrete: Type): Boolean = concrete match {
    case TSet(celementType, _) => elementType.unify(celementType)
    case _ => false
  }

  override def subst() = TSet(elementType.subst())

  def _typeCheck(a: Any): Boolean =
    a.isInstanceOf[TreeSet[_]] && a.asInstanceOf[TreeSet[_]].forall(elementType.typeCheck)

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean = false) {
    sb.append("Set[")
    elementType.pretty(sb, indent, compact)
    sb.append("]")
  }

  lazy val ordering: ExtendedOrdering = ExtendedOrdering.setOrdering(elementType.ordering)

  def literal(elems: Any*): TreeSet[Any] = TreeSet(elems: _*)(elementType.ordering.toTotalOrdering)

  def toLiteral(elems: IndexedSeq[Any]): TreeSet[Any] = literal(elems: _*)

  override def str(a: Annotation): String = JsonMethods.compact(toJSON(a))

  override def genNonmissingValue: Gen[Annotation] = Gen.treeSetOf[Any](elementType.genValue)(elementType.ordering.toTotalOrdering)

  override def scalaClassTag: ClassTag[Set[AnyRef]] = classTag[Set[AnyRef]]
}
