package is.hail.expr.types.virtual

import is.hail.annotations.{Annotation, ExtendedOrdering}
import is.hail.check.Gen
import org.json4s.jackson.JsonMethods

import scala.reflect.{ClassTag, classTag}

final case class TStream(elementType: Type) extends TIterable {
  override def pyString(sb: StringBuilder): Unit = {
    sb.append("stream<")
    elementType.pyString(sb)
    sb.append('>')
  }
  override val fundamentalType: TStream = {
    if (elementType == elementType.fundamentalType)
      this
    else
      this.copy(elementType = elementType.fundamentalType)
  }

  def _toPretty = s"Stream[$elementType]"

  override def canCompare(other: Type): Boolean =
    throw new UnsupportedOperationException("Stream comparison is currently undefined.")

  override def unify(concrete: Type): Boolean = concrete match {
    case TStream(celementType) => elementType.unify(celementType)
    case _ => false
  }

  override def subst() = TStream(elementType.subst())

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean = false) {
    sb.append("Stream[")
    elementType.pretty(sb, indent, compact)
    sb.append("]")
  }

  def _typeCheck(a: Any): Boolean = a.isInstanceOf[IndexedSeq[_]] &&
    a.asInstanceOf[IndexedSeq[_]].forall(elementType.typeCheck)

  override def str(a: Annotation): String = JsonMethods.compact(toJSON(a))

  override def isRealizable = false

  override def genNonmissingValue: Gen[Annotation] =
    throw new UnsupportedOperationException("Streams don't have associated annotations.")

  lazy val ordering: ExtendedOrdering =
    throw new UnsupportedOperationException("Stream comparison is currently undefined.")

  override def scalaClassTag: ClassTag[Iterator[AnyRef]] = classTag[Iterator[AnyRef]]
}

