package is.hail.expr.types.virtual

import is.hail.annotations.ExtendedOrdering
import is.hail.expr.types.Box
import is.hail.expr.types.physical.PType

import scala.collection.mutable
import scala.reflect.ClassTag

object TVariable {
  val condMap: Map[String, (Type) => Boolean] = Map(
    "numeric" -> ((t: Type) => t.isInstanceOf[TNumeric]),
    "int32" -> ((t: Type) => t.isInstanceOf[TInt32]),
    "int64" -> ((t: Type) => t.isInstanceOf[TInt64]),
    "float32" -> ((t: Type) => t.isInstanceOf[TFloat32]),
    "float64" -> ((t: Type) => t.isInstanceOf[TFloat64]),
    "locus" -> ((t: Type) => t.isInstanceOf[TLocus]),
    "struct" -> ((t: Type) => t.isInstanceOf[TStruct]),
    "tuple" -> ((t: Type) => t.isInstanceOf[TTuple]),
    "ndarray" -> ((t: Type) => t.isInstanceOf[TNDArray]))

  private[this] val namedBoxes: mutable.Map[String, Box[Type]] = mutable.Map()

  def fromName(name: String): Box[Type] = this.synchronized {
    namedBoxes.get(name) match {
      case Some(b) => b
      case None =>
        val b = Box[Type](matchCond = _.isOfType(_))
        namedBoxes(name) = b
        b
    }
  }
}

final case class TVariable(name: String, cond: String = null) extends Type {
  private[this] val b = TVariable.fromName(name)

  private[this] val condf: (Type) => Boolean =
    if (cond != null)
      TVariable.condMap(cond)
    else
      (t: Type) => true

  def physicalType: PType = ???

  def t: Type = b.get

  override val required = true

  override def _toPretty: String =
    if (cond != null)
      s"?$name:$cond"
    else
      s"?$name"

  override def pyString(sb: StringBuilder): Unit = {
    sb.append(_toPretty)
  }

  override def isRealizable = false

  def _typeCheck(a: Any): Boolean =
    throw new RuntimeException("TVariable is not realizable")

  override def unify(concrete: Type): Boolean =
    concrete.isRealizable && condf(concrete) && b.unify(concrete)

  override def isBound: Boolean = b.isEmpty

  override def clear() {
    b.clear()
  }

  override def subst(): Type = {
    assert(b.isDefined)
    t
  }

  override def scalaClassTag: ClassTag[AnyRef] = throw new RuntimeException("TVariable is not realizable")

  val ordering: ExtendedOrdering = null
}
