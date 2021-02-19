package is.hail.types.virtual

import is.hail.annotations.ExtendedOrdering
import is.hail.types.Box
import is.hail.types.physical.PType

import scala.collection.mutable
import scala.reflect.ClassTag
import java.util.function.Supplier

object TVariable {
  val condMap: Map[String, (Type) => Boolean] = Map(
    "numeric" -> ((t: Type) => t.isInstanceOf[TNumeric]),
    "int32" -> ((t: Type) => t == TInt32),
    "int64" -> ((t: Type) => t == TInt64),
    "float32" -> ((t: Type) => t == TFloat32),
    "float64" -> ((t: Type) => t == TFloat64),
    "locus" -> ((t: Type) => t.isInstanceOf[TLocus]),
    "struct" -> ((t: Type) => t.isInstanceOf[TStruct]),
    "tuple" -> ((t: Type) => t.isInstanceOf[TTuple]))

  private[this] val namedBoxes = ThreadLocal.withInitial(new Supplier[mutable.Map[String, Box[Type]]]() {
    def get(): mutable.Map[String, Box[Type]] = {
      mutable.Map()
    }
  })

  def fromName(name: String): Box[Type] = this.synchronized {
    val m = namedBoxes.get
    m.get(name) match {
      case Some(b) => b
      case None =>
        val b = Box[Type](matchCond = _ == _)
        m(name) = b
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

  def t: Type = b.get

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

  def mkOrdering(missingEqual: Boolean): ExtendedOrdering = null
}
