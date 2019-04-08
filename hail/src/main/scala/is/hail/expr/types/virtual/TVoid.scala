package is.hail.expr.types.virtual

import is.hail.annotations.ExtendedOrdering
import is.hail.expr.types.physical.PVoid

case object TVoid extends Type {
  def physicalType: PVoid.type = PVoid

  override val required = true

  override def _toPretty = "Void"

  override def pyString(sb: StringBuilder): Unit = {
    sb.append("void")
  }

  val ordering: ExtendedOrdering = null

  override def scalaClassTag: scala.reflect.ClassTag[_ <: AnyRef] = throw new UnsupportedOperationException("No ClassTag for Void")

  override def _typeCheck(a: Any): Boolean = true

  override def isRealizable = false
}
