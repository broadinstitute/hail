package is.hail.types.physical
import is.hail.annotations.{CodeOrdering, Region, UnsafeOrdering}
import is.hail.asm4s.{Code, MethodBuilder, Value}
import is.hail.expr.ir.EmitMethodBuilder
import is.hail.types.virtual.{TVoid, Type}

case object PVoid extends PType with PUnrealizable {
  def virtualType: Type = TVoid

  override val required = true

  def _asIdent = "void"

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean): Unit = sb.append("PVoid")

  def setRequired(required: Boolean) = PVoid

  override def unsafeOrdering(): UnsafeOrdering = throw new NotImplementedError()
}
