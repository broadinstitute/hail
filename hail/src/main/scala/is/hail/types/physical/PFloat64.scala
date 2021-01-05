package is.hail.types.physical

import is.hail.annotations._
import is.hail.asm4s.{Code, _}
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder}
import is.hail.types.physical.stypes.primitives.{SFloat64, SFloat64Code}
import is.hail.types.physical.stypes.{SCode, SType}
import is.hail.types.virtual.TFloat64

case object PFloat64Optional extends PFloat64(false)

case object PFloat64Required extends PFloat64(true)

class PFloat64(override val required: Boolean) extends PNumeric with PPrimitive {
  lazy val virtualType: TFloat64.type = TFloat64

  override type NType = PFloat64

  def _asIdent = "float64"

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean): Unit = sb.append("PFloat64")

  override def unsafeOrdering(): UnsafeOrdering = new UnsafeOrdering {
    def compare(o1: Long, o2: Long): Int = {
      java.lang.Double.compare(Region.loadDouble(o1), Region.loadDouble(o2))
    }
  }

  def codeOrdering(mb: EmitMethodBuilder[_], other: PType): CodeOrdering = {
    assert(other isOfType this)
    new CodeOrdering {
      def compareNonnull(cb: EmitCodeBuilder, x: PCode, y: PCode): Code[Int] =
        Code.invokeStatic2[java.lang.Double, Double, Double, Int]("compare", x.tcode[Double], y.tcode[Double])

      def ltNonnull(cb: EmitCodeBuilder, x: PCode, y: PCode): Code[Boolean] = x.tcode[Double] < y.tcode[Double]

      def lteqNonnull(cb: EmitCodeBuilder, x: PCode, y: PCode): Code[Boolean] = x.tcode[Double] <= y.tcode[Double]

      def gtNonnull(cb: EmitCodeBuilder, x: PCode, y: PCode): Code[Boolean] = x.tcode[Double] > y.tcode[Double]

      def gteqNonnull(cb: EmitCodeBuilder, x: PCode, y: PCode): Code[Boolean] = x.tcode[Double] >= y.tcode[Double]

      def equivNonnull(cb: EmitCodeBuilder, x: PCode, y: PCode): Code[Boolean] = x.tcode[Double].ceq(y.tcode[Double])
    }
  }

  override def byteSize: Long = 8

  override def zero = coerce[PFloat64](const(0.0))

  override def add(a: Code[_], b: Code[_]): Code[PFloat64] = {
    coerce[PFloat64](coerce[Double](a) + coerce[Double](b))
  }

  override def multiply(a: Code[_], b: Code[_]): Code[PFloat64] = {
    coerce[PFloat64](coerce[Double](a) * coerce[Double](b))
  }

  override def sType: SType = SFloat64(required)

  def storePrimitiveAtAddress(cb: EmitCodeBuilder, addr: Code[Long], value: SCode): Unit =
    cb.append(Region.storeDouble(addr, value.asDouble.doubleCode(cb)))

  override def loadCheapPCode(cb: EmitCodeBuilder, addr: Code[Long]): PCode = new SFloat64Code(required, Region.loadDouble(addr))
}

object PFloat64 {
  def apply(required: Boolean = false): PFloat64 = if (required) PFloat64Required else PFloat64Optional

  def unapply(t: PFloat64): Option[Boolean] = Option(t.required)
}
