package is.hail.types.physical.stypes.primitives

import is.hail.annotations.{CodeOrdering, Region}
import is.hail.asm4s.{Code, DoubleInfo, LineNumber, Settable, SettableBuilder, TypeInfo, Value}
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder, SortOrder}
import is.hail.types.physical.stypes.{SCode, SType}
import is.hail.types.physical.{PCode, PFloat64, PSettable, PType, PValue}
import is.hail.utils.FastIndexedSeq

case class SFloat64(required: Boolean) extends SType {
  override def pType: PFloat64  = PFloat64(required)

  def codeOrdering(mb: EmitMethodBuilder[_], other: SType, so: SortOrder): CodeOrdering = pType.codeOrdering(mb, other.pType, so)

  def coerceOrCopy(cb: EmitCodeBuilder, region: Value[Region], value: SCode, deepCopy: Boolean)(implicit line: LineNumber): SCode = {
    value.st match {
      case SFloat64(r) =>
        if (r == required)
          value
        else
          new SFloat64Code(required, value.asInstanceOf[SFloat64Code].code)
    }
  }

  def codeTupleTypes(): IndexedSeq[TypeInfo[_]] = FastIndexedSeq(DoubleInfo)

  def loadFrom(cb: EmitCodeBuilder, region: Value[Region], pt: PType, addr: Code[Long])(implicit line: LineNumber): SCode = {
    pt match {
      case _: PFloat64 =>
        new SFloat64Code(required, Region.loadDouble(addr))
    }
  }
}

trait PFloat64Value extends PValue {
  def doubleCode(cb: EmitCodeBuilder)(implicit line: LineNumber): Code[Double]

}

class SFloat64Code(required: Boolean, val code: Code[Double]) extends PCode {
  val pt: PFloat64 = PFloat64(required)

  def st: SFloat64 = SFloat64(required)

  def codeTuple(): IndexedSeq[Code[_]] = FastIndexedSeq(code)

  private[this] def memoizeWithBuilder(cb: EmitCodeBuilder, name: String, sb: SettableBuilder)(implicit line: LineNumber): PFloat64Value = {
    val s = new SFloat64Settable(required, sb.newSettable[Double]("sint64_memoize"))
    s.store(cb, this)
    s
  }

  def memoize(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PFloat64Value =
    memoizeWithBuilder(cb, name, cb.localBuilder)

  def memoizeField(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PFloat64Value =
    memoizeWithBuilder(cb, name, cb.fieldBuilder)

  def doubleCode(cb: EmitCodeBuilder): Code[Double] = code
}

object SFloat64Settable {
  def apply(sb: SettableBuilder, name: String, required: Boolean): SFloat64Settable = {
    new SFloat64Settable(required, sb.newSettable[Double](name))
  }
}

class SFloat64Settable(required: Boolean, x: Settable[Double]) extends PFloat64Value with PSettable {
  val pt: PFloat64 = PFloat64(required)

  def st: SFloat64 = SFloat64(required)

  def store(cb: EmitCodeBuilder, v: PCode)(implicit line: LineNumber): Unit = cb.assign(x, v.asDouble.doubleCode(cb))

  def settableTuple(): IndexedSeq[Settable[_]] = FastIndexedSeq(x)

  def get(implicit line: LineNumber): PCode = new SFloat64Code(required, x)

  def doubleCode(cb: EmitCodeBuilder)(implicit line: LineNumber): Code[Double] = x
}