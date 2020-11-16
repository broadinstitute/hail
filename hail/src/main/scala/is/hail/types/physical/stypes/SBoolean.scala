package is.hail.types.physical.stypes

import is.hail.annotations.{CodeOrdering, Region}
import is.hail.asm4s.{BooleanInfo, Code, Settable, SettableBuilder, TypeInfo, Value}
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder, SortOrder}
import is.hail.types.physical.{PBoolean, PCanonicalCall, PCode, PSettable, PType, PValue}
import is.hail.utils.FastIndexedSeq


case class SBoolean(required: Boolean) extends SType {
  override def pType: PBoolean = PBoolean(required)

  def codeOrdering(mb: EmitMethodBuilder[_], other: SType, so: SortOrder): CodeOrdering = pType.codeOrdering(mb, other.pType, so)

  def coerceOrCopy(cb: EmitCodeBuilder, region: Value[Region], value: PCode, deepCopy: Boolean): PCode = {
    value.st match {
      case SBoolean(_) =>
        value.asInstanceOf[SBooleanCode]
    }
  }

  def codeTupleTypes(): IndexedSeq[TypeInfo[_]] = FastIndexedSeq(BooleanInfo)

  def loadFrom(cb: EmitCodeBuilder, region: Value[Region], pt: PType, addr: Code[Long]): PCode = {
    pt match {
      case PBoolean(_) =>
        new SBooleanCode(required: Boolean, Region.loadBoolean(addr))
    }
  }
}

class SBooleanCode(required: Boolean, val code: Code[Boolean]) extends PCode {
  val pt: PBoolean = PBoolean(required)

  def st: SBoolean = SBoolean(required)

  def codeTuple(): IndexedSeq[Code[_]] = FastIndexedSeq(code)

  private[this] def memoizeWithBuilder(cb: EmitCodeBuilder, name: String, sb: SettableBuilder): SBooleanSettable = {
    val s = new SBooleanSettable(required, sb.newSettable[Boolean]("sboolean_memoize"))
    s.store(cb, this)
    s
  }

  def memoize(cb: EmitCodeBuilder, name: String): SBooleanSettable = memoizeWithBuilder(cb, name, cb.localBuilder)

  def memoizeField(cb: EmitCodeBuilder, name: String): SBooleanSettable = memoizeWithBuilder(cb, name, cb.fieldBuilder)

  def boolValue(cb: EmitCodeBuilder): Code[Boolean] = code
}

object SBooleanSettable {
  def apply(sb: SettableBuilder, name: String, required: Boolean): SBooleanSettable = {
    new SBooleanSettable(required, sb.newSettable[Boolean](name))
  }
}

class SBooleanSettable(required: Boolean, x: Settable[Boolean]) extends PValue with PSettable {
  val pt: PBoolean = PBoolean(required)

  def st: SBoolean = SBoolean(required)

  def store(cb: EmitCodeBuilder, v: PCode): Unit = cb.assign(x, v.asBoolean.boolValue(cb))

  def settableTuple(): IndexedSeq[Settable[_]] = FastIndexedSeq(x)

  def get: PCode = new SBooleanCode(required, x)

  def boolValue(cb: EmitCodeBuilder): Code[Boolean] = x
}