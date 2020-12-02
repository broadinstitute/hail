package is.hail.types.physical.stypes.concrete

import is.hail.annotations.{CodeOrdering, Region}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder, IEmitCode, SortOrder}
import is.hail.types.physical.stypes.interfaces.{SStruct, SStructSettable}
import is.hail.types.physical.stypes.{SCode, SType}
import is.hail.types.physical.{PBaseStruct, PBaseStructCode, PBaseStructValue, PCode, PStructSettable, PType}
import is.hail.utils.FastIndexedSeq


case class SBaseStructPointer(pType: PBaseStruct) extends SStruct {
  def codeOrdering(mb: EmitMethodBuilder[_], other: SType, so: SortOrder): CodeOrdering = pType.codeOrdering(mb, other.pType, so)

  def coerceOrCopy(cb: EmitCodeBuilder, region: Value[Region], value: SCode, deepCopy: Boolean)(implicit line: LineNumber): SCode = {
    new SBaseStructPointerCode(this, pType.store(cb, region, value, deepCopy))
  }

  def codeTupleTypes(): IndexedSeq[TypeInfo[_]] = FastIndexedSeq(LongInfo)

  def loadFrom(cb: EmitCodeBuilder, region: Value[Region], pt: PType, addr: Code[Long])(implicit line: LineNumber): SCode = {
    if (pt == this.pType)
      new SBaseStructPointerCode(this, addr)
    else
      coerceOrCopy(cb, region, pt.loadCheapPCode(cb, addr), deepCopy = false)
  }
}


object SBaseStructPointerSettable {
  def apply(sb: SettableBuilder, st: SBaseStructPointer, name: String): SBaseStructPointerSettable = {
    new SBaseStructPointerSettable(st, sb.newSettable(name))
  }
}

class SBaseStructPointerSettable(
  val st: SBaseStructPointer,
  val a: Settable[Long]
) extends PStructSettable {
  val pt: PBaseStruct = st.pType

  def get(implicit line: LineNumber): PBaseStructCode = new SBaseStructPointerCode(st, a)

  def settableTuple(): IndexedSeq[Settable[_]] = FastIndexedSeq(a)

  def loadField(cb: EmitCodeBuilder, fieldIdx: Int)(implicit line: LineNumber): IEmitCode = {
    IEmitCode(cb,
      pt.isFieldMissing(a, fieldIdx),
      pt.fields(fieldIdx).typ.loadCheapPCode(cb, pt.loadField(a, fieldIdx)))
  }

  def store(cb: EmitCodeBuilder, pv: PCode)(implicit line: LineNumber): Unit = {
    cb.assign(a, pv.asInstanceOf[SBaseStructPointerCode].a)
  }

  def isFieldMissing(fieldIdx: Int)(implicit line: LineNumber): Code[Boolean] = {
    pt.isFieldMissing(a, fieldIdx)
  }
}

class SBaseStructPointerCode(val st: SBaseStructPointer, val a: Code[Long]) extends PBaseStructCode {
  val pt: PBaseStruct = st.pType

  def code: Code[_] = a

  def codeTuple(): IndexedSeq[Code[_]] = FastIndexedSeq(a)

  def memoize(cb: EmitCodeBuilder, name: String, sb: SettableBuilder)(implicit line: LineNumber): PBaseStructValue = {
    val s = SBaseStructPointerSettable(sb, st, name)
    cb.assign(s, this)
    s
  }

  def memoize(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PBaseStructValue =
    memoize(cb, name, cb.localBuilder)

  def memoizeField(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PBaseStructValue =
    memoize(cb, name, cb.fieldBuilder)
}
