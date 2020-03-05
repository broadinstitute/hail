package is.hail.expr.ir

import is.hail.annotations.{Region, UnsafeUtils}
import is.hail.asm4s._
import is.hail.utils._
import is.hail.expr.types.physical._

abstract class PSettable[PV <: PValue] {
  def load(): PV

  def store(v: PV): Code[Unit]

  def :=(v: PV): Code[Unit] = store(v)

  def storeAny(v: PValue): Code[Unit] = store(v.asInstanceOf[PV])
}

object PValue {
  def apply(pt: PType, code: Code[_]): PValue = pt match {
    case pt: PCanonicalArray =>
      new PCanonicalIndexableValue(pt, coerce[Long](code))
    case pt: PCanonicalSet =>
      new PCanonicalIndexableValue(pt, coerce[Long](code))
    case pt: PCanonicalDict =>
      new PCanonicalIndexableValue(pt, coerce[Long](code))

    case pt: PCanonicalBaseStruct =>
      new PCanonicalBaseStructValue(pt, coerce[Long](code))

    case _ =>
      new PPrimitiveValue(pt, code)
  }

  def _empty: PValue = PValue(PVoid, Code._empty)
}

abstract class PValue {
  def pt: PType

  def code: Code[_]

  def typeInfo: TypeInfo[_] = typeToTypeInfo(pt)

  def tcode[T](implicit ti: TypeInfo[T]): Code[T] = {
    assert(ti == typeInfo)
    code.asInstanceOf[Code[T]]
  }

  def store(mb: EmitMethodBuilder, r: Code[Region], dst: Code[Long]): Code[Unit]

  def allocateAndStore(mb: EmitMethodBuilder, r: Code[Region]): (Code[Unit], Code[Long]) = {
    val dst = mb.newLocal[Long]
    (Code(dst := r.allocate(pt.byteSize, pt.alignment), store(mb, r, dst)), dst)
  }
}

class PPrimitiveValue(val pt: PType, val code: Code[_]) extends PValue {
  def store(mb: EmitMethodBuilder, r: Code[Region], a: Code[Long]): Code[Unit] =
    Region.storeIRIntermediate(pt)(a, code)
}

abstract class PIndexableValue extends PValue {
  def loadLength(): Code[Int]

  def isElementDefined(i: Code[Int]): Code[Boolean]

  def loadElement(length: Code[Int], i: Code[Int]): PValue

  def loadElement(i: Code[Int]): PValue = loadElement(loadLength(), i)

  def isElementMissing(i: Code[Int]): Code[Boolean] = !isElementDefined(i)
}

class PCanonicalIndexableValue(val pt: PContainer, val a: Code[Long]) extends PIndexableValue {
  def code: Code[_] = a

  def elementType: PType = pt.elementType

  def arrayElementSize: Long = UnsafeUtils.arrayElementSize(elementType)

  def loadLength(): Code[Int] = Region.loadInt(a)

  def nMissingBytes(len: Code[Int]): Code[Int] = (len + 7) >>> 3

  def isElementDefined(i: Code[Int]): Code[Boolean] =
    if (pt.elementType.required)
      const(true)
    else
      !Region.loadBit(a + const(4L), i.toL)

  def elementsOffset(length: Code[Int]): Code[Long] =
    if (elementType.required)
      UnsafeUtils.roundUpAlignment(4, elementType.alignment)
    else
      UnsafeUtils.roundUpAlignment(const(4L) + nMissingBytes(length).toL, elementType.alignment)

  def elementsAddress(length: Code[Int]): Code[Long] = a + elementsOffset(length)

  def elementAddress(length: Code[Int], i: Code[Int]): Code[Long] =
    elementsAddress(length) + i.toL * arrayElementSize

  def loadElement(length: Code[Int], i: Code[Int]): PValue =
    elementType.load(elementAddress(length, i))

  def store(mb: EmitMethodBuilder, r: Code[Region], dst: Code[Long]): Code[Unit] =
    Region.storeAddress(dst, a)
}

abstract class PBaseStructValue extends PValue {
  def pt: PBaseStruct

  def isFieldMissing(fieldIdx: Int): Code[Boolean]

  def loadField(fieldIdx: Int): PValue

  def isFieldMissing(fieldName: String): Code[Boolean] = isFieldMissing(pt.fieldIdx(fieldName))

  def isFieldDefined(fieldIdx: Int): Code[Boolean] = !isFieldMissing(fieldIdx)

  def isFieldDefined(fieldName: String): Code[Boolean] = !isFieldMissing(fieldName)

  def loadField(fieldName: String): PValue = loadField(pt.fieldIdx(fieldName))
}

class PCanonicalBaseStructValue(val pt: PCanonicalBaseStruct, val a: Code[Long]) extends PValue {
  def code: Code[_] = a

  def isFieldMissing(fieldIdx: Int): Code[Boolean] =
    if (pt.fieldRequired(fieldIdx))
      const(false)
    else
      Region.loadBit(a, pt.missingIdx(fieldIdx).toLong)

  def fieldAddress(fieldIdx: Int): Code[Long] = a + pt.byteOffsets(fieldIdx)

  def loadField(fieldIdx: Int): PValue = pt.fields(fieldIdx).typ.load(fieldAddress(fieldIdx))

  def store(mb: EmitMethodBuilder, r: Code[Region], dst: Code[Long]): Code[Unit] =
    pt.constructAtAddress(mb, dst, r, pt, a, forceDeep = false)
}
