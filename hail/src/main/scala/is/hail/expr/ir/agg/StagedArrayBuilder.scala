package is.hail.expr.ir.agg

import is.hail.annotations.{Region, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitMethodBuilder, EmitRegion}
import is.hail.expr.types.physical._
import is.hail.io.{CodecSpec, InputBuffer, OutputBuffer}
import is.hail.utils._

object StagedArrayBuilder {
  val END_SERIALIZATION: Int = 0x12345678
}

class StagedArrayBuilder(eltType: PType, mb: EmitMethodBuilder, er: EmitRegion, region: Code[Region], initialCapacity: Int = 8) {
  val eltArray = PArray(eltType, required = true)
  val stateType = PTuple(true, PInt32Required, PInt32Required, eltArray)
  val size: ClassFieldRef[Int] = mb.newField[Int]
  private val capacity = mb.newField[Int]
  private val data = mb.newField[Long]

  private val currentSizeOffset: Code[Long] => Code[Long] = stateType.fieldOffset(_, 0)
  private val capacityOffset: Code[Long] => Code[Long] = stateType.fieldOffset(_, 1)
  private val dataOffset: Code[Long] => Code[Long] = stateType.fieldOffset(_, 2)

  def loadFields(src: Code[Long]): Code[Unit] = {
    Code(
      size := region.loadInt(currentSizeOffset(src)),
      capacity := region.loadInt(capacityOffset(src)),
      data := region.loadAddress(dataOffset(src))
    )
  }

  def storeFields(dest: Code[Long]): Code[Unit] = {
    Code(
      region.storeInt(currentSizeOffset(dest), size),
      region.storeInt(capacityOffset(dest), capacity),
      region.storeAddress(dataOffset(dest), data)
    )
  }

  def serialize(codec: CodecSpec): Code[OutputBuffer] => Code[Unit] = {
    { ob: Code[OutputBuffer] =>
      val enc = codec.buildEmitEncoderF[Long](eltArray, eltArray, mb.fb)

      Code(
        ob.writeInt(size),
        ob.writeInt(capacity),
        enc(region, data, ob),
        ob.writeInt(const(StagedArrayBuilder.END_SERIALIZATION))
      )
    }
  }

  def deserialize(codec: CodecSpec): Code[InputBuffer] => Code[Unit] = {
    val dec = codec.buildEmitDecoderF[Long](eltArray, eltArray, mb.fb)(_, _)

    { (ib: Code[InputBuffer]) =>
      Code(
        size := ib.readInt(),
        capacity := ib.readInt(),
        data := dec(region, ib),
        ib.readInt()
          .cne(const(StagedArrayBuilder.END_SERIALIZATION))
          .orEmpty[Unit](Code._fatal(s"StagedArrayBuilder serialization failed"))
      )
    }
  }

  private def incrementSize(): Code[Unit] = Code(
    size := size + 1,
    resize()
  )

  def setMissing(): Code[Unit] = incrementSize() // all elements set to missing on initialization

  def append(elt: Code[_]): Code[Unit] = {
    val dest = eltArray.elementOffset(data, capacity, size)
    Code(
      eltArray.setElementPresent(region, data, size),
      eltType.fundamentalType match {
        case _: PBoolean => region.storeByte(dest, coerce[Boolean](elt).toI.toB)
        case _: PInt32 => region.storeInt(dest, coerce[Int](elt))
        case _: PInt64 => region.storeLong(dest, coerce[Long](elt))
        case _: PFloat32 => region.storeFloat(dest, coerce[Float](elt))
        case _: PFloat64 => region.storeDouble(dest, coerce[Double](elt))
        case _ => StagedRegionValueBuilder.deepCopy(er, eltType, coerce[Long](elt), dest)
      }, incrementSize())
  }

  def initialize(): Code[Unit] = initialize(const(0), const(initialCapacity))

  private def initialize(_size: Code[Int], _capacity: Code[Int]): Code[Unit] = {
    Code(
      size := _size,
      capacity := _capacity,
      data := eltArray.allocate(region, capacity),
      eltArray.stagedInitialize(data, capacity, setMissing = true)
    )
  }

  def loadElementOffset(idx: Code[Int]): (Code[Boolean], Code[Long]) = {
    (eltArray.isElementMissing(data, idx), eltArray.loadElement(region, data, capacity, idx))
  }

  def loadElement(idx: Code[Int]): (Code[Boolean], Code[_]) = {
    (eltArray.isElementMissing(data, idx), Region.loadIRIntermediate(eltType)(eltArray.loadElement(region, data, capacity, idx)))
  }

  private def resize(): Code[Unit] = {
    val newDataOffset = mb.newLocal[Long]
    size.ceq(capacity)
      .orEmpty(
        Code(
          capacity := capacity * 2,
          newDataOffset := eltArray.allocate(region, capacity),
          eltArray.stagedInitialize(newDataOffset, capacity, setMissing = true),
          region.copyFrom(region, data + 4L, newDataOffset + 4L, eltArray.nMissingBytes(size)),
          region.copyFrom(region,
            data + eltArray.elementsOffset(size),
            newDataOffset + eltArray.elementsOffset(capacity.load()),
            size.toL * const(eltArray.elementByteSize)),
          data := newDataOffset
        )
      )
  }
}
