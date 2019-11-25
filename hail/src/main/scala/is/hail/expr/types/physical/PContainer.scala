package is.hail.expr.types.physical

import is.hail.annotations._
import is.hail.asm4s._
import is.hail.asm4s.joinpoint._
import is.hail.expr.ir.EmitMethodBuilder
import is.hail.utils._

object PContainer {
  def loadLength(aoff: Long): Int =
    Region.loadInt(aoff)

  def loadLength(aoff: Code[Long]): Code[Int] =
    Region.loadInt(aoff)

  def storeLength(aoff: Long, length: Int): Unit =
    Region.storeInt(aoff, length)

  def storeLength(aoff: Code[Long], length: Code[Int]): Code[Unit] =
    Region.storeInt(aoff, length)

  def nMissingBytes(len: Code[Int]): Code[Int] = (len + 7) >>> 3
}

abstract class PContainer extends PIterable {

  def elementByteSize: Long

  override def byteSize: Long = 8

  def contentsAlignment: Long

  final def loadLength(region: Region, aoff: Long): Int =
    PContainer.loadLength(aoff)

  final def loadLength(aoff: Long): Int =
    PContainer.loadLength(aoff)

  final def loadLength(aoff: Code[Long]): Code[Int] =
    PContainer.loadLength(aoff)

  final def loadLength(region: Code[Region], aoff: Code[Long]): Code[Int] =
    loadLength(aoff)

  final def storeLength(region: Region, aoff: Long, length: Int): Unit =
    PContainer.storeLength(aoff, length)

  final def storeLength(aoff: Code[Long], length: Code[Int]): Code[Unit] =
    PContainer.storeLength(aoff, length)

  final def storeLength(region: Code[Region], aoff: Code[Long], length: Code[Int]): Code[Unit] =
    storeLength(aoff, length)

  def nMissingBytes(len: Code[Int]): Code[Int] = PContainer.nMissingBytes(len)

  def _elementsOffset(length: Int): Long =
    if (elementType.required)
      UnsafeUtils.roundUpAlignment(4, elementType.alignment)
    else
      UnsafeUtils.roundUpAlignment(4 + ((length + 7) >>> 3), elementType.alignment)

  def _elementsOffset(length: Code[Int]): Code[Long] =
    if (elementType.required)
      UnsafeUtils.roundUpAlignment(4, elementType.alignment)
    else
      UnsafeUtils.roundUpAlignment((nMissingBytes(length) + 4).toL, elementType.alignment)

  var elementsOffsetTable: Array[Long] = _

  def elementsOffset(length: Int): Long = {
    if (elementsOffsetTable == null)
      elementsOffsetTable = Array.tabulate[Long](10)(i => _elementsOffset(i))

    if (length < 10)
      elementsOffsetTable(length)
    else
      _elementsOffset(length)
  }

  def elementsOffset(length: Code[Int]): Code[Long] = {
    // FIXME: incorporate table, maybe?
    _elementsOffset(length)
  }

  def contentsByteSize(length: Int): Long =
    elementsOffset(length) + length * elementByteSize

  def contentsByteSize(length: Code[Int]): Code[Long] = {
    elementsOffset(length) + length.toL * elementByteSize
  }

  def isElementMissing(region: Region, aoff: Long, i: Int): Boolean =
    !isElementDefined(aoff, i)

  def isElementDefined(aoff: Long, i: Int): Boolean =
    elementType.required || !Region.loadBit(aoff + 4, i)

  def isElementDefined(region: Region, aoff: Long, i: Int): Boolean = isElementDefined(aoff, i)

  def isElementMissing(aoff: Code[Long], i: Code[Int]): Code[Boolean] =
    !isElementDefined(aoff, i)

  def isElementMissing(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Boolean] =
    isElementMissing(aoff, i)

  def isElementDefined(aoff: Code[Long], i: Code[Int]): Code[Boolean] =
    if (elementType.required)
      true
    else
      !Region.loadBit(aoff + 4L, i.toL)

  def isElementDefined(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Boolean] =
    isElementDefined(aoff, i)

  def setElementMissing(region: Region, aoff: Long, i: Int) {
    assert(!elementType.required)
    Region.setBit(aoff + 4, i)
  }

  def setElementMissing(aoff: Code[Long], i: Code[Int]): Code[Unit] =
    Region.setBit(aoff + 4L, i.toL)

  def setElementMissing(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Unit] =
    setElementMissing(aoff, i)

  def setElementPresent(region: Region, aoff: Long, i: Int) {
    assert(!elementType.required)
    Region.clearBit(aoff + 4, i)
  }

  def setElementPresent(aoff: Code[Long], i: Code[Int]): Code[Unit] =
    Region.clearBit(aoff + 4L, i.toL)

  def setElementPresent(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Unit] =
    setElementPresent(aoff, i)

  def elementOffset(aoff: Long, length: Int, i: Int): Long =
    aoff + elementsOffset(length) + i * elementByteSize

  def elementOffsetInRegion(region: Region, aoff: Long, i: Int): Long =
    elementOffset(aoff, loadLength(region, aoff), i)

  def elementOffset(aoff: Code[Long], length: Code[Int], i: Code[Int]): Code[Long] =
    aoff + elementsOffset(length) + i.toL * const(elementByteSize)

  def firstElementOffset(aoff: Code[Long], length: Code[Int]): Code[Long] =
    aoff + elementsOffset(length)

  def firstElementOffset(aoff: Code[Long]): Code[Long] =
    firstElementOffset(aoff, loadLength(aoff))

  def elementOffsetInRegion(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Long] =
    elementOffset(aoff, loadLength(region, aoff), i)

  def loadElement(aoff: Long, length: Int, i: Int): Long = {
    val off = elementOffset(aoff, length, i)
    elementType.fundamentalType match {
      case _: PArray | _: PBinary => Region.loadAddress(off)
      case _ => off
    }
  }

  def loadElement(region: Region, aoff: Long, length: Int, i: Int): Long = loadElement(aoff, length, i)

  def loadElement(region: Region, aoff: Long, i: Int): Long = loadElement(aoff, loadLength(aoff), i)

  def loadElement(region: Code[Region], aoff: Code[Long], length: Code[Int], i: Code[Int]): Code[Long] = {
    val off = elementOffset(aoff, length, i)
    elementType.fundamentalType match {
      case _: PArray | _: PBinary => Region.loadAddress(off)
      case _ => off
    }
  }

  def loadElement(region: Code[Region], aoff: Code[Long], i: Code[Int]): Code[Long] = {
    val length = loadLength(region, aoff)
    loadElement(region, aoff, length, i)
  }

  def allocate(region: Region, length: Int): Long = {
    region.allocate(contentsAlignment, contentsByteSize(length))
  }

  def allocate(region: Code[Region], length: Code[Int]): Code[Long] =
    region.allocate(contentsAlignment, contentsByteSize(length))

  private def writeMissingness(region: Region, aoff: Long, length: Int, value: Byte) {
    val nMissingBytes = (length + 7) / 8
    Region.setMemory(aoff + 4, nMissingBytes, value)
  }

  def setAllMissingBits(region: Region, aoff: Long, length: Int) {
    if (elementType.required)
      return
    writeMissingness(region, aoff, length, -1)
  }

  def clearMissingBits(region: Region, aoff: Long, length: Int) {
    if (elementType.required)
      return
    writeMissingness(region, aoff, length, 0)
  }

  def initialize(region: Region, aoff: Long, length: Int, setMissing: Boolean = false) {
    Region.storeInt(aoff, length)
    if (setMissing)
      setAllMissingBits(region, aoff, length)
    else
      clearMissingBits(region, aoff, length)
  }

  def stagedInitialize(aoff: Code[Long], length: Code[Int], setMissing: Boolean = false): Code[Unit] = {
    if (elementType.required)
      Region.storeInt(aoff, length)
    else
      Code(
        Region.storeInt(aoff, length),
        Region.setMemory(aoff + const(4), nMissingBytes(length).toL, const(if (setMissing) (-1).toByte else 0.toByte)))
  }

  def zeroes(region: Region, length: Int): Long = {
    require(elementType.isNumeric)
    val aoff = allocate(region, length)
    initialize(region, aoff, length)
    Region.setMemory(aoff + elementsOffset(length), length * elementByteSize, 0.toByte)
    aoff
  }

  def zeroes(mb: MethodBuilder, region: Code[Region], length: Code[Int]): Code[Long] = {
    require(elementType.isNumeric)
    val aoff = mb.newLocal[Long]
    Code(
      aoff := allocate(region, length),
      stagedInitialize(aoff, length),
      Region.setMemory(aoff + elementsOffset(length), length.toL * elementByteSize, 0.toByte),
      aoff)
  }

  def anyMissing(mb: MethodBuilder, aoff: Code[Long]): Code[Boolean] =
    if (elementType.required)
      true
    else {
      val n = mb.newLocal[Long]
      JoinPoint.CallCC[Code[Boolean]] { (jb, ret) =>
        val loop = jb.joinPoint[Code[Long]](mb)
        loop.define { ptr =>
          (ptr < n).mux(
            Region.loadInt(ptr).cne(0).mux(
              ret(true),
              loop(ptr + 4L)),
            (Region.loadByte(ptr) >>>
              (const(32) - (loadLength(aoff) | 31))).cne(0).mux(
              ret(true),
              ret(false)))
        }
        Code(
          n := aoff + ((loadLength(aoff) >>> 5) * 4 + 4).toL,
          loop(aoff + 4L))
      }
    }

  def forEach(mb: MethodBuilder, region: Code[Region], aoff: Code[Long], body: Code[Long] => Code[Unit]): Code[Unit] = {
    val i = mb.newLocal[Int]
    val n = mb.newLocal[Int]
    Code(
      n := loadLength(aoff),
      i := 0,
      Code.whileLoop(i < n,
        isElementDefined(aoff, i).mux(
          body(loadElement(region, aoff, n, i)),
          Code._empty
        )))
  }

  override def unsafeOrdering(): UnsafeOrdering =
    unsafeOrdering(this)

  override def unsafeOrdering(rightType: PType): UnsafeOrdering = {
    require(this.isOfType(rightType))

    val right = rightType.asInstanceOf[PContainer]
    val eltOrd = elementType.unsafeOrdering(
      right.elementType)

    new UnsafeOrdering {
      override def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
        val length1 = loadLength(r1, o1)
        val length2 = right.loadLength(r2, o2)

        var i = 0
        while (i < math.min(length1, length2)) {
          val leftDefined = isElementDefined(r1, o1, i)
          val rightDefined = right.isElementDefined(r2, o2, i)

          if (leftDefined && rightDefined) {
            val eOff1 = loadElement(r1, o1, length1, i)
            val eOff2 = right.loadElement(r2, o2, length2, i)
            val c = eltOrd.compare(r1, eOff1, r2, eOff2)
            if (c != 0)
              return c
          } else if (leftDefined != rightDefined) {
            val c = if (leftDefined) -1 else 1
            return c
          }
          i += 1
        }
        Integer.compare(length1, length2)
      }
    }
  }

  def checkedConvertFrom(mb: EmitMethodBuilder, r: Code[Region], value: Code[Long], otherPT: PType, msg: String): Code[Long] = {
    val otherPTA = otherPT.asInstanceOf[PArray]
    assert(otherPTA.elementType.isPrimitive)
    val oldOffset = value
    val len = otherPTA.loadLength(oldOffset)
    if (otherPTA.elementType.required == elementType.required) {
      value
    } else {
      val newOffset = mb.newField[Long]
      Code(
        newOffset := allocate(r, len),
        stagedInitialize(newOffset, len),
        if (otherPTA.elementType.required) {
          // convert from required to non-required
          Code._empty
        } else {
          //  convert from non-required to required
          val i = mb.newField[Int]
          Code(
            i := 0,
            Code.whileLoop(i < len,
              otherPTA.isElementMissing(oldOffset, i).orEmpty(Code._fatal(s"${msg}: convertFrom $otherPT failed: element missing.")),
              i := i + 1
            )
          )
        },
        Region.copyFrom(otherPTA.elementOffset(oldOffset, len, 0), elementOffset(newOffset, len, 0), len.toL * elementByteSize),
        newOffset
      )
    }
  }

  override def containsPointers: Boolean = true
}
