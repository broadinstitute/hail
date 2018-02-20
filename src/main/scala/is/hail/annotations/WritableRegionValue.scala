package is.hail.annotations

import is.hail.expr.types._
import is.hail.utils.StateMachine
import scala.collection.mutable.{ ArrayBuffer, ArrayBuilder }

object WritableRegionValue {
  def apply(t: Type, initial: RegionValue): WritableRegionValue =
    WritableRegionValue(t, initial.region, initial.offset)

  def apply(t: Type, initialRegion: Region, initialOffset: Long): WritableRegionValue = {
    val wrv = WritableRegionValue(t)
    wrv.set(initialRegion, initialOffset)
    wrv
  }

  def apply(t: Type): WritableRegionValue = {
    new WritableRegionValue(t)
  }
}

class WritableRegionValue private (val t: Type) {
  val region = Region()
  val value = RegionValue(region, 0)
  private val rvb: RegionValueBuilder = new RegionValueBuilder(region)

  def offset: Long = value.offset

  def setSelect(fromT: TStruct, toFromFieldIdx: Array[Int], fromRV: RegionValue) {
    (t: @unchecked) match {
      case t: TStruct =>
        region.clear()
        rvb.start(t)
        rvb.startStruct()
        var i = 0
        while (i < t.size) {
          rvb.addField(fromT, fromRV, toFromFieldIdx(i))
          i += 1
        }
        rvb.endStruct()
        value.setOffset(rvb.end())
    }
  }

  def set(rv: RegionValue): Unit = set(rv.region, rv.offset)

  def set(fromRegion: Region, fromOffset: Long) {
    region.clear()
    rvb.start(t)
    rvb.addRegionValue(t, fromRegion, fromOffset)
    value.setOffset(rvb.end())
  }

  def pretty: String = value.pretty(t)
}

class RegionValueArrayBuffer(val t: Type)
  extends Iterable[RegionValue] {

  val region = Region()
  val value = RegionValue(region, 0)

  private val rvb = new RegionValueBuilder(region)
  val idx = ArrayBuffer.empty[Long]
  private val wrv = WritableRegionValue(t)

  def +=(rv: RegionValue): RegionValueArrayBuffer = {
    this += (rv.region, rv.offset)
  }

  def +=(fromRegion: Region, fromOffset: Long): RegionValueArrayBuffer = {
    rvb.start(t)
    rvb.addRegionValue(t, fromRegion, fromOffset)
    idx += rvb.end()
    this
  }

  def +=(fromT: TStruct,
         toFromFieldIdx: Array[Int],
         fromRV: RegionValue): RegionValueArrayBuffer = {
    (t: @unchecked) match {
      case t: TStruct =>
        rvb.start(t)
        rvb.startStruct()
        var i = 0
        while (i < t.size) {
          rvb.addField(fromT, fromRV, toFromFieldIdx(i))
          i += 1
        }
        rvb.endStruct()
        idx += rvb.end()
    }
    this
  }

  def ++=(rvs: Iterator[RegionValue]): RegionValueArrayBuffer = {
    while (rvs.hasNext) this += rvs.next()
    this
  }

  def clear() {
    region.clear()
    idx.clear()
    rvb.clear()
  }

  private var itIdx = 0
  private val it = new Iterator[RegionValue] {
    def next(): RegionValue = {
      value.setOffset(idx(itIdx))
      itIdx += 1
      value
    }
    def hasNext: Boolean = itIdx < idx.size
  }

  def iterator: Iterator[RegionValue] = {
    itIdx = 0
    it
  }
}
