package is.hail.utils

import is.hail.expr._
import is.hail.annotations._
import scala.collection.mutable.BitSet

class MissingFloatArrayBuilder {
  private var len = 0
  private val elements = new ArrayBuilder[Float]()
  private val isMissing = new BitSet()

  def addMissing() {
    isMissing.add(len)
    len += 1
  }

  def add(i: Float) {
    elements += i
    len += 1
  }

  def length(): Int = len

  def foreach(whenMissing: (Int) => Unit)(whenPresent: (Int, Float) => Unit) {
    var i = 0
    var j = 0
    while (i < len) {
      if (isMissing(i))
        whenMissing(i)
      else {
        whenPresent(i, elements(j))
        j += 1
      }
      i += 1
    }
  }

  val typ = TArray(TFloat32())

  private val rvb = new RegionValueBuilder()

  def writeIntoRegion(region: Region): Long = {
    rvb.set(region)
    rvb.start(typ)
    rvb.startArray(len)
    var i = 0
    while (i < len) {
      if (isMissing(i))
        rvb.setMissing()
      else
        rvb.addFloat(elements(i))
      i += 1
    }
    rvb.endArray()
    rvb.end()
  }
}
