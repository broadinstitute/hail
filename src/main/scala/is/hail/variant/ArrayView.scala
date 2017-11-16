package is.hail.variant

import is.hail.annotations._
import is.hail.expr._
import is.hail.utils._

class ArrayView[V <: View](t: TArray, val elementView: V) extends VolatileIndexedSeq[V] {
  private var region: MemoryBuffer = _
  private var aoff: Long = _
  private var eoff: Long = _
  private var _length: Int = _

  def setRegion(region: MemoryBuffer, aoff: Long) {
    this.region = region
    this.aoff = aoff
    this._length = t.loadLength(region, aoff)
  }

  def apply(i: Int): V = {
    set(i)
    elementView
  }

  def set(i: Int) {
    this.eoff = t.loadElement(region, aoff, length, i)
    elementView.setRegion(region, eoff)
  }

  def length(): Int = length
}
