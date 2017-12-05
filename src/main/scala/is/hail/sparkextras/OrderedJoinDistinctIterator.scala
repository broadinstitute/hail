package is.hail.sparkextras

import is.hail.annotations.{JoinedRegionValue, RegionValue}
import is.hail.rvd.OrderedRVType

abstract class OrderedJoinDistinctIterator(leftTyp: OrderedRVType, rightTyp: OrderedRVType, leftIt: Iterator[RegionValue],
  rightIt: Iterator[RegionValue]) extends Iterator[JoinedRegionValue] {
  private val jrv = JoinedRegionValue()
  private var lrv: RegionValue = _
  private var rrv: RegionValue = if (rightIt.hasNext) rightIt.next() else null

  private var jrvPresent = false
  private val lrKOrd = OrderedRVType.selectUnsafeOrdering(leftTyp.rowType, leftTyp.kRowFieldIdx, rightTyp.rowType, rightTyp.kRowFieldIdx)

  def lrCompare(): Int = {
    assert(lrv != null && rrv != null)
    lrKOrd.compare(lrv, rrv)
  }

  def isPresent: Boolean = jrvPresent

  def advanceLeft1() {
    lrv = if (leftIt.hasNext) leftIt.next() else null
  }

  def advanceLeft() {
    assert(rrv != null)
    while (lrv != null && lrKOrd.compare(lrv, rrv) < 0) {
      advanceLeft1()
    }
  }

  def hasLeft: Boolean = lrv != null

  def advanceRight1() {
    rrv = if (rightIt.hasNext) rightIt.next() else null
  }

  def advanceRight() {
    assert(lrv != null)
    while (rrv != null && lrKOrd.compare(lrv, rrv) > 0) {
      advanceRight1()
    }
  }

  def hasRight: Boolean = rrv != null

  def setJRV() {
    jrv.set(lrv, rrv)
    jrvPresent = true
  }

  def setJRVRightNull() {
    jrv.set(lrv, null)
    jrvPresent = true
  }

  def setJRVLeftNull() {
    jrv.set(null, rrv)
    jrvPresent = true
  }

  def hasNext: Boolean

  def next(): JoinedRegionValue = {
    if (!hasNext)
      throw new NoSuchElementException("next on empty iterator")
    jrvPresent = false
    jrv
  }
}

class OrderedInnerJoinDistinctIterator(leftTyp: OrderedRVType, rightTyp: OrderedRVType, leftIt: Iterator[RegionValue],
  rightIt: Iterator[RegionValue]) extends OrderedJoinDistinctIterator(leftTyp, rightTyp, leftIt, rightIt) {

  def hasNext: Boolean = {
    if (!isPresent) {
      advanceLeft1()
      while (!isPresent && hasLeft && hasRight) {
        val c = lrCompare()
        if (c == 0)
          setJRV()
        else if (c > 0)
          advanceRight()
        else
          advanceLeft()
      }
    }

    isPresent
  }
}

class OrderedLeftJoinDistinctIterator(leftTyp: OrderedRVType, rightTyp: OrderedRVType, leftIt: Iterator[RegionValue],
  rightIt: Iterator[RegionValue]) extends OrderedJoinDistinctIterator(leftTyp, rightTyp, leftIt, rightIt) {

  def hasNext: Boolean = {
    if (!isPresent) {
      advanceLeft1()
      while (!isPresent && hasLeft) {
        if (!hasRight || lrCompare() < 0)
          setJRVRightNull()
        else if (lrCompare() == 0)
          setJRV()
        else
          advanceRight()
      }
    }

    isPresent
  }
}
