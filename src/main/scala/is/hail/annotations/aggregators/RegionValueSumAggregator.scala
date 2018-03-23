package is.hail.annotations.aggregators

import is.hail.asm4s._
import is.hail.expr.types._
import is.hail.annotations._

class RegionValueSumBooleanAggregator extends RegionValueAggregator {
  private var sum: Boolean = false

  def seqOp(b: Boolean, missing: Boolean) {
    if (!missing)
      sum |= b
  }

  def seqOp(region: Region, off: Long, missing: Boolean) {
    seqOp(region.loadBoolean(off), missing)
  }

  def combOp(agg2: RegionValueAggregator) {
    sum |= agg2.asInstanceOf[RegionValueSumBooleanAggregator].sum
  }

  def result(rvb: RegionValueBuilder) {
    rvb.addBoolean(sum)
  }

  def copy(): RegionValueSumBooleanAggregator = new RegionValueSumBooleanAggregator()
}

class RegionValueSumIntAggregator extends RegionValueAggregator {
  private var sum: Int = 0

  def seqOp(i: Int, missing: Boolean) {
    if (!missing)
      sum += i
  }

  def seqOp(region: Region, off: Long, missing: Boolean) {
    seqOp(region.loadInt(off), missing)
  }

  def combOp(agg2: RegionValueAggregator) {
    sum += agg2.asInstanceOf[RegionValueSumIntAggregator].sum
  }

  def result(rvb: RegionValueBuilder) {
    rvb.addInt(sum)
  }

  def copy(): RegionValueSumIntAggregator = new RegionValueSumIntAggregator()
}

class RegionValueSumLongAggregator extends RegionValueAggregator {
  private var sum: Long = 0L

  def seqOp(l: Long, missing: Boolean) {
    if (!missing)
      sum += l
  }

  def seqOp(region: Region, off: Long, missing: Boolean) {
    seqOp(region.loadLong(off), missing)
  }

  def combOp(agg2: RegionValueAggregator) {
    sum += agg2.asInstanceOf[RegionValueSumLongAggregator].sum
  }

  def result(rvb: RegionValueBuilder) {
    rvb.addLong(sum)
  }

  def copy(): RegionValueSumLongAggregator = new RegionValueSumLongAggregator()
}

class RegionValueSumFloatAggregator extends RegionValueAggregator {
  private var sum: Float = 0.0f

  def seqOp(f: Float, missing: Boolean) {
    if (!missing)
      sum += f
  }

  def seqOp(region: Region, off: Long, missing: Boolean) {
    seqOp(region.loadFloat(off), missing)
  }

  def combOp(agg2: RegionValueAggregator) {
    sum += agg2.asInstanceOf[RegionValueSumFloatAggregator].sum
  }

  def result(rvb: RegionValueBuilder) {
    rvb.addFloat(sum)
  }

  def copy(): RegionValueSumFloatAggregator = new RegionValueSumFloatAggregator()
}

class RegionValueSumDoubleAggregator extends RegionValueAggregator {
  private var sum: Double = 0.0

  def seqOp(d: Double, missing: Boolean) {
    if (!missing)
      sum += d
  }

  def seqOp(region: Region, off: Long, missing: Boolean) {
    seqOp(region.loadDouble(off), missing)
  }

  def combOp(agg2: RegionValueAggregator) {
    sum += agg2.asInstanceOf[RegionValueSumDoubleAggregator].sum
  }

  def result(rvb: RegionValueBuilder) {
    rvb.addDouble(sum)
  }

  def copy(): RegionValueSumDoubleAggregator = new RegionValueSumDoubleAggregator()
}
