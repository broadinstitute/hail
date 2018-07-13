package is.hail.stats

import breeze.linalg.{DenseMatrix, DenseVector, diag, dim, inv}
import breeze.numerics._
import is.hail.annotations.{Annotation, Region, RegionValueBuilder}
import is.hail.expr.types._
import net.sourceforge.jdistlib.T

object LinearRegressionCombiner {
  val typ: Type = TStruct(
    "beta" -> TArray(TFloat64()),
    "standard_error" -> TArray(TFloat64()),
    "t_stat" -> TArray(TFloat64()),
    "p_value" -> TArray(TFloat64()),
    "n" -> TInt64())

  val xType = TArray(TFloat64())
}

class LinearRegressionCombiner(k: Int) extends Serializable {
  var n = 0L
  var x = DenseVector.zeros[Double](k)
  var xtx = DenseMatrix.zeros[Double](k, k)
  var xty = DenseVector.zeros[Double](k)
  var yty = 0.0

  val xType = LinearRegressionCombiner.xType

  def merge(y: Double, xArray: IndexedSeq[Double]) {
    assert(k == xArray.length)
    x = DenseVector(xArray.toArray)

    n += 1
    xtx :+= x * x.t
    xty :+= x * y
    yty += y * y
  }

  def merge(region: Region, y: Double, xOffset: Long) {
    val length = xType.loadLength(region, xOffset)
    assert(k == length)

    var i = 0
    while (i < length) {
      if (xType.isElementMissing(region, xOffset, i))
        return

      x(i) = region.loadDouble(xType.loadElement(region, xOffset, i))
      i += 1
    }

    n += 1
    xtx :+= x * x.t
    xty :+= x * y
    yty += y * y
  }

  def merge(other: LinearRegressionCombiner) {
    n += other.n
    xtx :+= other.xtx
    xty :+= other.xty
    yty += other.yty
  }

  def computeResult(): (DenseVector[Double], DenseVector[Double], DenseVector[Double]) = {
    val b = xtx \ xty
    val rse2 = (yty - (xty dot b)) / (n - k) // residual standard error squared
    val se = sqrt(rse2 * diag(inv(xtx)))
    val t = b /:/ se
    (b, se, t)
  }

  def result(rvb: RegionValueBuilder) {
    val (b, se, t) = computeResult()

    rvb.startStruct()

    if (n != 0 && n > k) {
      rvb.startArray(k) // beta
      var i = 0
      while (i < k) {
        rvb.addDouble(b(i))
        i += 1
      }
      rvb.endArray()

      rvb.startArray(k) // standard_error
      i = 0
      while (i < k) {
        rvb.addDouble(se(i))
        i += 1
      }
      rvb.endArray()

      rvb.startArray(k) // t_stat
      i = 0
      while (i < k) {
        rvb.addDouble(t(i))
        i += 1
      }
      rvb.endArray()

      rvb.startArray(k) // p_value
      i = 0
      while (i < k) {
        rvb.addDouble(2 * T.cumulative(-math.abs(t(i)), n - k, true, false))
        i += 1
      }
      rvb.endArray()
    } else {
      rvb.setMissing()
      rvb.setMissing()
      rvb.setMissing()
      rvb.setMissing()
    }

    rvb.addLong(n) // n

    rvb.endStruct()

  }

  def result(): Annotation = {
    val (b, se, t) = computeResult()
    if (n != 0)
      Annotation(b.toArray: IndexedSeq[Double],
        se.toArray: IndexedSeq[Double],
        t.toArray: IndexedSeq[Double],
        t.map(ti => 2 * T.cumulative(-math.abs(ti), n - k, true, false)).toArray: IndexedSeq[Double],
        n)
    else
      Annotation(null, null, null, null, n)
  }

  def clear() {
    n = 0
    x = DenseVector.zeros[Double](k)
    xtx = DenseMatrix.zeros[Double](k, k)
    xty = DenseVector.zeros[Double](k)
    yty = 0.0
  }

  def copy(): LinearRegressionCombiner = {
    val combiner = new LinearRegressionCombiner(k)
    combiner.n = n
    combiner.x = x.copy
    combiner.xtx = xtx.copy
    combiner.xty = xty.copy
    combiner.yty = yty
    combiner
  }
}
