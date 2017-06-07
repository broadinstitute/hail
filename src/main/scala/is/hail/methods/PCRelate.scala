package is.hail.methods

import org.apache.spark.rdd.RDD
import is.hail.utils._
import is.hail.keytable.KeyTable
import is.hail.variant.{Variant, VariantDataset}
import is.hail.distributedmatrix.BlockMatrixIsDistributedMatrix
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds
import scala.language.implicitConversions

object PCRelate {
  type M = BlockMatrixIsDistributedMatrix.M
  val dm = BlockMatrixIsDistributedMatrix
  import dm.ops._

  case class Result(phiHat: M, k0: M, k1: M, k2: M)

  def maybefast(vds: VariantDataset, pcs: DenseMatrix, blockSize: Int): Result = {
    val g = vdsToMeanImputedMatrix(vds)

    val beta = fitBeta(g, pcs, blockSize)

    val pcsArray = pcs.toArray
    val pcsWithInterceptArray = new Array[Double](pcs.numRows * (pcs.numCols + 1))

    var i = 0
    while (i < pcs.numRows) {
      pcsWithInterceptArray(i) = 1
      i += 1
    }

    i = 0
    while (i < pcs.numRows * pcs.numCols) {
      pcsWithInterceptArray(pcs.numRows + i) = pcsArray(i)
      i += 1
    }

    val pcsWithIntercept =
      new DenseMatrix(pcs.numRows, pcs.numCols + 1, pcsWithInterceptArray)

    val mu = dm.map(clipToInterval _)((beta * pcsWithIntercept.transpose) / 2.0)

    val blockedG = dm.from(g, blockSize, blockSize)
    val gMinusMu = (blockedG :- (mu * 2.0))
    val variance = (mu :* (1.0 - mu))

    val phi = (((gMinusMu.t * gMinusMu) :/ (variance.t.sqrt * variance.sqrt)) / 4.0)

    val gD = dm.map2({
      case (g, mu) =>
        if (g == 0.0) mu
        else if (g == 1.0) 0.0
        else if (g == 2.0) 1.0 - mu
        else g
    })(blockedG, mu)

    val normalizedGD = (gD :- (variance --* (f(phi).map(1 + _))))

    val kTwo = ((normalizedGD.t * normalizedGD) :/ (variance.t * variance))

    val mu2 = dm.map(sqr _)(mu)
    val oneMinusMu2 = dm.map(sqr _)(1.0 - mu)

    val denom = (mu2.t * oneMinusMu2) :+ (oneMinusMu2.t * mu2)

    def _k0(phiHat: Double, denom: Double, k2: Double, ibs0: Double) =
      if (phiHat <= cutoff)
        1 - 4 * phiHat + k2
      else
        ibs0 / denom
    val kZero = dm.map4(_k0)(phi, denom, kTwo, ibs0(vds, blockSize))

    Result(phi, kZero, 1.0 - (kTwo :+ kZero), kTwo)
  }

  def apply(vds: VariantDataset, pcs: DenseMatrix, blockSize: Int): Result = {
    val g = vdsToMeanImputedMatrix(vds)

    val beta = fitBeta(g, pcs, blockSize)

    val mu = muHat(pcs, beta)

    val blockedG = dm.from(g, blockSize, blockSize)
    val phihat = phiHat(blockedG, mu)

    // FIXME: what should I do if the genotype is missing?
    val kTwo = k2(f(phihat), dm.map2({ case (g, mu) => if (g == 0.0) mu else if (g == 1.0) 0.0 else if (g == 2.0) 1.0 - mu else g })(blockedG, mu), mu)

    val kZero = k0(phihat, mu, kTwo, ibs0(vds, blockSize))

    // println(dm.toLocalMatrix(kTwo))

    Result(phihat, kZero, k1(kTwo, kZero), kTwo)
  }

  def vdsToMeanImputedMatrix(vds: VariantDataset): IndexedRowMatrix = {
    val nSamples = vds.nSamples
    val variants = vds.variants.collect()
    val variantIdxBc = vds.sparkContext.broadcast(variants.index)
    val rdd = vds.rdd.mapPartitions { part =>
      part.map { case (v, (va, gs)) =>
        var sum = 0
        var nNonMissing = 0
        val missingIndices = new ArrayBuilder[Int]()
        val a = new Array[Double](nSamples)

        var i = 0
        val it = gs.hardCallIterator
        while (it.hasNext) {
          val gt = it.next()
          if (gt == -1) {
            missingIndices += i
          } else {
            sum += gt
            a(i) = gt
            nNonMissing += 1
          }
          i += 1
        }

        val mean = sum.toDouble / nNonMissing

        for (i <- missingIndices.result()) {
          a(i) = mean
        }

        // FIXME: this should probably be a sparse vector
        new IndexedRow(variantIdxBc.value(v), new DenseVector(a))
      }
    }
    new IndexedRowMatrix(rdd, variants.length, nSamples)
  }

  /**
    *  g: SNP x Sample
    *  pcs: Sample x D
    *
    *  result: (SNP x (D+1))
    */
  def fitBeta(g: IndexedRowMatrix, pcs: DenseMatrix, blockSize: Int): M = {
    val aa = g.rows.sparkContext.broadcast(pcs.rowIter.map(_.toArray).toArray)
    val rdd = g.rows.map { case IndexedRow(i, v) =>
      val ols = new OLSMultipleLinearRegression()
      ols.newSampleData(v.toArray, aa.value)
      IndexedRow(i, new DenseVector(ols.estimateRegressionParameters()))
    }
    dm.from(new IndexedRowMatrix(rdd, g.numRows(), pcs.numCols + 1), blockSize, blockSize)
  }

  private def clipToInterval(x: Double): Double =
    if (x <= 0)
      Double.MinPositiveValue
    else if (x >= 1)
      1 - Double.MinPositiveValue
    else
      x

  /**
    *  pcs: Sample x D
    *  beta: SNP x (D+1)
    *
    *  result: SNP x Sample
    */
  def muHat(pcs: DenseMatrix, beta: M): M = {
    val pcsArray = pcs.toArray
    val pcsWithInterceptArray = new Array[Double](pcs.numRows * (pcs.numCols + 1))

    var i = 0
    while (i < pcs.numRows) {
      pcsWithInterceptArray(i) = 1
      i += 1
    }

    i = 0
    while (i < pcs.numRows * pcs.numCols) {
      pcsWithInterceptArray(pcs.numRows + i) = pcsArray(i)
      i += 1
    }

    val pcsWithIntercept =
      new DenseMatrix(pcs.numRows, pcs.numCols + 1, pcsWithInterceptArray)

    // println(pcsWithIntercept)
    // println(dm.toLocalMatrix(beta))

    dm.map(clipToInterval _)((beta * pcsWithIntercept.transpose) / 2.0)
  }

  /**
    * g: SNP x Sample
    * muHat: SNP x Sample
    **/
  def phiHat(g: M, muHat: M): M = {
    val gMinusMu = g :- (muHat * 2.0)
    val varianceHat = muHat :* (1.0 - muHat)

    ((gMinusMu.t * gMinusMu) :/ (varianceHat.t.sqrt * varianceHat.sqrt)) / 4.0
  }

  def f(phiHat: M): Array[Double] =
    dm.diagonal(phiHat).map(2.0 * _ - 1.0)

  def k2(f: Array[Double], gD: M, mu: M): M = {
    // println("mu")
    // println(dm.toLocalMatrix(mu))
    // println("gD")
    // println(dm.toLocalMatrix(gD))
    // println("f.map(1 + _)")
    // println(f.map(1 + _): IndexedSeq[Double])
    // println("mu :* (1.0 - mu)")
    // println(dm.toLocalMatrix(mu :* (1.0 - mu)))
    // println("(mu :* (1.0 - mu)) --* f.map(1 + _)")
    // println(dm.toLocalMatrix((mu :* (1.0 - mu)) --* (f.map(1 + _))))

    val normalizedGD = gD :- ((mu :* (1.0 - mu)) --* (f.map(1 + _)))
    val variance = mu :* (1.0 - mu)

    // println("numer")
    // println(dm.toLocalMatrix(normalizedGD.t * normalizedGD))
    // println("denom")
    // println(dm.toLocalMatrix(variance.t * variance))

    (normalizedGD.t * normalizedGD) :/ (variance.t * variance)
  }

  /**
    * muHat: SNP x Sample
    *
    **/
  private def sqr(x: Double): Double = x * x
  private val cutoff = math.pow(2.0, (-5.0/2.0))
  private def _k0(phiHat: Double, denom: Double, k2: Double, ibs0: Double) =
    if (phiHat <= cutoff)
      1 - 4 * phiHat + k2
    else
      ibs0 / denom
  def k0(phiHat: M, mu: M, k2: M, ibs0: M): M = {
    val mu2 = dm.map(sqr _)(mu)
    val oneMinusMu2 = dm.map(sqr _)(1.0 - mu)

    val denom = (mu2.t * oneMinusMu2) :+ (oneMinusMu2.t * mu2)

    dm.map4(_k0)(phiHat, denom, k2, ibs0)
  }

  def k1(k2: M, k0: M): M = {
    1.0 - (k2 :+ k0)
  }

  def ibs0(vds: VariantDataset, blockSize: Int): M =
    // FIXME: either a) use same blocksize everywhere or b) have efficient reblocking
    dm.from(IBD.ibs(vds)._1.toIndexedRowMatrix(), blockSize, blockSize)

}
