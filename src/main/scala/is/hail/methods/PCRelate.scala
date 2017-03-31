package is.hail.methods


import org.apache.spark.rdd.RDD
import is.hail.utils._
import is.hail.keytable.KeyTable
import is.hail.variant.{Genotype, Variant, VariantDataset}
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._

import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds
import scala.reflect.ClassTag

object PCRelate {

  /**
    *
    * @param vds
    * @param mean (variant, (sample, mean))
    * @return
    */
  def apply(vds: VariantDataset, mean: RDD[(Variant, (Int, Double))]): RDD[((String, String), Double)] = {
    assert(vds.wasSplit, "PCRelate requires biallelic VDSes")

    // (variant, (sample, gt))
    val g = vds.rdd.flatMap { case (v, (va, gs)) =>
      gs.zipWithIndex.map { case (g, i) =>
        (v, (i, g.nNonRefAlleles.getOrElse[Int](-1): Double)) } }

    val meanPairs = mean.join(mean)
      .filter { case (_, ((i, _), (j, _))) => j >= i }
      .map { case (vi, ((s1, mean1), (s2, mean2))) =>
        ((s1, s2, vi), (mean1, mean2))
    }

    val numerator = g.join(g)
      .filter { case (_, ((i, _), (j, _))) => j >= i }
      .map { case (vi, ((s1, gt1), (s2, gt2))) =>
        ((s1, s2, vi), (gt1, gt2))
    }
      .join(meanPairs)
      .map { case ((s1, s2, vi), ((gt1, gt2), (mean1, mean2))) =>
        ((s1, s2), (gt1 - 2 * mean1) * (gt2 - 2 * mean2))
    }
      .reduceByKey(_ + _)

    val denominator = mean.join(mean)
      .filter { case (_, ((i, _), (j, _))) => j >= i }
      .map { case (vi, ((s1, mean1), (s2, mean2))) =>
        ((s1, s2), Math.sqrt(mean1 * (1 - mean1) * mean2 * (1 - mean2)))
    }
      .reduceByKey(_ + _)

    val sampleIndexToId =
      vds.sampleIds.zipWithIndex.map { case (s, i) => (i, s) }.toMap

    numerator.join(denominator)
      .map { case ((s1, s2), (numerator, denominator)) => ((s1, s2), numerator / denominator / 4) }
      .map { case ((s1, s2), x) => ((sampleIndexToId(s1), sampleIndexToId(s2)), x) }
  }

  trait Matrix {
    def multiply(that: Matrix): Matrix
    def transpose: Matrix
    def index(i: Long, j: Long): Matrix
    def pointwiseAdd(that: Matrix): Matrix
    def pointwiseSubtract(that: Matrix): Matrix
    def pointwiseMultiply(that: Matrix): Matrix
    def pointwiseDivide(that: Matrix): Matrix
    def scalarMultiply(i: Double): Matrix
    def scalarAdd(i: Double): Matrix

    def vectorExtendAddRight(v: Vector): Matrix
    def vectorExtendAddLeft(v: Vector): Matrix

    def toArrayArray: Array[Array[Double]]

    def mapRows[U](f: Array[Double] => U): RDD[U]
    def mapCols[U](f: Array[Double] => U): RDD[U]
  }
  object Matrix {
    def from(rdd: RDD[Array[Double]]): Matrix = ???
    def from(m: DenseMatrix): Matrix = ???
  }

  trait Vector {
    def plus(that: Vector): Vector
  }
  object Vector {
    def from(rdd: RDD[Double]): Vector = ???
  }

  case class Result(phiHat: Matrix)

  def pcRelate(vds: VariantDataset, pcs: Matrix): Result = {
    val g = vdsToMeanImputedMatrix(vds)

    val (beta0, betas) = fitBeta(g, pcs)

    Result(phiHat(g, muHat(pcs, beta0, betas)))
  }

  def vdsToMeanImputedMatrix(vds: VariantDataset): Matrix = {
    val rdd = vds.rdd.mapPartitions { stuff =>
      val ols = new OLSMultipleLinearRegression()
      stuff.map { case (v, (va, gs)) =>
        val goptions = gs.map(_.gt.map(_.toDouble)).toArray
        val defined = goptions.flatMap(x => x)
        val mean: Double = defined.sum / defined.size
        goptions.map(_.getOrElse(mean))
      }
    }
    Matrix.from(rdd)
  }

  /**
    *  g: SNP x Sample
    *  pcs: Sample x D
    *
    *  result: (D, SNP x D)
    */
  def fitBeta(g: Matrix, pcs: Matrix): (Vector, Matrix) = {
    val rdd: RDD[(Double, Array[Double])] = g.mapRows { row =>
      val ols = new OLSMultipleLinearRegression()
      ols.newSampleData(row, pcs.toArrayArray)
      val allBetas = ols.estimateRegressionParameters().toArray
      (allBetas(0), allBetas.slice(1,allBetas.size))
    }
    val vecRdd = rdd.map(_._1)
    val matRdd = rdd.map(_._2)
    (Vector.from(vecRdd), Matrix.from(matRdd))
  }

  /**
    *  pcs: Sample x D
    *  betas: SNP x D
    *  beta0: SNP
    *
    *  result: Sample x SNP
    */
  def muHat(pcs: Matrix, beta0: Vector, betas: Matrix): Matrix =
    pcs.multiply(betas.transpose).vectorExtendAddRight(beta0)

  def phiHat(g: Matrix, muHat: Matrix): Matrix = {
    val gMinusMu = g.pointwiseSubtract(muHat)
    val oneMinusMu = muHat.scalarMultiply(-1).scalarAdd(1)
    val varianceHat = muHat.pointwiseMultiply(oneMinusMu)
    gMinusMu.multiply(gMinusMu.transpose)
      .pointwiseDivide(varianceHat.multiply(varianceHat))
  }

}
