package is.hail.stats

import is.hail.SparkSuite
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.testng.annotations.Test
import org.testng.Assert.assertEquals
import breeze.stats._

import scala.collection.mutable

class BaldingNicholsModelSuite extends SparkSuite {

  @Test def testDeterminism() = {
    val K = 3
    val N = 10
    val M = 10
    val popDist = Array(1d, 2d, 3d)
    val FstOfPop = Array(0.1, 0.2, 0.3)
    val seed = 0

    val bnm1 = BaldingNicholsModel(sc, K, N, M, popDist, FstOfPop, seed, 2, "bn")
    val bnm2 = BaldingNicholsModel(sc, K, N, M, popDist, FstOfPop, seed, 2, "bn")

    assert(bnm1.rdd.collect().toSeq == bnm2.rdd.collect().toSeq)
    assert(bnm1.globalAnnotation == bnm2.globalAnnotation)
    assert(bnm1.sampleAnnotations == bnm2.sampleAnnotations)
  }

  @Test def testDimensions() = {
    val K = 5
    val N = 10
    val M = 100
    val popDist = Array(1.0, 2.0, 3.0, 4.0, 5.0)
    val FstOfPop = Array(0.1, 0.2, 0.3, 0.2, 0.2)
    val seed = 0

    val bnm = BaldingNicholsModel(sc, K, N, M, popDist, FstOfPop, seed, 2, "bn")

    val gs_by_variant = bnm.rdd.collect.map(x => x._2._2)
    assert(gs_by_variant.forall(_.size == 10))

    assert(bnm.rdd.count() == 100)
  }

  @Test def testStats() {
    val K = 10
    testStatsHelp(K, 100, 100, Array.fill(K)(1.0), Array.fill(K)(0.1), 0)
  }

  def testStatsHelp(K: Int, N: Int, M: Int, popDist: Array[Double], FstOfPop:Array[Double], seed: Int) = {
    val bnm = BaldingNicholsModel(sc, K, N, M, popDist, FstOfPop, seed, 4, "bn")

    //Test population distribution
    val populationArray = bnm.sampleAnnotations.toArray.map(_.toString.toInt)
    val populationCounts = populationArray.groupBy(x => x).values.toSeq.sortBy(_(0)).map(_.size)

    populationCounts.indices.foreach(index => {
      assertEquals(populationCounts(index) / N, popDist(index), Math.ceil(N / K * .1))
    })

    //Test AF distributions
    val arrayOfVARows = bnm.variantsAndAnnotations.collect().map(_._2).toSeq.asInstanceOf[mutable.WrappedArray[GenericRow]]
    val arrayOfVATuples = arrayOfVARows.map(row => row.get(0).asInstanceOf[GenericRow]).map(row => (row.get(0), row.get(1)).asInstanceOf[(Double, Vector[Double])])

    val AFStats = arrayOfVATuples.map(tuple => meanAndVariance(tuple._2))

    arrayOfVATuples.map(_._1).zip(AFStats).foreach{
      case (p, mv) =>
        assertEquals(p, mv.mean, .2) //Consider alternatives to .2
        assertEquals(.1 * p * (1 - p), mv.variance, .1)
    }

    //Test genotype distributions
    val meanGeno_mk = bnm.rdd
      .map(_._2._2.zip(populationArray).groupBy(_._2).toSeq.sortBy(_._1))
      .map(_.map(popIterPair => mean(popIterPair._2.map(_._1.gt.get.toDouble))).toArray)
      .collect()

    val p_mk = arrayOfVATuples.map(_._2.toArray)

    val meanDiff = (meanGeno_mk.flatten.sum - 2 * p_mk.flatten.sum) / (M * K)
    assertEquals(meanDiff, 0, .1)
  }
}
