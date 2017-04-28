package is.hail.methods

import breeze.linalg._
import is.hail.annotations.Annotation
import is.hail.expr._
import is.hail.stats._
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.rdd.RDD

object LogisticRegression {

  def apply(vds: VariantDataset, test: String, yExpr: String, covExpr: Array[String], root: String): VariantDataset = {
    require(vds.wasSplit)

    def tests = Map("wald" -> WaldTest, "lrt" -> LikelihoodRatioTest, "score" -> ScoreTest, "firth" -> FirthTest)
    if (!tests.isDefinedAt(test))
      fatal(s"Supported tests are ${ tests.keys.mkString(", ") }, got: $test")

    val logRegTest = tests(test)

    val (y, cov, completeSamples) = RegressionUtils.getPhenoCovCompleteSamples(vds, yExpr, covExpr)
    val sampleMask = vds.sampleIds.map(completeSamples.toSet).toArray

    if (!y.forall(yi => yi == 0d || yi == 1d))
      fatal(s"For logistic regression, phenotype must be Boolean or numeric with all present values equal to 0 or 1")

    val n = y.size
    val k = cov.cols
    val d = n - k - 1

    if (d < 1)
      fatal(s"$n samples and $k ${ plural(k, "covariate") } including intercept implies $d degrees of freedom.")

    info(s"Running $test logistic regression on $n samples with $k ${ plural(k, "covariate") } including intercept...")

    val nullModel = new LogisticRegressionModel(cov, y)
    val nullFit = nullModel.fit()

    if (!nullFit.converged)
      fatal("Failed to fit (unregulatized) logistic regression null model (covariates only): " + (
        if (nullFit.exploded)
          s"exploded at Newton iteration ${ nullFit.nIter }"
        else
          "Newton iteration failed to converge"))

    val sc = vds.sparkContext
    val sampleMaskBc = sc.broadcast(sampleMask)
    val yBc = sc.broadcast(y)
    val XBc = sc.broadcast(new DenseMatrix[Double](n, k + 1, cov.toArray ++ Array.ofDim[Double](n)))
    val nullFitBc = sc.broadcast(nullFit)
    val logRegTestBc = sc.broadcast(logRegTest)

    val pathVA = Parser.parseAnnotationRoot(root, Annotation.VARIANT_HEAD)
    val (newVAS, inserter) = vds.insertVA(logRegTest.schema, pathVA)
    val emptyStats = logRegTest.emptyStats

    vds.copy(rdd = vds.rdd.mapPartitions( { it =>
      val X = XBc.value.copy
      it.map { case (v, (va, gs)) =>
        val logregAnnot =
          if (RegressionUtils.setLastColumnToMaskedGts(X, gs.hardCallGenotypeIterator, sampleMaskBc.value))
            logRegTestBc.value.test(X, yBc.value, nullFitBc.value).toAnnotation(emptyStats)
          else
            null

        val newAnnotation = inserter(va, logregAnnot)
        assert(newVAS.typeCheck(newAnnotation))
        (v, (newAnnotation, gs))
      }
    }, preservesPartitioning = true).asOrderedRDD).copy(vaSignature = newVAS)
  }
}

case class LogisticRegression(rdd: RDD[(Variant, Annotation)], schema: Type)