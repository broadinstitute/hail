package is.hail.methods

import breeze.linalg._
import is.hail.annotations.Annotation
import is.hail.expr._
import is.hail.stats._
import is.hail.utils._
import is.hail.variant._

object LinearRegressionMultiPheno {
  def apply(vds: VariantDataset, ysExpr: Array[String], covExpr: Array[String], root: String, useDosages: Boolean, minAC: Int, minAF: Double): VariantDataset = {
    require(vds.wasSplit)

    val (y, cov, completeSamples) = RegressionUtils.getPhenosCovCompleteSamples(vds, ysExpr, covExpr)
    val completeSamplesSet = completeSamples.toSet
    val sampleMask = vds.sampleIds.map(completeSamplesSet).toArray
    val completeSampleIndex = (0 until vds.nSamples)
      .filter(i => completeSamplesSet(vds.sampleIds(i)))
      .toArray

    val n = y.rows
    val k = cov.cols
    val d = n - k - 1
    val dRec = 1d / d

    if (minAC < 1)
      fatal(s"Minumum alternate allele count must be a positive integer, got $minAC")
    if (minAF < 0d || minAF > 1d)
      fatal(s"Minumum alternate allele frequency must lie in [0.0, 1.0], got $minAF")
    val combinedMinAC = math.max(minAC, (math.ceil(2 * n * minAF) + 0.5).toInt)

    if (d < 1)
      fatal(s"$n samples and $k ${ plural(k, "covariate") } including intercept implies $d degrees of freedom.")

    info(s"Running linear regression for ${y.cols} ${ plural(y.cols, "phenotype") } on $n samples with $k ${ plural(k, "covariate") } including intercept...")

    val Qt = qr.reduced.justQ(cov).t
    val Qty = Qt * y

    val sc = vds.sparkContext
    val sampleMaskBc = sc.broadcast(sampleMask)
    val completeSampleIndexBc = sc.broadcast(completeSampleIndex)
    val yBc = sc.broadcast(y)
    val QtBc = sc.broadcast(Qt)
    val QtyBc = sc.broadcast(Qty)
    val yypBc = sc.broadcast(y.t(*, ::).map(r => r dot r) - Qty.t(*, ::).map(r => r dot r))

    val pathVA = Parser.parseAnnotationRoot(root, Annotation.VARIANT_HEAD)
    val (newVAS, inserter) = vds.insertVA(LinearRegressionModel.schemaMultiPheno, pathVA)

    vds.mapAnnotations { case (v, va, gs) =>
      val (x: Vector[Double], ac) =
        if (!useDosages) // replace by hardCalls in 0.2, with ac post-imputation
          RegressionUtils.hardCallsWithAC(gs, n, sampleMaskBc.value)
        else {
          val x = RegressionUtils.dosages(gs, completeSampleIndexBc.value)
          (x, sum(x))
        }

      // constant checking to be removed in 0.2
      val nonConstant = useDosages || !RegressionUtils.constantVector(x)
      
      val linregAnnot =
        if (ac >= combinedMinAC && nonConstant)
          LinearRegressionModel.fitMultiPheno(x, yBc.value, yypBc.value, QtBc.value, QtyBc.value, d).toAnnotation
        else
          null

      val newAnnotation = inserter(va, linregAnnot)
      assert(newVAS.typeCheck(newAnnotation))
      newAnnotation
    }.copy(vaSignature = newVAS)
  }
}