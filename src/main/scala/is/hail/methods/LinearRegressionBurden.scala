package is.hail.methods

import breeze.linalg._
import is.hail.annotations.{Annotation, Inserter}
import is.hail.keytable.KeyTable
import is.hail.stats._
import is.hail.utils._
import is.hail.expr._
import is.hail.variant._
import net.sourceforge.jdistlib.T
import org.apache.spark.sql.Row

object LinearRegressionBurden {

  def apply(vds: VariantDataset,
    keyName: String,
    variantKeySetVA: String,
    aggregateExpr: String,
    ySA: String,
    covSA: Array[String]): (KeyTable, KeyTable) = {

    val (y, cov, completeSamples) = RegressionUtils.getPhenoCovCompleteSamples(vds, ySA, covSA)

    val n = y.size
    val k = cov.cols
    val d = n - k - 1

    if (d < 1)
      fatal(s"$n samples and $k ${ plural(k, "covariate") } including intercept implies $d degrees of freedom.")

    info(s"Running linreg_burden, aggregated by key $keyName on $n samples with $k ${ plural(k, "covariate") } including intercept...")

    val completeSamplesSet = completeSamples.toSet

    if (completeSamplesSet(keyName))
      fatal(s"Sample name conflicts with the key name $keyName")

    def sampleKT = vds.filterSamples((s, sa) => completeSamplesSet(s))
      .aggregateBySamplePerVariantKey(keyName, variantKeySetVA, aggregateExpr)

    val emptyStats = Annotation.emptyIndexedSeq(LinearRegression.schema.fields.size)

    val Qt = qr.reduced.justQ(cov).t
    val Qty = Qt * y

    val sc = sampleKT.hc.sc
    val yBc = sc.broadcast(y)
    val QtBc = sc.broadcast(Qt)
    val QtyBc = sc.broadcast(Qty)
    val yypBc = sc.broadcast((y dot y) - (Qty dot Qty))

    val linregRDD = sampleKT.mapAnnotations { keyedRow =>
      val key = keyedRow.get(0).asInstanceOf[String]

      RegressionUtils.denseStats(keyedRow, y) match {
        case Some((x, xx, xy)) =>
          val qtx = QtBc.value * x
          val qty = QtyBc.value
          val xxp: Double = xx - (qtx dot qtx)
          val xyp: Double = xy - (qtx dot qty)
          val yyp: Double = yypBc.value

          val b = xyp / xxp
          val se = math.sqrt((yyp / xxp - b * b) / d)
          val t = b / se
          val p = 2 * T.cumulative(-math.abs(t), d, true, false)

          Row(key, b, se, t, p)
        case None =>
          Row(key +: emptyStats)
      }
    }

    def linregSignature = TStruct(keyName -> TString).merge(LinearRegression.schema)._1

    val linregKT = new KeyTable(sampleKT.hc, linregRDD, signature = linregSignature, keyNames = Array(keyName))

    (linregKT, sampleKT)
  }
}