package is.hail.stats

import breeze.linalg.{Matrix, Vector}
import is.hail.annotations.Annotation
import is.hail.expr.typ.TStruct
import net.sourceforge.jdistlib.T

object LinearRegressionModel {
  def schema = TStruct(
    ("beta", TFloat64()),
    ("se", TFloat64()),
    ("tstat", TFloat64()),
    ("pval", TFloat64()))

  def fit(x: Vector[Double], y: Vector[Double], yyp: Double, qt: Matrix[Double], qty: Vector[Double], d: Int): Annotation = {
    val qtx = qt * x
    val xxp = (x dot x) - (qtx dot qtx)
    val xyp = (x dot y) - (qtx dot qty)

    val b = xyp / xxp
    val se = math.sqrt((yyp / xxp - b * b) / d)
    val t = b / se
    val p = 2 * T.cumulative(-math.abs(t), d, true, false)

    Annotation(b, se, t, p)
  }
}
