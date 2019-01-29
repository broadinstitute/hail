package is.hail.expr.ir.functions

import is.hail.annotations.{Region, StagedRegionValueBuilder}
import is.hail.asm4s.{AsmFunction3, Code}
import is.hail.expr.ir._
import is.hail.expr.types._
import org.apache.commons.math3.special.Gamma
import is.hail.stats._
import is.hail.utils._
import is.hail.asm4s
import is.hail.expr.types.virtual._

object MathFunctions extends RegistryFunctions {
  def log(x: Double, b: Double): Double = math.log(x) / math.log(b)

  def gamma(x: Double): Double = Gamma.gamma(x)

  def floor(x: Float): Float = math.floor(x).toFloat

  def floor(x: Double): Double = math.floor(x)

  def ceil(x: Float): Float = math.ceil(x).toFloat

  def ceil(x: Double): Double = math.ceil(x)

  def mod(x: Int, y: Int): Int = {
    if (y == 0)
      fatal(s"$x % 0: modulo by zero")
    java.lang.Math.floorMod(x, y)
  }

  def mod(x: Long, y: Long): Long = {
    if (y == 0L)
      fatal(s"$x % 0: modulo by zero")
    java.lang.Math.floorMod(x, y)
  }

  def mod(x: Float, y: Float): Float = {
    if (y == 0.0)
      fatal(s"$x % 0: modulo by zero")
    val t = x % y
    if (t < 0) t + y else t
  }
  def mod(x: Double, y: Double): Double = {
    if (y == 0.0)
      fatal(s"$x % 0: modulo by zero")
    val t = x % y
    if (t < 0) t + y else t
  }

  def pow(x: Int, y: Int): Double = math.pow(x, y)
  def pow(x: Long, y: Long): Double = math.pow(x, y)
  def pow(x: Float, y: Float): Double = math.pow(x, y)
  def pow(x: Double, y: Double): Double = math.pow(x, y)

  def floorDiv(x: Int, y: Int): Int = {
    if (y == 0)
      fatal(s"$x // 0: integer division by zero")
    java.lang.Math.floorDiv(x, y)
  }


  def floorDiv(x: Long, y: Long): Long = {
    if (y == 0L)
      fatal(s"$x // 0: integer division by zero")
    java.lang.Math.floorDiv(x, y)
  }

  def floorDiv(x: Float, y: Float): Float = math.floor(x / y).toFloat

  def floorDiv(x: Double, y: Double): Double = math.floor(x / y)

  def approxEqual(x: Double, y: Double, tolerance: Double, absolute: Boolean, nanSame: Boolean): Boolean = {
    val withinTol =
      if (absolute)
        math.abs(x - y) <= tolerance
      else
        D_==(x, y, tolerance)
    x == y || withinTol || (nanSame && x.isNaN && y.isNaN)
  }

  def iruniroot(region: Region, irf: AsmFunction3[Region, Double, Boolean, Double], min: Double, max: Double): java.lang.Double = {
    val f: Double => Double = irf(region, _, false)
    if (!(min < max))
      fatal(s"min must be less than max in call to uniroot, got: min $min, max $max")

    val fmin = f(min)
    val fmax = f(max)

    if (fmin * fmax > 0.0)
      fatal(s"sign of endpoints must have opposite signs, got: f(min) = $fmin, f(max) = $fmax")

    val r = uniroot(f, min, max)
    if (r.isEmpty)
      null
    else
      r.get
  }
  
  def irentropy(s: String): Double = entropy(s)

  val mathPackageClass: Class[_] = Class.forName("scala.math.package$")

  def registerAll() {
    val thisClass = getClass
    val statsPackageClass = Class.forName("is.hail.stats.package$")
    val jMathClass = classOf[java.lang.Math]
    val jIntegerClass = classOf[java.lang.Integer]
    val jFloatClass = classOf[java.lang.Float]
    val jDoubleClass = classOf[java.lang.Double]    

    // numeric conversions
    registerIR("toInt32", tnum("T"), TInt32())(x => Cast(x, TInt32()))
    registerIR("toInt64", tnum("T"), TInt64())(x => Cast(x, TInt64()))
    registerIR("toFloat32", tnum("T"), TFloat32())(x => Cast(x, TFloat32()))
    registerIR("toFloat64", tnum("T"), TFloat64())(x => Cast(x, TFloat64()))
    
    registerScalaFunction("abs", TInt32(), TInt32())(mathPackageClass, "abs")
    registerScalaFunction("abs", TInt64(), TInt64())(mathPackageClass, "abs")
    registerScalaFunction("abs", TFloat32(), TFloat32())(mathPackageClass, "abs")
    registerScalaFunction("abs", TFloat64(), TFloat64())(mathPackageClass, "abs")

    registerScalaFunction("**", TInt32(), TInt32(), TFloat64())(thisClass, "pow")
    registerScalaFunction("**", TInt64(), TInt64(), TFloat64())(thisClass, "pow")
    registerScalaFunction("**", TFloat32(), TFloat32(), TFloat64())(thisClass, "pow")
    registerScalaFunction("**", TFloat64(), TFloat64(), TFloat64())(thisClass, "pow")

    registerScalaFunction("exp", TFloat64(), TFloat64())(mathPackageClass, "exp")
    registerScalaFunction("log10", TFloat64(), TFloat64())(mathPackageClass, "log10")
    registerScalaFunction("sqrt", TFloat64(), TFloat64())(mathPackageClass, "sqrt")
    registerScalaFunction("log", TFloat64(), TFloat64())(mathPackageClass, "log")
    registerScalaFunction("log", TFloat64(), TFloat64(), TFloat64())(thisClass, "log")
    registerScalaFunction("gamma", TFloat64(), TFloat64())(thisClass, "gamma")

    registerScalaFunction("binomTest", TInt32(), TInt32(), TFloat64(), TInt32(), TFloat64())(statsPackageClass, "binomTest")

    registerScalaFunction("dbeta", TFloat64(), TFloat64(), TFloat64(), TFloat64())(statsPackageClass, "dbeta")

    registerScalaFunction("pnorm", TFloat64(), TFloat64())(statsPackageClass, "pnorm")
    registerScalaFunction("qnorm", TFloat64(), TFloat64())(statsPackageClass, "qnorm")

    registerScalaFunction("dpois", TFloat64(), TFloat64(), TFloat64())(statsPackageClass, "dpois")
    registerScalaFunction("dpois", TFloat64(), TFloat64(), TBoolean(), TFloat64())(statsPackageClass, "dpois")

    registerScalaFunction("ppois", TFloat64(), TFloat64(), TFloat64())(statsPackageClass, "ppois")
    registerScalaFunction("ppois", TFloat64(), TFloat64(), TBoolean(), TBoolean(), TFloat64())(statsPackageClass, "ppois")

    registerScalaFunction("qpois", TFloat64(), TFloat64(), TInt32())(statsPackageClass, "qpois")
    registerScalaFunction("qpois", TFloat64(), TFloat64(), TBoolean(), TBoolean(), TInt32())(statsPackageClass, "qpois")

    registerScalaFunction("pchisqtail", TFloat64(), TFloat64(), TFloat64())(statsPackageClass, "chiSquaredTail")
    registerScalaFunction("qchisqtail", TFloat64(), TFloat64(), TFloat64())(statsPackageClass, "inverseChiSquaredTail")

    registerScalaFunction("floor", TFloat32(), TFloat32())(thisClass, "floor")
    registerScalaFunction("floor", TFloat64(), TFloat64())(thisClass, "floor")

    registerScalaFunction("ceil", TFloat32(), TFloat32())(thisClass, "ceil")
    registerScalaFunction("ceil", TFloat64(), TFloat64())(thisClass, "ceil")

    registerScalaFunction("%", TInt32(), TInt32(), TInt32())(thisClass, "mod")
    registerScalaFunction("%", TInt64(), TInt64(), TInt64())(thisClass, "mod")
    registerScalaFunction("%", TFloat32(), TFloat32(), TFloat32())(thisClass, "mod")
    registerScalaFunction("%", TFloat64(), TFloat64(), TFloat64())(thisClass, "mod")

    registerJavaStaticFunction("isnan", TFloat32(), TBoolean())(jFloatClass, "isNaN")
    registerJavaStaticFunction("isnan", TFloat64(), TBoolean())(jDoubleClass, "isNaN")

    registerJavaStaticFunction("is_finite", TFloat32(), TBoolean())(jFloatClass, "isFinite")
    registerJavaStaticFunction("is_finite", TFloat64(), TBoolean())(jDoubleClass, "isFinite")

    registerJavaStaticFunction("is_infinite", TFloat32(), TBoolean())(jFloatClass, "isInfinite")
    registerJavaStaticFunction("is_infinite", TFloat64(), TBoolean())(jDoubleClass, "isInfinite")

    registerJavaStaticFunction("sign", TInt32(), TInt32())(jIntegerClass, "signum")
    registerScalaFunction("sign", TInt64(), TInt64())(mathPackageClass, "signum")
    registerJavaStaticFunction("sign", TFloat32(), TFloat32())(jMathClass, "signum")
    registerJavaStaticFunction("sign", TFloat64(), TFloat64())(jMathClass, "signum")
    
    registerScalaFunction("approxEqual", TFloat64(), TFloat64(), TFloat64(), TBoolean(), TBoolean(), TBoolean())(thisClass, "approxEqual")

    registerWrappedScalaFunction("entropy", TString(), TFloat64())(thisClass, "irentropy")

    registerCode("fisher_exact_test", TInt32(), TInt32(), TInt32(), TInt32(), fetStruct){ case (mb, a, b, c, d) =>
      val res = mb.newLocal[Array[Double]]
      val srvb = new StagedRegionValueBuilder(mb, fetStruct.physicalType)
      Code(
        res := Code.invokeScalaObject[Int, Int, Int, Int, Array[Double]](statsPackageClass, "fisherExactTest", a, b, c, d),
        srvb.start(),
        srvb.addDouble(res(0)),
        srvb.advance(),
        srvb.addDouble(res(1)),
        srvb.advance(),
        srvb.addDouble(res(2)),
        srvb.advance(),
        srvb.addDouble(res(3)),
        srvb.advance(),
        srvb.offset
      )
    }
    
    registerCode("chi_squared_test", TInt32(), TInt32(), TInt32(), TInt32(), chisqStruct){ case (mb, a, b, c, d) =>
      val res = mb.newLocal[Array[Double]]
      val srvb = new StagedRegionValueBuilder(mb, chisqStruct.physicalType)
      Code(
        res := Code.invokeScalaObject[Int, Int, Int, Int, Array[Double]](statsPackageClass, "chiSquaredTest", a, b, c, d),
        srvb.start(),
        srvb.addDouble(res(0)),
        srvb.advance(),
        srvb.addDouble(res(1)),
        srvb.advance(),
        srvb.offset
      )
    }

    registerCode("contingency_table_test", TInt32(), TInt32(), TInt32(), TInt32(), TInt32(), chisqStruct){ case (mb, a, b, c, d, min_cell_count) =>
      val res = mb.newLocal[Array[Double]]
      val srvb = new StagedRegionValueBuilder(mb, chisqStruct.physicalType)
      Code(
        res := Code.invokeScalaObject[Int, Int, Int, Int, Int, Array[Double]](statsPackageClass, "contingencyTableTest", a, b, c, d, min_cell_count),
        srvb.start(),
        srvb.addDouble(res(0)),
        srvb.advance(),
        srvb.addDouble(res(1)),
        srvb.advance(),
        srvb.offset
      )
    }

    registerCode("hardy_weinberg_test", TInt32(), TInt32(), TInt32(), hweStruct){ case (mb, nHomRef, nHet, nHomVar) =>
      val res = mb.newLocal[Array[Double]]
      val srvb = new StagedRegionValueBuilder(mb, hweStruct.physicalType)
      Code(
        res := Code.invokeScalaObject[Int, Int, Int, Array[Double]](statsPackageClass, "hardyWeinbergTest", nHomRef, nHet, nHomVar),
        srvb.start(),
        srvb.addDouble(res(0)),
        srvb.advance(),
        srvb.addDouble(res(1)),
        srvb.advance(),
        srvb.offset
      )
    }
  }
}
