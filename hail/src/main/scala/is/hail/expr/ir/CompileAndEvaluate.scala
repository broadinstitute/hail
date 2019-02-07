package is.hail.expr.ir

import is.hail.annotations.{Region, RegionValueBuilder, SafeRow}
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.types.physical.{PBaseStruct, PType}
import is.hail.expr.types.virtual.{TStruct, TTuple, Type}
import is.hail.utils.{FastIndexedSeq, FastSeq}
import org.apache.spark.sql.Row
import org.json4s.jackson.JsonMethods

object CompileAndEvaluate {
  def evaluateToJSON(ir: IR): String = {
    val t = ir.typ
    val value = apply[Any](ir)
    JsonMethods.compact(JSONAnnotationImpex.exportAnnotation(value, t))
  }

  def apply[T](ir: IR): T = apply(ir, Env.empty[(Any, Type)], FastIndexedSeq()).asInstanceOf[T]

  def apply[T](ir: IR, optimize: Boolean): T = apply(ir, Env.empty[(Any, Type)], FastIndexedSeq(), optimize).asInstanceOf[T]

  def apply[T](ir0: IR,
    env: Env[(Any, Type)],
    args: IndexedSeq[(Any, Type)],
    optimize: Boolean = true
  ): T = {
    var ir = ir0

    def optimizeIR(canGenerateLiterals: Boolean) {
      ir = Optimize(ir, noisy = true, canGenerateLiterals)
      TypeCheck(ir, env.mapValues(_._2), None)
    }

    if (optimize) optimizeIR(true)
    ir = LiftNonCompilable(ir).asInstanceOf[IR]
    ir = LowerMatrixIR(ir)
    if (optimize) optimizeIR(true)

    val (evalIR, ncValue, ncType, ncVar) = InterpretNonCompilable(ir0)
    ir = evalIR

    val argsInVar = genUID()
    val argsInType = TTuple(args.map(_._2))
    val argsInValue = Row.fromSeq(args.map(_._1))

    // don't do exra work
    val rewriteArgsIn: IR => IR = if (args.isEmpty) identity[IR] else {
      def rewriteArgsIn(x: IR): IR = {
        x match {
          case In(i, t) =>
            GetTupleElement(Ref(argsInVar, argsInType), i)
          case _ =>
            MapIR(rewriteArgsIn)(x)
        }
      }
      rewriteArgsIn
    }

    val (envVar, envType, envValue, rewriteEnv): (String, TStruct, Any, IR => IR) = {
      env.m.toArray match {
        // common case; don't do extra work
        case Array((envVar, (envValue, envType: TStruct))) => (envVar, envType, envValue, identity[IR])
        case eArray =>
          val envVar = genUID()
          val envType = TStruct(eArray.map { case (name, (_, t)) => name -> t}: _*)
          val envValue = Row.fromSeq(eArray.map(_._2._1))
          (envVar,
            envType,
            envValue,
            Subst(_, Env[IR](envType.fieldNames.map(s => s -> GetField(Ref(envVar, envType), s)): _*)))
      }
    }

    ir = rewriteArgsIn(ir)
    ir = rewriteEnv(ir)

    val ncPType = PType.canonical(ncType)
    val argsInPType = PType.canonical(argsInType)
    val envPType = PType.canonical(envType)

    val (resultPType, f) = Compile[Long, Long, Long, Long](
      ncVar, ncPType,
      argsInVar, argsInPType,
      envVar, envPType,
      MakeTuple(FastSeq(ir)))

    Region.scoped { region =>
      val rvb = new RegionValueBuilder(region)
      rvb.start(ncPType)
      rvb.addAnnotation(ncType, ncValue)
      val ncOffset = rvb.end()

      rvb.start(argsInPType)
      rvb.addAnnotation(argsInType, argsInValue)
      val argsInOffset = rvb.end()

      rvb.start(envPType)
      rvb.addAnnotation(envType, envValue)
      val envOffset = rvb.end()

      val resultOff = f(0)(region,
        ncOffset, ncValue == null,
        argsInOffset, argsInValue == null,
        envOffset, envValue == null)
      SafeRow(resultPType.asInstanceOf[PBaseStruct], region, resultOff).getAs[T](0)
    }
  }
}
