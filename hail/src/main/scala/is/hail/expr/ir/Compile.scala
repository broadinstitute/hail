package is.hail.expr.ir

import java.util
import java.util.Map.Entry

import is.hail.annotations._
import is.hail.annotations.aggregators.RegionValueAggregator
import is.hail.asm4s._
import is.hail.expr.types.physical.PType
import is.hail.expr.types.virtual.Type
import is.hail.utils._

import scala.reflect.{ClassTag, classTag}

class CacheMap[K, V] {
  val capacity: Int = 30

  val m = new util.LinkedHashMap[K, V](capacity, 0.75f, true) {
    override def removeEldestEntry(eldest: Entry[K, V]): Boolean = size() > capacity
  }

  def get(k: K): Option[V] = Option(m.get(k))

  def +=(p: (K, V)): Unit = m.put(p._1, p._2)

  def size: Int = m.size()
}

case class CodeCacheKey(args: Seq[(String, PType)], nSpecialArgs: Int, body: IR)

case class CodeCacheValue(typ: PType, f: Int => Any)

class NormalizeNames {
  var count: Int = 0

  def gen(): String = {
    count += 1
    count.toString
  }

  def apply(ir: IR, env: Env[String]): IR = apply(ir, env, None)

  def apply(ir: IR, env: Env[String], aggEnv: Option[Env[String]]): IR = {
    def normalize(ir: IR, env: Env[String] = env, aggEnv: Option[Env[String]] = aggEnv): IR = apply(ir, env, aggEnv)

    ir match {
      case Let(name, value, body) =>
        val newName = gen()
        Let(newName, normalize(value), normalize(body, env.bind(name, newName)))
      case Ref(name, typ) =>
        Ref(env.lookup(name), typ)
      case AggLet(name, value, body) =>
        val newName = gen()
        AggLet(newName, normalize(value), normalize(body, env, Some(aggEnv.get.bind(name, newName))))
      case ArraySort(a, left, right, compare) =>
        val newLeft = gen()
        val newRight = gen()
        ArraySort(normalize(a), newLeft, newRight, normalize(compare, env.bind(left -> newLeft, right -> newRight)))
      case ArrayMap(a, name, body) =>
        val newName = gen()
        ArrayMap(normalize(a), newName, normalize(body, env.bind(name, newName)))
      case ArrayFilter(a, name, body) =>
        val newName = gen()
        ArrayFilter(normalize(a), newName, normalize(body, env.bind(name, newName)))
      case ArrayFlatMap(a, name, body) =>
        val newName = gen()
        ArrayFlatMap(normalize(a), newName, normalize(body, env.bind(name, newName)))
      case ArrayFold(a, zero, accumName, valueName, body) =>
        val newAccumName = gen()
        val newValueName = gen()
        ArrayFold(normalize(a), normalize(zero), newAccumName, newValueName, normalize(body, env.bind(accumName -> newAccumName, valueName -> newValueName)))
      case ArrayScan(a, zero, accumName, valueName, body) =>
        val newAccumName = gen()
        val newValueName = gen()
        ArrayScan(normalize(a), normalize(zero), newAccumName, newValueName, normalize(body, env.bind(accumName -> newAccumName, valueName -> newValueName)))
      case ArrayFor(a, valueName, body) =>
        val newValueName = gen()
        ArrayFor(normalize(a), newValueName, normalize(body, env.bind(valueName, newValueName)))
      case ArrayAgg(a, name, body) =>
        assert(aggEnv.isEmpty)
        val newName = gen()
        ArrayAgg(normalize(a), newName, normalize(body, env, Some(env.bind(name, newName))))
      case ArrayLeftJoinDistinct(left, right, l, r, keyF, joinF) =>
        val newL = gen()
        val newR = gen()
        val newEnv = env.bind(l -> newL, r -> newR)
        ArrayLeftJoinDistinct(normalize(left), normalize(right), newL, newR, normalize(keyF, newEnv), normalize(joinF, newEnv))
      case AggExplode(a, name, aggBody) =>
        val newName = gen()
        AggExplode(normalize(a, aggEnv.get, None), newName, normalize(aggBody, env, Some(aggEnv.get.bind(name, newName))))
      case AggArrayPerElement(a, name, aggBody) =>
        val newName = gen()
        AggArrayPerElement(normalize(a, aggEnv.get, None), newName, normalize(aggBody, env, Some(aggEnv.get.bind(name, newName))))
      case ApplyAggOp(ctorArgs, initOpArgs, seqOpArgs, aggSig) =>
        ApplyAggOp(ctorArgs.map(a => normalize(a)),
          initOpArgs.map(_.map(a => normalize(a))),
          seqOpArgs.map(a => normalize(a, aggEnv.get, None)),
          aggSig)
      case _ =>
        // FIXME when Binding lands, assert nothing is bound in any child
        Copy(ir, ir.children.map {
          case c: IR => normalize(c)
        })
    }
  }
}

object Compile {
  private[this] val codeCache: CacheMap[CodeCacheKey, CodeCacheValue] = new CacheMap()

  private def apply[F >: Null : TypeInfo, R: TypeInfo : ClassTag](
    args: Seq[(String, PType, ClassTag[_])],
    argTypeInfo: Array[MaybeGenericTypeInfo[_]],
    body: IR,
    nSpecialArgs: Int
  ): (PType, Int => F) = {

    val normalizeNames = new NormalizeNames
    val normalizedBody = normalizeNames(body,
      Env(args.map { case (n, _, _) => n -> n }: _*))
    val k = CodeCacheKey(args.map { case (n, pt, _) => (n, pt) }, nSpecialArgs, normalizedBody)
    codeCache.get(k) match {
      case Some(v) => 
        return (v.typ, v.f.asInstanceOf[Int => F])
      case None =>
    }

    val fb = new EmitFunctionBuilder[F](argTypeInfo, GenericTypeInfo[R]())

    var ir = body
    ir = Optimize(ir, noisy = false, canGenerateLiterals = false)
    TypeCheck(ir, Env.empty[Type].bind(args.map { case (name, t, _) => name -> t.virtualType}: _*), None)

    val env = args
      .zipWithIndex
      .foldLeft(Env.empty[IR]) { case (e, ((n, t, _), i)) => e.bind(n, In(i, t.virtualType)) }

    ir = Subst(ir, env)
    assert(TypeToIRIntermediateClassTag(ir.typ) == classTag[R])

    Emit(ir, fb, nSpecialArgs)

    val f = fb.resultWithIndex()
    codeCache += k -> CodeCacheValue(ir.pType, f)
    assert(codeCache.get(k).isDefined)
    (ir.pType, f)
  }

  def apply[F >: Null : TypeInfo, R: TypeInfo : ClassTag](
    args: Seq[(String, PType, ClassTag[_])],
    body: IR,
    nSpecialArgs: Int
  ): (PType, Int => F) = {
    assert(args.forall { case (_, t, ct) => TypeToIRIntermediateClassTag(t.virtualType) == ct })

    val ab = new ArrayBuilder[MaybeGenericTypeInfo[_]]()
    ab += GenericTypeInfo[Region]()
    if (nSpecialArgs == 2)
      ab += GenericTypeInfo[Array[RegionValueAggregator]]()
    args.foreach { case (_, t, _) =>
      ab += GenericTypeInfo()(typeToTypeInfo(t))
      ab += GenericTypeInfo[Boolean]()
    }

    val argTypeInfo: Array[MaybeGenericTypeInfo[_]] = ab.result()

    Compile[F, R](args, argTypeInfo, body, nSpecialArgs)
  }

  def apply[R: TypeInfo : ClassTag](body: IR): (PType, Int => AsmFunction1[Region, R]) = {
    apply[AsmFunction1[Region, R], R](FastSeq[(String, PType, ClassTag[_])](), body, 1)
  }

  def apply[T0: ClassTag, R: TypeInfo : ClassTag](
    name0: String,
    typ0: PType,
    body: IR): (PType, Int => AsmFunction3[Region, T0, Boolean, R]) = {

    apply[AsmFunction3[Region, T0, Boolean, R], R](FastSeq((name0, typ0, classTag[T0])), body, 1)
  }

  def apply[T0: ClassTag, T1: ClassTag, R: TypeInfo : ClassTag](
    name0: String,
    typ0: PType,
    name1: String,
    typ1: PType,
    body: IR): (PType, Int => AsmFunction5[Region, T0, Boolean, T1, Boolean, R]) = {

    apply[AsmFunction5[Region, T0, Boolean, T1, Boolean, R], R](FastSeq((name0, typ0, classTag[T0]), (name1, typ1, classTag[T1])), body, 1)
  }

  def apply[
  T0: TypeInfo : ClassTag,
  T1: TypeInfo : ClassTag,
  T2: TypeInfo : ClassTag,
  R: TypeInfo : ClassTag
  ](name0: String,
    typ0: PType,
    name1: String,
    typ1: PType,
    name2: String,
    typ2: PType,
    body: IR
  ): (PType, Int => AsmFunction7[Region, T0, Boolean, T1, Boolean, T2, Boolean, R]) = {
    apply[AsmFunction7[Region, T0, Boolean, T1, Boolean, T2, Boolean, R], R](FastSeq(
      (name0, typ0, classTag[T0]),
      (name1, typ1, classTag[T1]),
      (name2, typ2, classTag[T2])
    ), body, 1)
  }

  def apply[
  T0: TypeInfo : ClassTag,
  T1: TypeInfo : ClassTag,
  T2: TypeInfo : ClassTag,
  T3: TypeInfo : ClassTag,
  R: TypeInfo : ClassTag
  ](name0: String, typ0: PType,
    name1: String, typ1: PType,
    name2: String, typ2: PType,
    name3: String, typ3: PType,
    body: IR
  ): (PType, Int => AsmFunction9[Region, T0, Boolean, T1, Boolean, T2, Boolean, T3, Boolean, R]) = {
    apply[AsmFunction9[Region, T0, Boolean, T1, Boolean, T2, Boolean, T3, Boolean, R], R](FastSeq(
      (name0, typ0, classTag[T0]),
      (name1, typ1, classTag[T1]),
      (name2, typ2, classTag[T2]),
      (name3, typ3, classTag[T3])
    ), body, 1)
  }

  def apply[
  T0: ClassTag,
  T1: ClassTag,
  T2: ClassTag,
  T3: ClassTag,
  T4: ClassTag,
  T5: ClassTag,
  R: TypeInfo : ClassTag
  ](name0: String, typ0: PType,
    name1: String, typ1: PType,
    name2: String, typ2: PType,
    name3: String, typ3: PType,
    name4: String, typ4: PType,
    name5: String, typ5: PType,
    body: IR
  ): (PType, Int => AsmFunction13[Region, T0, Boolean, T1, Boolean, T2, Boolean, T3, Boolean, T4, Boolean, T5, Boolean, R]) = {

    apply[AsmFunction13[Region, T0, Boolean, T1, Boolean, T2, Boolean, T3, Boolean, T4, Boolean, T5, Boolean, R], R](FastSeq(
      (name0, typ0, classTag[T0]),
      (name1, typ1, classTag[T1]),
      (name2, typ2, classTag[T2]),
      (name3, typ3, classTag[T3]),
      (name4, typ4, classTag[T4]),
      (name5, typ5, classTag[T5])
    ), body, 1)
  }
}

object CompileWithAggregators {
  type Compiler[F] = (IR) => (PType, Int => F)
  type IRAggFun1[T0] =
    AsmFunction4[Region, Array[RegionValueAggregator], T0, Boolean, Unit]
  type IRAggFun2[T0, T1] =
    AsmFunction6[Region, Array[RegionValueAggregator],
      T0, Boolean,
      T1, Boolean,
      Unit]
  type IRAggFun3[T0, T1, T2] =
    AsmFunction8[Region, Array[RegionValueAggregator],
      T0, Boolean,
      T1, Boolean,
      T2, Boolean,
      Unit]
  type IRAggFun4[T0, T1, T2, T3] =
    AsmFunction10[Region, Array[RegionValueAggregator],
      T0, Boolean,
      T1, Boolean,
      T2, Boolean,
      T3, Boolean,
      Unit]
  type IRFun1[T0, R] =
    AsmFunction3[Region, T0, Boolean, R]
  type IRFun2[T0, T1, R] =
    AsmFunction5[Region, T0, Boolean, T1, Boolean, R]
  type IRFun3[T0, T1, T2, R] =
    AsmFunction7[Region, T0, Boolean, T1, Boolean, T2, Boolean, R]
  type IRFun4[T0, T1, T2, T3, R] =
    AsmFunction9[Region, T0, Boolean, T1, Boolean, T2, Boolean, T3, Boolean, R]

  def liftScan(ir: IR): IR = ir match {
    case ApplyScanOp(a, b, c, d) =>
      ApplyAggOp(a, b, c, d)
    case x => MapIR(liftScan)(x)
  }

  def compileAggIRs[
  FAggInit >: Null : TypeInfo,
  FAggSeq >: Null : TypeInfo
  ](initScopeArgs: Seq[(String, PType, ClassTag[_])],
    aggScopeArgs: Seq[(String, PType, ClassTag[_])],
    body: IR, aggResultName: String
  ): (Array[RegionValueAggregator], (IR, Compiler[FAggInit]), (IR, Compiler[FAggSeq]), PType, IR) = {
    assert((initScopeArgs ++ aggScopeArgs).forall { case (_, t, ct) => TypeToIRIntermediateClassTag(t.virtualType) == ct })

    val ExtractedAggregators(postAggIR, aggResultType, initOpIR, seqOpIR, rvAggs) = ExtractAggregators(body, aggResultName)
    val compileInitOp = (initOp: IR) => Compile[FAggInit, Unit](initScopeArgs, initOp, 2)
    val compileSeqOp = (seqOp: IR) => Compile[FAggSeq, Unit](aggScopeArgs, seqOp, 2)

    (rvAggs,
      (initOpIR, compileInitOp),
      (seqOpIR, compileSeqOp),
      aggResultType,
      postAggIR)
  }

  def apply[
  F0 >: Null : TypeInfo,
  F1 >: Null : TypeInfo
  ](initScopeArgs: Seq[(String, PType, ClassTag[_])],
    aggScopeArgs: Seq[(String, PType, ClassTag[_])],
    body: IR, aggResultName: String,
    transformInitOp: (Int, IR) => IR,
    transformSeqOp: (Int, IR) => IR
  ): (Array[RegionValueAggregator], Int => F0, Int => F1, PType, IR) = {
    val (rvAggs, (initOpIR, compileInitOp),
      (seqOpIR, compileSeqOp),
      aggResultType, postAggIR
    ) = compileAggIRs[F0, F1](initScopeArgs, aggScopeArgs, body, aggResultName)

    val nAggs = rvAggs.length
    val (_, initOps) = compileInitOp(trace("initop", transformInitOp(nAggs, initOpIR)))
    val (_, seqOps) = compileSeqOp(trace("seqop", transformSeqOp(nAggs, seqOpIR)))
    (rvAggs, initOps, seqOps, aggResultType, postAggIR)
  }

  private[this] def trace(name: String, t: IR): IR = {
    log.info(name + " " + Pretty(t))
    t
  }

  def apply[
  T0: ClassTag,
  S0: ClassTag,
  S1: ClassTag
  ](name0: String, typ0: PType,
    aggName0: String, aggTyp0: PType,
    aggName1: String, aggTyp1: PType,
    body: IR, aggResultName: String,
    transformInitOp: (Int, IR) => IR,
    transformSeqOp: (Int, IR) => IR
  ): (Array[RegionValueAggregator],
    Int => IRAggFun1[T0],
    Int => IRAggFun2[S0, S1],
    PType,
    IR) = {
    val args = FastSeq((name0, typ0, classTag[T0]))

    val aggScopeArgs = FastSeq(
      (aggName0, aggTyp0, classTag[S0]),
      (aggName1, aggTyp1, classTag[S1]))

    apply[IRAggFun1[T0], IRAggFun2[S0, S1]](args, aggScopeArgs, body, aggResultName, transformInitOp, transformSeqOp)
  }

  def apply[
  T0: ClassTag,
  T1: ClassTag,
  S0: ClassTag,
  S1: ClassTag,
  S2: ClassTag
  ](name0: String, typ0: PType,
    name1: String, typ1: PType,
    aggName0: String, aggType0: PType,
    aggName1: String, aggType1: PType,
    aggName2: String, aggType2: PType,
    body: IR, aggResultName: String,
    transformInitOp: (Int, IR) => IR,
    transformSeqOp: (Int, IR) => IR
  ): (Array[RegionValueAggregator],
    Int => IRAggFun2[T0, T1],
    Int => IRAggFun3[S0, S1, S2],
    PType,
    IR) = {
    val args = FastSeq(
      (name0, typ0, classTag[T0]),
      (name1, typ1, classTag[T1]))

    val aggArgs = FastSeq(
      (aggName0, aggType0, classTag[S0]),
      (aggName1, aggType1, classTag[S1]),
      (aggName2, aggType2, classTag[S2]))

    apply[IRAggFun2[T0, T1], IRAggFun3[S0, S1, S2]](args, aggArgs, body, aggResultName, transformInitOp, transformSeqOp)
  }

  def apply[
    T0: ClassTag,
    S0: ClassTag,
    S1: ClassTag,
    S2: ClassTag
  ](name0: String, typ0: PType,
    aggName0: String, aggTyp0: PType,
    aggName1: String, aggTyp1: PType,
    aggName2: String, aggTyp2: PType,
    body: IR, aggResultName: String,
    transformInitOp: (Int, IR) => IR,
    transformSeqOp: (Int, IR) => IR
  ): (Array[RegionValueAggregator],
    Int => IRAggFun1[T0],
    Int => IRAggFun3[S0, S1, S2],
    PType,
    IR) = {
    val args = FastSeq((name0, typ0, classTag[T0]))

    val aggScopeArgs = FastSeq(
      (aggName0, aggTyp0, classTag[S0]),
      (aggName1, aggTyp1, classTag[S1]),
      (aggName2, aggTyp2, classTag[S1]))

    apply[IRAggFun1[T0], IRAggFun3[S0, S1, S2]](args, aggScopeArgs, body, aggResultName, transformInitOp, transformSeqOp)
  }

  def apply[
  T0: ClassTag,
  T1: ClassTag,
  S0: ClassTag,
  S1: ClassTag,
  S2: ClassTag,
  S3: ClassTag
  ](name0: String, typ0: PType,
    name1: String, typ1: PType,
    aggName0: String, aggType0: PType,
    aggName1: String, aggType1: PType,
    aggName2: String, aggType2: PType,
    aggName3: String, aggType3: PType,
    body: IR, aggResultName: String,
    transformInitOp: (Int, IR) => IR,
    transformSeqOp: (Int, IR) => IR
  ): (Array[RegionValueAggregator],
    Int => IRAggFun2[T0, T1],
    Int => IRAggFun4[S0, S1, S2, S3],
    PType,
    IR) = {
    val args = FastSeq(
      (name0, typ0, classTag[T0]),
      (name1, typ1, classTag[T1]))

    val aggArgs = FastSeq(
      (aggName0, aggType0, classTag[S0]),
      (aggName1, aggType1, classTag[S1]),
      (aggName2, aggType2, classTag[S2]),
      (aggName3, aggType3, classTag[S3]))

    apply[IRAggFun2[T0, T1], IRAggFun4[S0, S1, S2, S3]
      ](args, aggArgs, body, aggResultName, transformInitOp, transformSeqOp)
  }
}
