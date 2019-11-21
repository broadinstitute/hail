package is.hail.expr.ir

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import is.hail.HailContext
import is.hail.annotations._
import is.hail.annotations.aggregators._
import is.hail.asm4s.{Code, _}
import is.hail.expr.ir.functions.{MathFunctions, StringFunctions}
import is.hail.expr.types.physical._
import is.hail.expr.types.virtual._
import is.hail.io.{BufferSpec, InputBuffer, OutputBuffer, TypedCodecSpec}
import is.hail.linalg.LAPACKLibrary
import is.hail.utils._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.{existentials, postfixOps}

object Emit {
  type E = Env[(TypeInfo[_], Code[Boolean], Code[_])]

  type F = (Code[Boolean], Code[_]) => Code[Unit]

  private[ir] def toCode(ctx: ExecuteContext, ir: IR, fb: EmitFunctionBuilder[_], nSpecialArguments: Int): EmitTriplet = {
    emit(ctx: ExecuteContext, ir, fb, Env.empty, nSpecialArguments, None)
  }

  def apply(ctx: ExecuteContext, ir: IR, fb: EmitFunctionBuilder[_], nSpecialArguments: Int, aggs: Option[Array[AggSignature2]] = None) {
    val triplet = emit(ctx, ir, fb, Env.empty, nSpecialArguments, aggs)
    typeToTypeInfo(ir.typ) match {
      case ti: TypeInfo[t] =>
        fb.emit(Code(triplet.setup, triplet.m.mux(
          Code._throw(Code.newInstance[RuntimeException, String]("cannot return empty")),
          coerce[t](triplet.v))))
    }
  }

  private def emit(
    ctx: ExecuteContext,
    ir: IR,
    fb: EmitFunctionBuilder[_],
    env: E,
    nSpecialArguments: Int,
    aggs: Option[Array[AggSignature2]]): EmitTriplet = {
    TypeCheck(ir)
    val container = aggs.map { a =>
      val c = fb.addAggStates(a)
      Some(AggContainer(a, c))
    }.getOrElse(None)

    val baseTriplet = new Emit(ctx: ExecuteContext, fb.apply_method, nSpecialArguments).emit(ir, env, EmitRegion.default(fb.apply_method), container = container)

    EmitTriplet(
      baseTriplet.setup,
      baseTriplet.m,
      baseTriplet.v)
  }
}

object AggContainer {
  def fromVars(aggs: Array[AggSignature2], fb: EmitFunctionBuilder[_], region: ClassFieldRef[Region], off: ClassFieldRef[Long]): (AggContainer, Code[Unit], Code[Unit]) = {
    val states = agg.StateTuple(aggs.map(a => agg.Extract.getAgg(a).createState(fb)).toArray)
    val aggState = new agg.TupleAggregatorState(fb, states, region, off)

    val setup = Code(
      region := Region.stagedCreate(Region.REGULAR),
      region.load().setNumParents(aggs.length),
      off := region.load().allocate(aggState.storageType.alignment, aggState.storageType.byteSize),
      states.createStates(fb),
      aggState.newState)

    val cleanup = Code(
      region.load().invalidate(),
      region := Code._null)

    (AggContainer(aggs, aggState), setup, cleanup)
  }
  def fromFunctionBuilder(aggs: Array[AggSignature2], fb: EmitFunctionBuilder[_], varPrefix: String): (AggContainer, Code[Unit], Code[Unit]) =
    fromVars(aggs, fb, fb.newField[Region](s"${varPrefix}_top_region"), fb.newField[Long](s"${varPrefix}_off"))
}

case class AggContainer(aggs: Array[AggSignature2], container: agg.TupleAggregatorState) {

  def nested(i: Int, init: Boolean): Option[AggContainer] = {
    aggs(i).nested.map { n =>
      aggs(i).op match {
        case AggElements() | AggElementsLengthCheck() =>
          val state = container.states(i).asInstanceOf[agg.ArrayElementState]
          if (init)
            AggContainer(n.toArray, state.initContainer)
          else
            AggContainer(n.toArray, state.container)
        case Group() =>
          val state = container.states(i).asInstanceOf[agg.DictState]
          if (init)
            AggContainer(n.toArray, state.initContainer)
          else
            AggContainer(n.toArray, state.keyed.container)
      }
    }
  }
}

object EmitRegion {
  def default(mb: EmitMethodBuilder): EmitRegion = EmitRegion(mb, mb.getArg[Region](1))
}

case class EmitRegion(mb: EmitMethodBuilder, region: Code[Region]) {
  def baseRegion: Code[Region] = mb.getArg[Region](1)
}

case class EmitTriplet(setup: Code[Unit], m: Code[Boolean], v: Code[_]) {
  def value[T]: Code[T] = coerce[T](v)
}

case class EmitArrayTriplet(setup: Code[Unit], m: Option[Code[Boolean]], addElements: Code[Unit])

case class ArrayIteratorTriplet(calcLength: Code[Unit], length: Option[Code[Int]], arrayEmitter: Emit.F => EmitArrayTriplet) {
  def arrayEmitterFromBuilder(sab: StagedArrayBuilder): EmitArrayTriplet = {
    arrayEmitter( { (m: Code[Boolean], v: Code[_]) => m.mux(sab.addMissing(), sab.add(v)) } )
  }

  def wrapContinuation(contMap: (Emit.F, Code[Boolean], Code[_]) => Code[Unit]): ArrayIteratorTriplet =
    copy(calcLength = calcLength, length = length, arrayEmitter = { cont: Emit.F => arrayEmitter(contMap(cont, _, _)) })

  def addSetup(setup: Code[Unit]): ArrayIteratorTriplet =
    copy(calcLength = calcLength, length = length, arrayEmitter = { cont: Emit.F =>
      val et = arrayEmitter(cont)
      EmitArrayTriplet(Code(et.setup, setup), et.m, et.addElements)
    })

  def toEmitTriplet(mb: MethodBuilder, aTyp: PArray): EmitTriplet = {
    val srvb = new StagedRegionValueBuilder(mb, aTyp)

    length match {
      case Some(len) =>
        val cont = { (m: Code[Boolean], v: Code[_]) =>
          coerce[Unit](
            Code(
              m.mux(
                srvb.setMissing(),
                srvb.addIRIntermediate(aTyp.elementType)(v)),
              srvb.advance()))
        }
        val processAElts = arrayEmitter(cont)
        EmitTriplet(processAElts.setup, processAElts.m.getOrElse(const(false)), Code(
          calcLength,
          srvb.start(len, init = true),
          processAElts.addElements,
          srvb.offset
        ))

      case None =>
        val len = mb.newLocal[Int]
        val i = mb.newLocal[Int]
        val vab = new StagedArrayBuilder(aTyp.elementType, mb, 16)
        val processArrayElts = arrayEmitter { (m: Code[Boolean], v: Code[_]) => m.mux(vab.addMissing(), vab.add(v)) }
        EmitTriplet(Code(vab.clear, processArrayElts.setup), processArrayElts.m.getOrElse(const(false)), Code(
          calcLength,
          processArrayElts.addElements,
          len := vab.size,
          srvb.start(len, init = true),
          i := 0,
          Code.whileLoop(i < len,
            vab.isMissing(i).mux(
              srvb.setMissing(),
              srvb.addIRIntermediate(aTyp.elementType)(vab(i))),
            i := i + 1,
            srvb.advance()),
          srvb.offset))
    }
  }
}

abstract class MethodBuilderLike[M <: MethodBuilderLike[M]] {
  type MB <: MethodBuilder

  def mb: MB

  def newMethod(paramInfo: Array[TypeInfo[_]], returnInfo: TypeInfo[_]): M
}

class MethodBuilderSelfLike(val mb: MethodBuilder) extends MethodBuilderLike[MethodBuilderSelfLike] {
  type MB = MethodBuilder

  def newMethod(paramInfo: Array[TypeInfo[_]], returnInfo: TypeInfo[_]): MethodBuilderSelfLike =
    new MethodBuilderSelfLike(mb.fb.newMethod(paramInfo, returnInfo))
}

abstract class EstimableEmitter[M <: MethodBuilderLike[M]] {
  def emit(mb: M): Code[Unit]

  def estimatedSize: Int
}

object EmitUtils {
  private val maxBytecodeSizeTarget: Int = 4096

  def getChunkBounds(sizes: Seq[Int]): Array[Int] = {
    val ab = new ArrayBuilder[Int]()
    ab += 0
    var total = sizes.head
    sizes.zipWithIndex.tail.foreach { case (size, i) =>
      if (total + size <= maxBytecodeSizeTarget)
        total += size
      else {
        ab += i
        total = size
      }
    }
    ab += sizes.size
    ab.result()
  }

  def wrapToMethod[T, M <: MethodBuilderLike[M]](items: Seq[EstimableEmitter[M]], mbLike: M): Code[Unit] = {
    if (items.isEmpty)
      return Code._empty

    val sizes = items.map(_.estimatedSize)
    if (sizes.sum < 100)
      return coerce[Unit](Code(items.map(_.emit(mbLike)): _*))

    val chunkBounds = getChunkBounds(sizes)
    assert(chunkBounds(0) == 0 && chunkBounds.last == sizes.length)

    val chunks = chunkBounds.zip(chunkBounds.tail).map { case (start, end) =>
      assert(start < end)
      val newMBLike = mbLike.newMethod(mbLike.mb.parameterTypeInfo, typeInfo[Unit])
      val c = items.slice(start, end)
      newMBLike.mb.emit(Code(c.map(_.emit(newMBLike)): _*))
      new EstimableEmitter[M] {
        def emit(mbLike: M): Code[Unit] = {
          val args = mbLike.mb.parameterTypeInfo.zipWithIndex.map { case (ti, i) => mbLike.mb.getArg(i + 1)(ti).load() }
          coerce[Unit](newMBLike.mb.invoke(args: _*))
        }

        def estimatedSize: Int = 5
      }
    }
    wrapToMethod(chunks, mbLike)
  }
}

private class Emit(
  val ctx: ExecuteContext,
  val mb: EmitMethodBuilder,
  val nSpecialArguments: Int) {

  val resultRegion: EmitRegion = EmitRegion.default(mb)
  val region: Code[Region] = mb.getArg[Region](1)
  val methods: mutable.Map[String, Seq[(Seq[PType], PType, EmitMethodBuilder)]] = mutable.Map().withDefaultValue(FastSeq())

  import Emit.{E, F}

  class EmitMethodBuilderLike(val emit: Emit) extends MethodBuilderLike[EmitMethodBuilderLike] {
    type MB = EmitMethodBuilder

    def mb: MB = emit.mb

    def newMethod(paramInfo: Array[TypeInfo[_]], returnInfo: TypeInfo[_]): EmitMethodBuilderLike = {
      val newMB = emit.mb.fb.newMethod(paramInfo, returnInfo)
      val newEmitter = new Emit(ctx, newMB, emit.nSpecialArguments)
      new EmitMethodBuilderLike(newEmitter)
    }
  }

  private def wrapToMethod(irs: Seq[IR], env: E, container: Option[AggContainer])(useValues: (EmitMethodBuilder, PType, EmitTriplet) => Code[Unit]): Code[Unit] = {
    val opSize: Int = 20
    val items = irs.map { ir =>
      new EstimableEmitter[EmitMethodBuilderLike] {
        def estimatedSize: Int = ir.size * opSize

        def emit(mbLike: EmitMethodBuilderLike): Code[Unit] =
          useValues(mbLike.mb, ir.pType, mbLike.emit.emit(ir, env, EmitRegion.default(mbLike.emit.mb), container))
      }
    }

    EmitUtils.wrapToMethod(items, new EmitMethodBuilderLike(this))
  }

  /**
    * Invariants of the Returned Triplet
    * ----------------------------------
    *
    * The elements of the triplet are called (precompute, missingness, value)
    *
    *  1. evaluate each returned Code[_] at most once
    *  2. evaluate precompute *on all static code-paths* leading to missingness or value
    *  3. guard the the evaluation of value by missingness
    *
    * Triplets returning values cannot have side-effects.  For void triplets, precompute
    * contains the side effect, missingness is false, and value is {@code Code._empty}.
    *
    * JVM gotcha:
    * a variable must be initialized on all static code-paths prior to its use (ergo defaultValue)
    *
    * Argument Convention
    * -------------------
    *
    * {@code In(i)} occupies two argument slots, one for the value and one for a
    * missing bit. The value for {@code  In(0)} is passed as argument
    * {@code  nSpecialArguments + 1}. The missingness bit is the subsequent
    * argument. In general, the value for {@code  In(i)} appears at
    * {@code  nSpecialArguments + 1 + 2 * i}.
    *
    * There must always be at least one special argument: a {@code  Region} in
    * which the IR can allocate memory.
    *
    * When compiling an aggregation expression, {@code AggIn} refers to the first
    * argument {@code In(0)} whose type must be of type
    * {@code tAggIn.elementType}.  {@code tAggIn.symTab} is not used by Emit.
    *
    **/
  private[ir] def emit(ir: IR, env: E, er: EmitRegion, container: Option[AggContainer]): EmitTriplet = {

    def emit(ir: IR, env: E = env, er: EmitRegion = er, container: Option[AggContainer] = container): EmitTriplet =
      this.emit(ir, env, er, container)

    def wrapToMethod(irs: Seq[IR], env: E = env, container: Option[AggContainer] = container)(useValues: (EmitMethodBuilder, PType, EmitTriplet) => Code[Unit]): Code[Unit] =
      this.wrapToMethod(irs, env, container)(useValues)

    def emitArrayIterator(ir: IR, env: E = env, container: Option[AggContainer] = container) = this.emitArrayIterator(ir, env, er, container)

    def emitDeforestedNDArray(ir: NDArrayIR) =
      deforestNDArray(resultRegion, ir, env).emit(ir.pType)

    val region = er.region

    (ir: @unchecked) match {
      case I32(x) =>
        present(const(x))
      case I64(x) =>
        present(const(x))
      case F32(x) =>
        present(const(x))
      case F64(x) =>
        present(const(x))
      case Str(x) =>
        present(mb.fb.addLiteral(x, TString(), er.baseRegion))
      case Literal(t, v) =>
        if (v == null)
          emit(NA(t))
        else
          present(mb.fb.addLiteral(v, t, er.baseRegion))
      case True() =>
        present(const(true))
      case False() =>
        present(const(false))
      case Void() =>
        EmitTriplet(Code._empty, const(false), Code._empty)

      case Cast(v, typ) =>
        val codeV = emit(v)
        val cast = Casts.get(v.typ, typ)
        EmitTriplet(codeV.setup, codeV.m, cast(codeV.v))

      case CastRename(v, _typ) =>
        emit(v)

      case NA(typ) =>
        EmitTriplet(Code._empty, const(true), defaultValue(typ))
      case IsNA(v) =>
        val codeV = emit(v)
        EmitTriplet(codeV.setup, const(false), codeV.m)

      case Coalesce(values) =>
        val va = values.toArray.map(emit(_))
        val mbs = Array.fill(va.length - 1)(mb.newLocal[Boolean])

        val outType = values.head.typ
        val mout = mb.newLocal[Boolean]()
        val out = coerce[Any](mb.newLocal()(typeToTypeInfo(outType)))

        val setup = va.indices
          .init
          .foldRight(Code(
            mout := va.last.m,
            out := defaultValue(outType),
            mout.mux(Code._empty, out := va.last.v))) { case (i, comb) =>
            Code(
              mbs(i) := va(i).m,
              mbs(i).mux(
                comb,
                Code(
                  mout := false,
                  out := va(i).v)))
          }

        EmitTriplet(
          setup = Code(
            Code(va.map(_.setup): _*),
            setup),
          m = mout,
          v = out
        )

      case If(cond, cnsq, altr) =>
        assert(cnsq.typ == altr.typ)

        if (cnsq.typ == TVoid) {
          val codeCond = emit(cond)
          val codeCnsq = emit(cnsq)
          val codeAltr = emit(altr)
          EmitTriplet(
            Code(
              codeCond.setup,
              codeCond.m.mux(
                Code._empty,
                coerce[Boolean](codeCond.v).mux(
                  codeCnsq.setup,
                  codeAltr.setup))),
            const(false),
            Code._empty)
        } else {
          val typ = ir.typ
          val codeCond = emit(cond)
          val out = coerce[Any](mb.newLocal()(typeToTypeInfo(typ)))
          val mout = mb.newLocal[Boolean]()
          val codeCnsq = emit(cnsq)
          val codeAltr = emit(altr)
          val setup = Code(
            codeCond.setup,
            codeCond.m.mux(
              Code(mout := true, out := defaultValue(typ)),
              coerce[Boolean](codeCond.v).mux(
                Code(codeCnsq.setup, mout := codeCnsq.m, out := mout.mux(defaultValue(typ), codeCnsq.v)),
                Code(codeAltr.setup, mout := codeAltr.m, out := mout.mux(defaultValue(typ), codeAltr.v)))))

          EmitTriplet(setup, mout, out)
        }

      case Let(name, value, body) =>
        val typ = ir.typ
        val vti = typeToTypeInfo(value.typ)
        val mx = mb.newField[Boolean]()
        val x = coerce[Any](mb.newField(name)(vti))
        val codeV = emit(value)
        val bodyenv = env.bind(name, (vti, mx.load(), x.load()))
        val codeBody = emit(body, env = bodyenv)
        val setup = Code(
          codeV.setup,
          mx := codeV.m,
          x := mx.mux(defaultValue(value.typ), codeV.v),
          codeBody.setup)

        EmitTriplet(setup, codeBody.m, codeBody.v)
      case Ref(name, typ) =>
        val ti = typeToTypeInfo(typ)
        val (t, m, v) = env.lookup(name)
        assert(t == ti, s"$name type annotation, $typ, $t doesn't match typeinfo: $ti")
        EmitTriplet(Code._empty, m, v)

      case ApplyBinaryPrimOp(op, l, r) =>
        val typ = ir.typ
        val codeL = emit(l)
        val codeR = emit(r)
        strict(BinaryOp.emit(op, l.typ, r.typ, codeL.v, codeR.v), codeL, codeR)
      case ApplyUnaryPrimOp(op, x) =>
        val typ = ir.typ
        val v = emit(x)
        strict(UnaryOp.emit(op, x.typ, v.v), v)
      case ApplyComparisonOp(op, l, r) =>
        val f = op.codeOrdering(mb, l.pType, r.pType)
        val codeL = emit(l)
        val codeR = emit(r)
        if (op.strict) {
          strict(f((false, codeL.v), (false, codeR.v)),
            codeL, codeR)
        } else {
          val lm = mb.newLocal[Boolean]
          val rm = mb.newLocal[Boolean]
          present(Code(
            codeL.setup,
            codeR.setup,
            lm := codeL.m,
            rm := codeR.m,
            f((lm, lm.mux(defaultValue(l.typ), codeL.v)),
              (rm, rm.mux(defaultValue(r.typ), codeR.v)))))
        }

      case x@MakeArray(args, typ) =>
        val pType = x.pType.asInstanceOf[PArray]
        val srvb = new StagedRegionValueBuilder(mb, pType)
        val addElement = srvb.addIRIntermediate(pType.elementType)
        val addElts = { (newMB: EmitMethodBuilder, t: PType, v: EmitTriplet) =>
          Code(
            v.setup,
            v.m.mux(srvb.setMissing(), addElement(v.v)),
            srvb.advance())
        }
        present(Code(srvb.start(args.size, init = true), wrapToMethod(args)(addElts), srvb.offset))
      case x@ArrayRef(a, i) =>
        val typ = x.typ
        val ti = typeToTypeInfo(typ)
        val pArray = coerce[PStreamable](a.pType).asPArray
        val ati = coerce[Long](typeToTypeInfo(pArray))
        val codeA = emit(a)
        val codeI = emit(i)
        val xma = mb.newLocal[Boolean]()
        val xa = mb.newLocal()(ati)
        val xi = mb.newLocal[Int]
        val len = mb.newLocal[Int]
        val xmi = mb.newLocal[Boolean]()
        val xmv = mb.newLocal[Boolean]()
        val prettied = Pretty(x)
        val irString =
          if (prettied.size > 100) prettied.take(100) + " ..."
          else prettied
        val setup = Code(
          codeA.setup,
          xma := codeA.m,
          xa := coerce[Long](defaultValue(pArray)),
          codeI.setup,
          xmi := codeI.m,
          xi := coerce[Int](defaultValue(TInt32())),
          len := coerce[Int](defaultValue(TInt32())),
          (xmi || xma).mux(
            xmv := const(true),
            Code(
              xa := coerce[Long](codeA.v),
              xi := coerce[Int](codeI.v),
              len := pArray.loadLength(region, xa),
              (xi < len && xi >= 0).mux(
                xmv := !pArray.isElementDefined(region, xa, xi),
                Code._fatal(
                  const("array index out of bounds: ")
                    .concat(xi.load().toS)
                    .concat(" / ")
                    .concat(len.load().toS)
                    .concat(". IR: ")
                    .concat(irString))))))

        EmitTriplet(setup, xmv, Code(
          Region.loadIRIntermediate(x.pType)(pArray.elementOffset(xa, len, xi))))
      case ArrayLen(a) =>
        val codeA = emit(a)
        strict(PContainer.loadLength(coerce[Long](codeA.v)), codeA)

      case x@(_: ArraySort | _: ToSet | _: ToDict) =>
        val atyp = coerce[PIterable](x.pType)
        val eltType = -atyp.elementType
        val eltVType = eltType.virtualType
        val vab = new StagedArrayBuilder(atyp.elementType, mb, 16)
        val sorter = new ArraySorter(er, vab)

        val (array, compare, distinct) = (x: @unchecked) match {
          case ArraySort(a, l, r, comp) => (a, Subst(comp, BindingEnv(Env[IR](l -> In(0, eltType), r -> In(1, eltType)))), Code._empty[Unit])
          case ToSet(a) =>
            val discardNext = mb.fb.newMethod(Array[TypeInfo[_]](typeInfo[Region], sorter.ti, typeInfo[Boolean], sorter.ti, typeInfo[Boolean]), typeInfo[Boolean])
            val EmitTriplet(s, m, v) = new Emit(ctx, discardNext, 1).emit(ApplyComparisonOp(EQWithNA(eltVType), In(0, eltType), In(1, eltType)), Env.empty, er, container)
            discardNext.emit(Code(s, m || coerce[Boolean](v)))
            (a, ApplyComparisonOp(Compare(eltVType), In(0, eltType), In(1, eltType)) < 0, sorter.distinctFromSorted(discardNext.invoke(_, _, _, _, _)))
          case ToDict(a) =>
            val elementType = a.pType.asInstanceOf[PStreamable].elementType
            val (k0, k1, keyType) = elementType match {
              case t: PStruct => (GetField(In(0, elementType.virtualType), "key"), GetField(In(1, elementType.virtualType), "key"), t.fieldType("key"))
              case t: PTuple => (GetTupleElement(In(0, elementType.virtualType), 0), GetTupleElement(In(1, elementType.virtualType), 0), t.types(0))
            }
            val discardNext = mb.fb.newMethod(Array[TypeInfo[_]](typeInfo[Region], sorter.ti, typeInfo[Boolean], sorter.ti, typeInfo[Boolean]), typeInfo[Boolean])
            val EmitTriplet(s, m, v) = new Emit(ctx, discardNext, 1).emit(ApplyComparisonOp(EQWithNA(keyType.virtualType), k0, k1), Env.empty, er, container)
            discardNext.emit(Code(s, m || coerce[Boolean](v)))
            (a, ApplyComparisonOp(Compare(keyType.virtualType), k0, k1) < 0, Code(sorter.pruneMissing, sorter.distinctFromSorted(discardNext.invoke(_, _, _, _, _))))
        }

        val compF = vab.ti match {
          case BooleanInfo => sorter.sort(makeDependentSortingFunction[Boolean](compare, env))
          case IntInfo => sorter.sort(makeDependentSortingFunction[Int](compare, env))
          case LongInfo => sorter.sort(makeDependentSortingFunction[Long](compare, env))
          case FloatInfo => sorter.sort(makeDependentSortingFunction[Float](compare, env))
          case DoubleInfo => sorter.sort(makeDependentSortingFunction[Double](compare, env))
        }

        val aout = emitArrayIterator(array)

        val processArrayElts = aout.arrayEmitterFromBuilder(vab)
        EmitTriplet(
          Code(
            vab.clear,
            processArrayElts.setup),
          processArrayElts.m.getOrElse(const(false)),
          Code(
            aout.calcLength,
            processArrayElts.addElements,
            compF,
            distinct,
            sorter.toRegion()))

      case ToArray(a) =>
        emit(a)

      case ToStream(a) =>
        emit(a)

      case x@LowerBoundOnOrderedCollection(orderedCollection, elem, onKey) =>
        val typ: PContainer = coerce[PIterable](orderedCollection.pType).asPContainer
        val a = emit(orderedCollection)
        val e = emit(elem)
        val bs = new BinarySearch(mb, typ, keyOnly = onKey)

        val localA = mb.newLocal[Long]()
        val localElementMB = mb.newLocal[Boolean]()
        val localElementValue = mb.newLocal()(typeToTypeInfo(elem.pType))
        EmitTriplet(
          Code(a.setup, e.setup),
          a.m,
          Code(
            localA := a.value[Long],
            localElementMB := e.m,
            localElementMB.mux(localElementValue.storeAny(defaultValue(elem.pType)), localElementValue.storeAny(e.v)),
            bs.getClosestIndex(localA, localElementMB, localElementValue)))

      case GroupByKey(collection) =>
        //sort collection by group
        val atyp = coerce[PStreamable](collection.pType).asPArray
        val etyp = coerce[PBaseStruct](atyp.elementType)
        val ktyp = etyp.types(0)
        val vtyp = etyp.types(1)
        val eltOut = coerce[PDict](ir.pType).elementType

        val aout = emitArrayIterator(collection)

        val eab = new StagedArrayBuilder(etyp, mb, 16)
        val sorter = new ArraySorter(er, eab)

        val (k1, k2) = etyp match {
          case t: PStruct => GetField(In(0, t), "key") -> GetField(In(1, t), "key")
          case t: PTuple =>
            assert(t.fields(0).index == 0)
            GetTupleElement(In(0, t), 0) -> GetTupleElement(In(1, t), 0)
        }

        val compare = ApplyComparisonOp(Compare(etyp.types(0).virtualType), k1, k2) < 0
        val compF = eab.ti match {
          case BooleanInfo => sorter.sort(makeDependentSortingFunction[Boolean](compare, env))
          case IntInfo => sorter.sort(makeDependentSortingFunction[Int](compare, env))
          case LongInfo => sorter.sort(makeDependentSortingFunction[Long](compare, env))
          case FloatInfo => sorter.sort(makeDependentSortingFunction[Float](compare, env))
          case DoubleInfo => sorter.sort(makeDependentSortingFunction[Double](compare, env))
        }

        val nab = new StagedArrayBuilder(PInt32(), mb, 16)
        val i = mb.newLocal[Int]

        def loadKey(n: Code[Int]): Code[_] =
          Region.loadIRIntermediate(ktyp)(etyp.fieldOffset(coerce[Long](eab(n)), 0))

        def loadValue(n: Code[Int]): Code[_] =
          Region.loadIRIntermediate(vtyp)(etyp.fieldOffset(coerce[Long](eab(n)), 1))

        val srvb = new StagedRegionValueBuilder(mb, ir.pType)

        type E = Env[(TypeInfo[_], Code[Boolean], Code[_])]

        val (lastKey, currKey) = (etyp.virtualType: @unchecked) match {
          case ts: TStruct =>
            GetField(Ref("i-1", ts), ts.fieldNames(0)) -> GetField(Ref("i", ts), ts.fieldNames(0))
          case tt: TTuple =>
            GetTupleElement(Ref("i-1", tt), tt.fields(0).index) -> GetTupleElement(Ref("i", tt), tt.fields(0).index)
        }

        val isSame = emit(
          ApplyComparisonOp(EQWithNA(ktyp.virtualType),
            lastKey,
            currKey),
          Env(
            ("i-1", (typeInfo[Long], eab.isMissing(i-1), eab.apply(i-1))),
            ("i", (typeInfo[Long], eab.isMissing(i), eab.apply(i)))))

        val processArrayElts = aout.arrayEmitterFromBuilder(eab)
        EmitTriplet(Code(eab.clear, processArrayElts.setup), processArrayElts.m.getOrElse(const(false)), Code(
          nab.clear,
          aout.calcLength,
          processArrayElts.addElements,
          compF,
          sorter.pruneMissing,
          eab.size.ceq(0).mux(
            Code(srvb.start(0), srvb.offset),
            Code(
              i := 1,
              nab.add(1),
              Code.whileLoop(i < eab.size,
                isSame.setup,
                (isSame.m || isSame.value[Boolean]).mux(
                  nab.update(nab.size - 1, coerce[Int](nab(nab.size - 1)) + 1),
                  nab.add(1)),
                i += 1),
              i := 0,
              srvb.start(nab.size),
              Code.whileLoop(srvb.arrayIdx < nab.size,
                srvb.addBaseStruct(eltOut, { structbuilder =>
                  Code(
                    structbuilder.start(),
                    structbuilder.addIRIntermediate(ktyp)(loadKey(i)),
                    structbuilder.advance(),
                    structbuilder.addArray(coerce[PStreamable](eltOut.types(1)).asPArray, { arraybuilder =>
                      Code(
                        arraybuilder.start(coerce[Int](nab(srvb.arrayIdx))),
                        Code.whileLoop(arraybuilder.arrayIdx < coerce[Int](nab(srvb.arrayIdx)),
                          etyp.isFieldMissing(region, coerce[Long](eab(i)), 1).mux(
                            arraybuilder.setMissing(),
                            arraybuilder.addIRIntermediate(etyp.types(1))(loadValue(i))
                          ),
                          i += 1,
                          arraybuilder.advance()
                        ))
                    }))
                }),
                srvb.advance()
              ),
              srvb.offset
            ))))

      case _: ArrayMap | _: ArrayFilter | _: ArrayRange | _: ArrayFlatMap | _: ArrayScan | _: ArrayLeftJoinDistinct | _: ArrayAggScan | _: ReadPartition =>
        emitArrayIterator(ir).toEmitTriplet(mb, PArray(coerce[PStreamable](ir.pType).elementType))

      case ArrayFold(a, zero, name1, name2, body) =>
        val typ = ir.typ
        val tarray = coerce[TStreamable](a.typ)
        val tti = typeToTypeInfo(typ)
        val eti = typeToTypeInfo(tarray.elementType)
        val xmv = mb.newField[Boolean](name2 + "_missing")
        val xvv = coerce[Any](mb.newField(name2)(eti))
        val xmbody = mb.newField[Boolean](name1 + "_missing_tmp")
        val xmaccum = mb.newField[Boolean](name1 + "_missing")
        val xvaccum = coerce[Any](mb.newField(name1)(tti))
        val bodyenv = env.bind(
          (name1, (tti, xmaccum.load(), xvaccum.load())),
          (name2, (eti, xmv.load(), xvv.load())))

        val codeZ = emit(zero)
        val codeB = emit(body, env = bodyenv)

        val aBase = emitArrayIterator(a)

        val cont = { (m: Code[Boolean], v: Code[_]) =>
          Code(
            xmv := m,
            xvv := xmv.mux(defaultValue(tarray.elementType), v),
            codeB.setup,
            xmbody := codeB.m,
            xvaccum := xmbody.mux(defaultValue(typ), codeB.v),
            xmaccum := xmbody)
        }

        val processAElts = aBase.arrayEmitter(cont)
        val marray = processAElts.m.getOrElse(const(false))

        EmitTriplet(Code(
          codeZ.setup,
          xmaccum := codeZ.m,
          xvaccum := xmaccum.mux(defaultValue(typ), codeZ.v),
          processAElts.setup,
          marray.mux(
            Code(
              xmaccum := true,
              xvaccum := defaultValue(typ)),
            Code(
              aBase.calcLength,
              processAElts.addElements))),
          xmaccum, xvaccum)

      case ArrayFold2(a, acc, valueName, seq, res) =>
        val typ = ir.typ
        val tarray = coerce[TStreamable](a.typ)
        val tti = typeToTypeInfo(typ)
        val eti = typeToTypeInfo(tarray.elementType)
        val xmv = mb.newField[Boolean](valueName + "_missing")
        val xvv = coerce[Any](mb.newField(valueName)(eti))
        val accVars = acc.map { case (name, value) =>
          val ti = typeToTypeInfo(value.typ)
          (name, (ti, mb.newField[Boolean](s"${name}_missing"), mb.newField(name)(ti)))}
        val xmtmp = mb.newField[Boolean]("arrayfold2_missing_tmp")

        val resEnv = env.bindIterable(accVars.map { case (name, (ti, xm, xv)) => (name, (ti, xm.load(), xv.load())) })
        val seqEnv = resEnv.bind(valueName, (eti, xmv.load(), xvv.load()))

        val codeZ = acc.map { case (_, value) => emit(value) }
        val codeSeq = seq.map(emit(_, env = seqEnv))

        val aBase = emitArrayIterator(a)

        val cont = { (m: Code[Boolean], v: Code[_]) =>
          Code(
            xmv := m,
            xvv := xmv.mux(defaultValue(tarray.elementType), v),
            Code(codeSeq.map(_.setup): _*),
            coerce[Unit](Code(codeSeq.zipWithIndex.map { case (et, i) =>
              val (_, (_, accm, accv)) = accVars(i)
              Code(
                xmtmp := et.m,
                accv.storeAny(xmtmp.mux(defaultValue(acc(i)._2.typ): Code[_], et.v)),
                accm := xmtmp
              )
            }: _*)))
        }

        val processAElts = aBase.arrayEmitter(cont)
        val marray = processAElts.m.getOrElse(const(false))

        val xresm = mb.newField[Boolean]
        val xresv = mb.newField(typeToTypeInfo(res.typ))
        val codeR = emit(res, env = resEnv)

        EmitTriplet(Code(
          codeZ.map(_.setup),
          accVars.zipWithIndex.map { case ((_, (ti, xm, xv)), i) =>
            Code(xm := codeZ(i).m, xv.storeAny(xm.mux(defaultValue(acc(i)._2.typ), codeZ(i).v)))
          },
          processAElts.setup,
          marray.mux(
            Code(
              xresm := true,
              xresv.storeAny(defaultValue(res.typ))),
            Code(
              aBase.calcLength,
              processAElts.addElements,
              codeR.setup,
              xresm := codeR.m,
              xresv.storeAny(codeR.v)))),
          xresm, xresv)

      case ArrayFor(a, valueName, body) =>
        val tarray = coerce[TStreamable](a.typ)
        val eti = typeToTypeInfo(tarray.elementType)
        val xmv = mb.newField[Boolean]()
        val xvv = coerce[Any](mb.newField(valueName)(eti))
        val bodyenv = env.bind(
          (valueName, (eti, xmv.load(), xvv.load())))
        val codeB = emit(body, env = bodyenv)
        val aBase = emitArrayIterator(a)
        val cont = { (m: Code[Boolean], v: Code[_]) =>
          Code(
            xmv := m,
            xvv := xmv.mux(defaultValue(tarray.elementType), v),
            codeB.setup)
        }

        val processAElts = aBase.arrayEmitter(cont)
        val ma = processAElts.m.getOrElse(const(false))
        EmitTriplet(
          Code(
            processAElts.setup,
            ma.mux(
              Code._empty,
              Code(aBase.calcLength, processAElts.addElements))),
          const(false),
          Code._empty)

      case ArrayAgg(a, name, query) =>
        assert(!ContainsAggIntermediate(query))
        val tarray = coerce[TStreamable](a.typ)
        val eti = typeToTypeInfo(tarray.elementType)
        val xmv = mb.newField[Boolean]()
        val xvv = coerce[Any](mb.newField(name)(eti))
        val perEltEnv = env.bind(
          (name, (eti, xmv.load(), xvv.load())))

        val res = genUID()
        val extracted = agg.Extract(query, res)

        val (newContainer, aggSetup, aggCleanup) = AggContainer.fromFunctionBuilder(extracted.aggs, mb.fb, "array_agg")

        val init = Optimize(extracted.init, noisy = true,
          context = "ArrayAgg/agg.Extract/init", ctx)
        val perElt = Optimize(extracted.seqPerElt, noisy = true,
          context = "ArrayAgg/agg.Extract/perElt", ctx)
        val postAggIR = Optimize[IR](Let(res, extracted.results, extracted.postAggIR), noisy = true,
          context = "ArrayAgg/agg.Extract/postAggIR", ctx)

        val codeInit = emit(init, env = env, container = Some(newContainer))
        val codePerElt = emit(perElt, env = perEltEnv, container = Some(newContainer))
        val postAgg = emit(postAggIR, env = env, container = Some(newContainer))

        val resm = mb.newField[Boolean]()
        val resv = mb.newField(name)(typeToTypeInfo(query.pType))

        val aBase = emitArrayIterator(a)
        val cont = { (m: Code[Boolean], v: Code[_]) =>
          Code(xmv := m,
            xvv := xmv.mux(defaultValue(tarray.elementType), v),
            codePerElt.setup)
        }
        val processAElts = aBase.arrayEmitter(cont)
        val ma = processAElts.m.getOrElse(const(false))

        val aggregation = Code(
          aggSetup,
          codeInit.setup,
          processAElts.setup,
          ma.mux(
            Code._empty,
            Code(aBase.calcLength, processAElts.addElements)),
          postAgg.setup,
          resm := postAgg.m,
          resv.storeAny(resm.mux(defaultValue(query.pType), postAgg.value)),
          aggCleanup)

        EmitTriplet(aggregation, resm, resv)

      case InitOp2(i, args, aggSig) =>
        val AggContainer(aggs, sc) = container.get
        assert(agg.Extract.compatible(aggs(i), aggSig))
        val rvAgg = agg.Extract.getAgg(aggSig)

        val argVars = args.map(a => emit(a, container = container.flatMap(_.nested(i, init = true)))).toArray
        void(
          sc.newState(i),
          rvAgg.initOp(sc.states(i), argVars))

      case SeqOp2(i, args, aggSig) =>
        val AggContainer(aggs, sc) = container.get
        assert(agg.Extract.compatible(aggs(i), aggSig), s"${ aggs(i) } vs $aggSig")
        val rvAgg = agg.Extract.getAgg(aggSig)

        val argVars = args.map(a => emit(a, container = container.flatMap(_.nested(i, init = false)))).toArray
        void(rvAgg.seqOp(sc.states(i), argVars))

      case CombOp2(i1, i2, aggSig) =>
        val AggContainer(aggs, sc) = container.get
        assert(agg.Extract.compatible(aggs(i1), aggSig), s"${ aggs(i1) } vs $aggSig")
        assert(agg.Extract.compatible(aggs(i2), aggSig), s"${ aggs(i2) } vs $aggSig")
        val rvAgg = agg.Extract.getAgg(aggSig)

        void(rvAgg.combOp(sc.states(i1), sc.states(i2)))

      case x@ResultOp2(start, aggSigs) =>
        val newRegion = mb.newField[Region]
        val AggContainer(aggs, sc) = container.get
        val srvb = new StagedRegionValueBuilder(EmitRegion(mb, newRegion), x.pType)
        val addFields = mb.fb.wrapVoids(Array.tabulate(aggSigs.length) { j =>
          val idx = start + j
          assert(aggSigs(j) == aggs(idx))
          val rvAgg = agg.Extract.getAgg(aggSigs(j))
          Code(
            rvAgg.result(sc.states(idx), srvb),
            srvb.advance())
        }, "aggs_result")

        present(Code(
          newRegion := region,
          srvb.start(),
          addFields,
          sc.store,
          srvb.offset))

      case SerializeAggs(start, sIdx, spec, aggSigs) =>
        val AggContainer(aggs, sc) = container.get
        val ob = mb.newField[OutputBuffer]
        val baos = mb.newField[ByteArrayOutputStream]

        val serialize = Array.range(start, start + aggSigs.length)
          .map { idx => sc.states(idx).serialize(spec)(ob) }

        void(
          baos := Code.newInstance[ByteArrayOutputStream](),
          ob := spec.buildCodeOutputBuffer(baos),
          mb.fb.wrapVoids(serialize, "serialize_aggs"),
          ob.invoke[Unit]("flush"),
          ob.invoke[Unit]("close"),
          mb.fb.setSerializedAgg(sIdx, baos.invoke[Array[Byte]]("toByteArray")),
          sc.store)

      case DeserializeAggs(start, sIdx, spec, aggSigs) =>
        val AggContainer(aggs, sc) = container.get
        val ib = mb.newField[InputBuffer]
        val bais = mb.newField[ByteArrayInputStream]

        val deserializers = sc.states.states
          .slice(start, start + aggSigs.length)
          .map(sc => sc.deserialize(BufferSpec.defaultUncompressed))

        val init = coerce[Unit](Code(Array.range(start, start + aggSigs.length)
          .map(i => sc.newState(i)): _*))

        val unserialize = Array.tabulate(aggSigs.length) { j =>
          deserializers(j)(ib)
        }

        void(
          init,
          ib := spec.buildCodeInputBuffer(
            Code.newInstance[ByteArrayInputStream, Array[Byte]](
              mb.fb.getSerializedAgg(sIdx))),
          mb.fb.wrapVoids(unserialize, "deserialize_aggs"))

      case Begin(xs) =>
        EmitTriplet(
          wrapToMethod(xs) { case (_, t, code) =>
            code.setup
          },
          const(false),
          Code._empty)

      case x@MakeStruct(fields) =>
        val srvb = new StagedRegionValueBuilder(mb, x.pType)
        val addFields = { (newMB: EmitMethodBuilder, t: PType, v: EmitTriplet) =>
          Code(
            v.setup,
            v.m.mux(srvb.setMissing(), srvb.addIRIntermediate(t)(v.v)),
            srvb.advance())
        }
        present(Code(srvb.start(init = true), wrapToMethod(fields.map(_._2))(addFields), srvb.offset))

      case x@SelectFields(oldStruct, fields) =>
        val old = emit(oldStruct)
        val oldt = coerce[PStruct](oldStruct.pType)
        val oldv = mb.newField[Long]
        val srvb = new StagedRegionValueBuilder(mb, x.pType)

        val addFields = fields.map { name =>
          new EstimableEmitter[EmitMethodBuilderLike] {
            def estimatedSize: Int = 20

            def emit(mbLike: EmitMethodBuilderLike): Code[Unit] = {
              val i = oldt.fieldIdx(name)
              val t = oldt.types(i)
              val fieldMissing = oldt.isFieldMissing(region, oldv, i)
              val fieldValue = Region.loadIRIntermediate(t)(oldt.fieldOffset(oldv, i))
              Code(
                fieldMissing.mux(
                  srvb.setMissing(),
                  srvb.addIRIntermediate(t)(fieldValue)),
                srvb.advance())
            }
          }
        }

        EmitTriplet(
          old.setup,
          old.m,
          Code(
            oldv := old.value[Long],
            srvb.start(),
            EmitUtils.wrapToMethod(addFields, new EmitMethodBuilderLike(this)),
            srvb.offset))


      case x@InsertFields(old, fields, fieldOrder) =>
        if (fields.isEmpty)
          emit(old)
        else
          old.pType match {
            case oldtype: PStruct =>
              val codeOld = emit(old)
              val xo = mb.newField[Long]
              val updateMap = Map(fields: _*)
              val srvb = new StagedRegionValueBuilder(mb, x.pType)

              val addFields = { (newMB: EmitMethodBuilder, t: PType, v: EmitTriplet) =>
                Code(
                  v.setup,
                  v.m.mux(srvb.setMissing(), srvb.addIRIntermediate(t)(v.v)),
                  srvb.advance())
              }

              val opSize: Int = 20
              val items = x.pType.fields.map { f =>
                updateMap.get(f.name) match {
                  case Some(vir) =>
                    new EstimableEmitter[EmitMethodBuilderLike] {
                      def estimatedSize: Int = vir.size * opSize

                      def emit(mbLike: EmitMethodBuilderLike): Code[Unit] =
                        addFields(mbLike.mb, vir.pType, mbLike.emit.emit(vir, env, EmitRegion.default(mbLike.mb), container))
                    }
                  case None =>
                    val oldField = oldtype.field(f.name)
                    new EstimableEmitter[EmitMethodBuilderLike] {
                      def estimatedSize: Int = 20

                      def emit(mbLike: EmitMethodBuilderLike): Code[Unit] =
                        Code(
                          oldtype.isFieldMissing(region, xo, oldField.index).mux(
                            srvb.setMissing(),
                            srvb.addIRIntermediate(f.typ)(Region.loadIRIntermediate(oldField.typ)(oldtype.fieldOffset(xo, oldField.index)))),
                          srvb.advance())
                    }
                }
              }

              EmitTriplet(
                codeOld.setup,
                codeOld.m,
                Code(
                  srvb.start(init = true),
                  xo := coerce[Long](codeOld.v),
                  EmitUtils.wrapToMethod(items, new EmitMethodBuilderLike(this)),
                  srvb.offset))
            case _ =>
              val newIR = MakeStruct(fields)
              emit(newIR)
          }

      case GetField(o, name) =>
        val t = coerce[PStruct](o.pType)
        val fieldIdx = t.fieldIdx(name)
        val codeO = emit(o)
        val xmo = mb.newLocal[Boolean]()
        val xo = mb.newLocal[Long]
        val setup = Code(
          codeO.setup,
          xmo := codeO.m,
          xo := coerce[Long](xmo.mux(defaultValue(t), codeO.v)))
        EmitTriplet(setup,
          xmo || !t.isFieldDefined(region, xo, fieldIdx),
          Region.loadIRIntermediate(t.types(fieldIdx))(t.fieldOffset(xo, fieldIdx)))

      case x@MakeTuple(fields) =>
        val srvb = new StagedRegionValueBuilder(mb, x.pType)
        val addFields = { (newMB: EmitMethodBuilder, t: PType, v: EmitTriplet) =>
          Code(
            v.setup,
            v.m.mux(srvb.setMissing(), srvb.addIRIntermediate(t)(v.v)),
            srvb.advance())
        }
        present(Code(srvb.start(init = true), wrapToMethod(fields.map(_._2))(addFields), srvb.offset))

      case GetTupleElement(o, i) =>
        val t = coerce[PTuple](o.pType)
        val idx = t.fieldIndex(i)
        val codeO = emit(o)
        val xmo = mb.newLocal[Boolean]()
        val xo = mb.newLocal[Long]
        val setup = Code(
          codeO.setup,
          xmo := codeO.m,
          xo := coerce[Long](xmo.mux(defaultValue(t), codeO.v)))
        EmitTriplet(setup,
          xmo || !t.isFieldDefined(region, xo, idx),
          Region.loadIRIntermediate(t.types(idx))(t.fieldOffset(xo, idx)))

      case In(i, typ) =>
        EmitTriplet(Code._empty,
          mb.getArg[Boolean](normalArgumentPosition(i) + 1),
          mb.getArg(normalArgumentPosition(i))(typeToTypeInfo(typ)))
      case Die(m, typ) =>
        val cm = emit(m)
        EmitTriplet(
          Code(
            cm.setup,
            Code._throw(Code.newInstance[HailException, String](
              cm.m.mux[String](
                "<exception message missing>",
                coerce[String](StringFunctions.wrapArg(er, m.pType)(cm.v)))))),
          false,
          defaultValue(typ))
      case ir@ApplyIR(fn, args) =>
        assert(!ir.inline)
        val mfield = mb.newField[Boolean]
        val vfield = mb.newField()(typeToTypeInfo(ir.typ))

        val addFields = { (newMB: EmitMethodBuilder, t: PType, v: EmitTriplet) =>
          Code(
            v.setup,
            mfield := v.m,
            mfield.mux(
              vfield.storeAny(defaultValue(t)),
              vfield.storeAny(v.v)))
        }

        EmitTriplet(
          wrapToMethod(FastSeq(ir.explicitNode))(addFields),
          mfield, vfield)

      case ir@Apply(fn, args, rt) =>
        val impl = ir.implementation
        val unified = impl.unify(args.map(_.typ) :+ rt)
        assert(unified)

        val argPTypes = args.map(_.pType)
        val meth =
          methods(fn).filter { case (argt, rtExpected, _) =>
            argt.zip(argPTypes).forall { case (t1, t2) => t1 == t2 } &&
              rtExpected == ir.pType
          } match {
            case Seq((_, _, funcMB)) =>
              funcMB
            case Seq() =>
              val methodbuilder = impl.getAsMethod(mb.fb, rt, argPTypes: _*)
              methods.update(fn, methods(fn) :+ ((argPTypes, ir.pType, methodbuilder)))
              methodbuilder
          }
        val codeArgs = args.map(emit(_))
        val vars = args.map { a => coerce[Any](mb.newLocal()(typeToTypeInfo(a.typ))) }
        val ins = vars.zip(codeArgs.map(_.v)).map { case (l, i) => l := i }
        val value = Code(ins :+ meth.invoke(mb.getArg[Region](1).load() +: vars.map { a => a.load() }: _*): _*)
        strict(value, codeArgs: _*)
      case x@ApplySeeded(fn, args, seed, rt) =>
        val codeArgs = args.map(a => (a.pType, emit(a)))
        val impl = x.implementation
        val unified = impl.unify(args.map(_.typ) :+ rt)
        assert(unified)
        impl.setSeed(seed)
        impl.apply(er, codeArgs: _*)
      case x@ApplySpecial(_, args, rt) =>
        val codeArgs = args.map(a => (a.pType, emit(a)))
        val impl = x.implementation
        impl.argTypes.foreach(_.clear())
        val unified = impl.unify(args.map(_.typ) :+ rt)
        assert(unified)
        impl.apply(er, codeArgs: _*)
      case x@Uniroot(argname, fn, min, max) =>
        val missingError = s"result of function missing in call to uniroot; must be defined along entire interval"
        val asmfunction = getAsDependentFunction[Double, Double](fn, Env[IR](argname -> In(0, PFloat64())), env, missingError)

        val localF = mb.newField[AsmFunction3[Region, Double, Boolean, Double]]
        val codeMin = emit(min)
        val codeMax = emit(max)

        val res = mb.newLocal[java.lang.Double]

        val setup = Code(codeMin.setup, codeMax.setup)
        val m = (codeMin.m || codeMax.m).mux(
          Code(
            localF := Code._null,
            res := Code._null,
            const(true)),
          Code(
            localF := asmfunction.newInstance(),
            res := Code.invokeScalaObject[Region, AsmFunction3[Region, Double, Boolean, Double], Double, Double, java.lang.Double](
              MathFunctions.getClass,
              "iruniroot", region, localF, codeMin.value[Double], codeMax.value[Double]),
            res.isNull))

        EmitTriplet(setup, m, res.invoke[Double]("doubleValue"))
      case x@MakeNDArray(dataIR, shapeIR, rowMajorIR) =>
        val xP = x.pType
        val dataContainer = dataIR.pType
        val shapePType = coerce[PTuple](shapeIR.pType)
        val dataPType = xP.data.pType
        val nDims = shapePType.size

        val datat = emit(dataIR)
        val shapet = emit(shapeIR)
        val rowMajort = emit(rowMajorIR)

        val requiredData = dataPType.checkedConvertFrom(mb, region, datat.value[Long], coerce[PArray](dataContainer), "NDArray cannot have missing data")
        val shapeAddress = mb.newField[Long]

        val shapeTuple = new CodePTuple(shapePType, region, shapeAddress)

        val shapeVariables = (0 until nDims).map(_ => mb.newLocal[Long]).toArray

        def shapeBuilder(srvb: StagedRegionValueBuilder): Code[Unit] = {
          Code(
            srvb.start(),
            Code.foreach(0 until nDims) { index =>
              Code(
                srvb.addLong(shapeVariables(index)),
                srvb.advance()
              )
            }
          )
        }

        val setup = Code(
          shapet.setup,
          datat.setup,
          rowMajort.setup
        )
        val result = Code(
          shapeAddress := shapet.value[Long],
          Code.foreach(0 until nDims) {index =>
            shapeTuple.isMissing(index).mux[Unit](
              Code._fatal(s"shape missing at index $index"),
              shapeVariables(index) := shapeTuple(index)
            )
          },
          xP.construct(0, 0, shapeBuilder, xP.makeDefaultStridesBuilder(shapeVariables.map(_.load()), mb), requiredData, mb)
        )
        EmitTriplet(setup, datat.m || shapet.m, result)
      case NDArrayShape(ndIR) =>
        val ndt = emit(ndIR)
        val ndP = ndIR.pType.asInstanceOf[PNDArray]

        EmitTriplet(ndt.setup, ndt.m, ndP.shape.load(region, ndt.value[Long]))
      case NDArrayRef(nd, idxs) =>
        val ndt = emit(nd)
        val idxst = idxs.map(emit(_))
        val childPType = coerce[PNDArray](nd.pType)
        val ndAddress = mb.newField[Long]
        val overallMissing = mb.newField[Boolean]

        val idxFields = idxst.map(_ => mb.newField[Long])
        val idxFieldsBinding = Code(
          idxFields.zip(idxst).map{ case (field, idxTriplet) =>
            field := idxTriplet.value[Long]
          }
        )
        val cachedIdxVals = idxFields.map(_.load()).toArray

        val targetElementPosition = childPType.getElementAddress(cachedIdxVals, ndAddress, region, mb)

        val setup = coerce[Unit](Code(
          ndt.setup,
          overallMissing := ndt.m,
          Code(idxst.map(_.setup)),
          Code.foreach(idxst.map(_.m)){ idxMissingness =>
            overallMissing := overallMissing || idxMissingness
          }
        ))

        val value = Code(
          ndAddress := ndt.value[Long],
          idxFieldsBinding,
          childPType.outOfBounds(cachedIdxVals, ndAddress, region, mb).orEmpty(Code._fatal("Index out of bounds")),
          Region.loadIRIntermediate(childPType.data.pType.elementType)(targetElementPosition)
        )

        EmitTriplet(setup, overallMissing, value)
      case x@NDArrayReindex(child, indexMap) =>
        val childt = emit(child)
        val childAddress = mb.newField[Long]
        val childPType = coerce[PNDArray](child.pType)

        val childShape = new CodePTuple(childPType.shape.pType, region, childPType.shape.load(region, childAddress))
        val childStrides = new CodePTuple(childPType.strides.pType, region, childPType.strides.load(region, childAddress))

        val setup = childt.setup
        val value = Code(
          childAddress := childt.value[Long],
          x.pType.construct(
            childPType.flags.load(region, childAddress),
            childPType.offset.load(region, childAddress),
            { srvb =>
              Code(
                srvb.start(),
                Code.foreach(indexMap) {childIndex =>
                  Code(
                    srvb.addLong(if (childIndex < childPType.nDims) childShape(childIndex) else 1L),
                    srvb.advance()
                  )
                }
              )
            },
            { srvb =>
              Code(
                srvb.start(),
                Code.foreach(indexMap) {index =>
                  Code(
                    srvb.addLong(if (index < childPType.nDims) childStrides(index) else 0L),
                    srvb.advance()
                  )
                }
              )
            },
            childPType.data.load(region, childAddress),
            mb
          )
        )
        EmitTriplet(setup, childt.m, value)
      case x: NDArrayMap  =>  emitDeforestedNDArray(x)
      case x: NDArrayMap2 =>  emitDeforestedNDArray(x)
      case x: NDArrayReshape => emitDeforestedNDArray(x)
      case x: NDArraySlice => emitDeforestedNDArray(x)

      case NDArrayMatMul(lChild, rChild) =>
        val lT = emit(lChild)
        val rT = emit(rChild)

        val lPType = coerce[PNDArray](lChild.pType)
        val rPType = coerce[PNDArray](rChild.pType)

        val leftND = mb.newField[Long]
        val rightND = mb.newField[Long]

        val leftShape = lPType.shape.load(region, leftND)
        val rightShape = rPType.shape.load(region, rightND)

        val lShapeTuple = new CodePTuple(lPType.shape.pType, region, leftShape)
        val rShapeTuple = new CodePTuple(rPType.shape.pType, region, rightShape)

        val (leftShapeArraySetup, leftShapeArray) = (0 until lPType.nDims).map(i => coerce[Long](lShapeTuple(i))).toArray.cacheEntries(mb, LongInfo)
        val (rightShapeArraySetup, rightShapeArray) = (0 until rPType.nDims).map(i => coerce[Long](rShapeTuple(i))).toArray.cacheEntries(mb, LongInfo)

        val (unifyShapeSetup, unifiedShapeArray) = NDArrayEmitter.matmulShape(leftShapeArray, rightShapeArray)

        val leftBroadcastMask = if (lPType.nDims > 2) NDArrayEmitter.broadcastMask(leftShapeArray) else Array[Code[Long]]()
        val rightBroadcastMask = if (rPType.nDims > 2) NDArrayEmitter.broadcastMask(rightShapeArray) else Array[Code[Long]]()

        val missingSetup = Code(
          lT.setup,
          rT.setup
        )

        val shapeSetup = Code(
          leftND := lT.value[Long],
          rightND := rT.value[Long],
          leftShapeArraySetup,
          rightShapeArraySetup,
          unifyShapeSetup
        )

        val outputPType = PNDArray(lPType.elementType, TNDArray.matMulNDims(lPType.nDims, rPType.nDims), true)

        val numericElementType = coerce[PNumeric](lPType.elementType)

        val eVti = typeToTypeInfo(numericElementType.virtualType)

        val emitter = new NDArrayEmitter(mb, outputPType.nDims, unifiedShapeArray, lPType.shape.pType, lPType.elementType, shapeSetup, missingSetup, lT.m || rT.m) {
          override def outputElement(idxVars: Array[Code[Long]]): Code[_] = {
            val element = coerce[Any](mb.newField("matmul_element")(eVti))
            val k = mb.newField[Long]

            val (lIndices: Array[Code[Long]], rIndices: Array[Code[Long]]) = (lPType.nDims, rPType.nDims, idxVars.toSeq) match {
              case (1, 1, Seq()) => (Array[Code[Long]](k), Array[Code[Long]](k))
              case (1, _, stack :+ m) =>
                val rStackVars = NDArrayEmitter.zeroBroadcastedDims(stack.toArray, rightBroadcastMask)
                (Array(k.load()), rStackVars :+ k.load() :+ m)
              case (_, 1, stack :+ n) =>
                val lStackVars = NDArrayEmitter.zeroBroadcastedDims(stack.toArray, leftBroadcastMask)
                (lStackVars :+ n :+ k.load(), Array(k.load()))
              case (_, _, stack :+ n :+ m) => {
                val lStackVars = NDArrayEmitter.zeroBroadcastedDims(stack.toArray, leftBroadcastMask)
                val rStackVars = NDArrayEmitter.zeroBroadcastedDims(stack.toArray, rightBroadcastMask)
                (lStackVars :+ n :+ k.load(), rStackVars :+ k.load() :+  m)
              }
            }

            val lElem = lPType.loadElementToIRIntermediate(lIndices, leftND, region, mb)
            val rElem = rPType.loadElementToIRIntermediate(rIndices, rightND, region, mb)
            val kLen = mb.newField[Long]

            val innerMethod = mb.fb.newMethod(eVti)

            val loopCode = Code(
              k := 0L,
              kLen := leftShapeArray(lPType.nDims - 1),
              element := numericElementType.zero,
              Code.whileLoop(k < kLen,
                  element := numericElementType.add(numericElementType.multiply(lElem, rElem), element),
                  k := k + 1L
              ),
              element
            )
            innerMethod.emit(loopCode)
            innerMethod.invoke()
          }
        }
        emitter.emit(outputPType)

      case x@NDArrayQR(nd, mode) =>
        val ndt = emit(nd)

        val ndAddress = mb.newField[Long]

        val ndPType = nd.pType.asInstanceOf[PNDArray]

        assert(ndPType.elementType.isInstanceOf[PFloat64])

        val shapeAddress = ndPType.shape.load(region, ndAddress)
        val shapeTuple = new CodePTuple(ndPType.shape.pType, region, shapeAddress)
        val shapeArray = (0 until ndPType.shape.pType.nFields).map(shapeTuple[Long](_)).toArray

        // I want a way to lay out a Scala tuple in hail managed memory on the fly
        val M = shapeArray(0)
        val N = shapeArray(1)
        val K = (M < N).mux(M, N)
        val LDA = M
        val LWORK = 100000

        val dataAddress = ndPType.data.load(region, ndAddress)

        //Plan:
        // First round,let's just give a lot of work space. Later we can figure out how to do better.
        // Have to copy the data into the space where the answer should go, since QR just overrwrites its input
        // Have to allocate a buffer of workspace.

        val tauPType = PArray(PFloat64Required, true)
        val tauAddress = mb.newField[Long]
        val workAddress = mb.newField[Long]
        val answerAddress = mb.newField[Long]
        val answerNumElements = ndPType.numElements(shapeArray, mb)

        val infoResult = mb.newLocal[Int]

        val alwaysNeeded = Code(
          ndAddress := ndt.value[Long],

          Code._println("answerNumElements.toI"),
          Code.getStatic[java.lang.System, java.io.PrintStream]("out").invoke[Int, Unit](
            "println", answerNumElements.toI),

          // Make some space for the output (which means copying the input)
          answerAddress := ndPType.data.pType.allocate(region, answerNumElements.toI),
          ndPType.data.pType.stagedInitialize(answerAddress, answerNumElements.toI),
          ndPType.copyRowMajorToColumnMajor(ndPType.data.pType.elementOffset(dataAddress, answerNumElements.toI, 0), ndPType.data.pType.elementOffset(answerAddress, answerNumElements.toI, 0), M, N, mb),
          //Region.copyFrom(ndPType.data.pType.elementOffset(dataAddress, answerNumElements.toI, 0),
          //  ndPType.data.pType.elementOffset(answerAddress, answerNumElements.toI, 0), answerNumElements * ndPType.elementType.byteSize),

          // Make some space for Tau
          tauAddress := tauPType.allocate(region, K.toI),
          tauPType.stagedInitialize(tauAddress, K.toI),
          Code._println("tau length before calling LAPACK"),
          Code.getStatic[java.lang.System, java.io.PrintStream]("out").invoke[Int, Unit](
            "println", tauPType.loadLength(tauAddress)),

          // Make some space for work
          workAddress := Code.invokeStatic[Memory, Long, Long]("malloc", LWORK.toLong),

          infoResult := Code.invokeScalaObject[Int, Int, Long, Int, Long, Long, Int, Int](LAPACKLibrary.getClass, "dgeqrf",
            M.toI,
            N.toI,
            ndPType.data.pType.elementOffset(answerAddress, answerNumElements.toI, 0),
            LDA.toI,
            tauPType.elementOffset(tauAddress, K.toI, 0),
            workAddress,
            LWORK
          ),
          Code._println("Info after LAPACK invocation"),
          Code.getStatic[java.lang.System, java.io.PrintStream]("out").invoke[Int, Unit](
            "println", infoResult),
          Code._println("Initial data length"),
          Code.getStatic[java.lang.System, java.io.PrintStream]("out").invoke[Int, Unit](
            "println", ndPType.data.pType.loadLength(dataAddress)),
          Code._println("Answer data length"),
          Code.getStatic[java.lang.System, java.io.PrintStream]("out").invoke[Int, Unit](
            "println", ndPType.data.pType.loadLength(answerAddress)),
          Code._println("tau length"),
          Code.getStatic[java.lang.System, java.io.PrintStream]("out").invoke[Int, Unit](
            "println", tauPType.loadLength(tauAddress))



          //Code.invokeStatic[Memory, Long, Unit]("free", LWORK.toLong)
        )

        if (mode == "raw") {
          val rawPType = x.pType.asInstanceOf[PTuple]
          val rawOutputSrvb = new StagedRegionValueBuilder(mb, x.pType, region)
          val hPType = rawPType.types(0).asInstanceOf[PNDArray]
          val tauPType = rawPType.types(1).asInstanceOf[PNDArray]
          val hShapeBuilder = hPType.makeShapeBuilder(Array(M, N))
          val hStridesBuilder = hPType.makeDefaultStridesBuilder(Array(M, N), mb)

          def tauShapeBuilder(srvb: StagedRegionValueBuilder): Code[Unit] = {
            Code(
              srvb.start(),
              srvb.addLong(K),
              srvb.advance()
            )
          }

          val tauStridesBuilder = tauPType.makeDefaultStridesBuilder(Array(K), mb)

          val h = hPType.construct(0, 0, hShapeBuilder, hStridesBuilder, answerAddress, mb)
          val tau = tauPType.construct(0, 0, tauShapeBuilder, tauStridesBuilder, tauAddress, mb)

          val ifRaw = Code(
            rawOutputSrvb.start(),
            rawOutputSrvb.addIRIntermediate(hPType)(h),
            rawOutputSrvb.advance(),
            rawOutputSrvb.addIRIntermediate(tauPType)(tau),
            rawOutputSrvb.advance(),
            rawOutputSrvb.end()
          )


          val result = Code(
            alwaysNeeded,
            ifRaw
          )

          EmitTriplet(ndt.setup, ndt.m, result)
        }
        else {
          if (mode == "r") {
            // In R mode, the upper right hand corner of A contains what I want. I should just zero out the bottom corner and call it a day.
            val rPType = x.pType.asInstanceOf[PNDArray]
            val rShapeBuilder = rPType.makeShapeBuilder(Array(M, N))
            val rStridesBuilder = rPType.makeDefaultStridesBuilder(Array(M, N), mb)

            val currRow = mb.newField[Int]
            val currCol = mb.newField[Int]
            val zeroOutCorner = Code(
              currRow := 0,
              Code.whileLoop(currRow < M.toI,
                currCol := 0,
                Code.whileLoop(currCol < N.toI,
                  (currRow > currCol).orEmpty(
                    // TODO Not row / column major agnostic!
                    Region.storeDouble(ndPType.data.pType.elementOffset(answerAddress, answerNumElements.toI,
                      currRow * N.toI + currCol), 0.0)
                  ),
                  currCol := currCol + 1
                ),
                currRow := currRow + 1
              )
            )

            // Now I have the code to zero out a corner. I should run that code, then return only A.
            val result = Code(
              alwaysNeeded,
              zeroOutCorner,
              rPType.construct(0, 0, rShapeBuilder, rStridesBuilder, answerAddress, mb)
            )

            EmitTriplet(ndt.setup, ndt.m, result)
          }
          else {
            throw new IllegalArgumentException("Only support raw and R")
          }
        }


      case x@CollectDistributedArray(contexts, globals, cname, gname, body) =>
        val ctxType = coerce[PArray](contexts.pType).elementType
        val gType = globals.pType
        val bType = body.pType

        val ctxTypeTuple = PTuple(ctxType)
        val gTypeTuple = PTuple(gType)
        val bTypeTuple = PTuple(bType)

        val spec = BufferSpec.defaultUncompressed
        val parentFB = mb.fb

        val cCodec = TypedCodecSpec(ctxTypeTuple, spec)
        val gCodec = TypedCodecSpec(gTypeTuple, spec)
        val bCodec = TypedCodecSpec(bTypeTuple, spec)

        val functionID: String = {
          val bodyFB = EmitFunctionBuilder[Region, Array[Byte], Array[Byte], Array[Byte]]("collect_distributed_array")
          val bodyMB = bodyFB.newMethod(Array[TypeInfo[_]](typeInfo[Region], typeToTypeInfo(ctxType), typeInfo[Boolean], typeToTypeInfo(gType), typeInfo[Boolean]), typeInfo[Long])

          val (cRetPtype, cDec) = cCodec.buildEmitDecoderF[Long](ctxTypeTuple.virtualType, bodyFB)
          val (gRetPtype, gDec) = gCodec.buildEmitDecoderF[Long](gTypeTuple.virtualType, bodyFB)
          val bEnc = bCodec.buildEmitEncoderF[Long](bTypeTuple, bodyFB)
          val bOB = bodyFB.newField[OutputBuffer]

          assert(cRetPtype == ctxTypeTuple)
          assert(gRetPtype == gTypeTuple)

          val env = Env[(TypeInfo[_], Code[Boolean], Code[_])](
            (cname, (typeToTypeInfo(ctxType), bodyMB.getArg[Boolean](3).load(), bodyMB.getArg(2)(typeToTypeInfo(ctxType)).load())),
            (gname, (typeToTypeInfo(gType), bodyMB.getArg[Boolean](5).load(), bodyMB.getArg(4)(typeToTypeInfo(gType)).load())))

          // FIXME fix number of aggs here
          val t = new Emit(ctx, bodyMB, 1).emit(MakeTuple.ordered(FastSeq(body)), env, EmitRegion.default(bodyMB), None)
          bodyMB.emit(Code(t.setup, t.m.mux(Code._fatal("return cannot be missing"), t.v)))

          val ctxIS = Code.newInstance[ByteArrayInputStream, Array[Byte]](bodyFB.getArg[Array[Byte]](2))
          val gIS = Code.newInstance[ByteArrayInputStream, Array[Byte]](bodyFB.getArg[Array[Byte]](3))

          val ctxOff = bodyFB.newLocal[Long]
          val gOff = bodyFB.newLocal[Long]
          val bOff = bodyFB.newLocal[Long]
          val bOS = bodyFB.newLocal[ByteArrayOutputStream]

          bodyFB.emit(Code(
            ctxOff := cDec(bodyFB.getArg[Region](1), cCodec.buildCodeInputBuffer(ctxIS)),
            gOff := gDec(bodyFB.getArg[Region](1), gCodec.buildCodeInputBuffer(gIS)),
            bOff := bodyMB.invoke[Long](bodyFB.getArg[Region](1),
              Region.loadIRIntermediate(ctxType)(ctxTypeTuple.fieldOffset(ctxOff, 0)),
              ctxTypeTuple.isFieldMissing(region, ctxOff, 0),
              Region.loadIRIntermediate(gType)(gTypeTuple.fieldOffset(gOff, 0)),
              gTypeTuple.isFieldMissing(region, gOff, 0)),
            bOS := Code.newInstance[ByteArrayOutputStream](),
            bOB := bCodec.buildCodeOutputBuffer(bOS),
            bEnc(bodyFB.getArg[Region](1), bOff, bOB),
            bOB.invoke[Unit]("flush"),
            bOB.invoke[Unit]("close"),
            bOS.invoke[Array[Byte]]("toByteArray")))

          val fID = genUID()
          parentFB.addModule(fID, bodyFB.resultWithIndex())
          fID
        }

        val spark = parentFB.backend()
        val contextAE = emitArrayIterator(contexts)
        val globalsT = emit(globals)

        val cEnc = cCodec.buildEmitEncoderF[Long](ctxTypeTuple, parentFB)
        val gEnc = gCodec.buildEmitEncoderF[Long](gTypeTuple, parentFB)
        val (bRetPType, bDec) = bCodec.buildEmitDecoderF[Long](bTypeTuple.virtualType, parentFB)

        assert(bRetPType == bTypeTuple)

        val baos = mb.newField[ByteArrayOutputStream]
        val buf = mb.newField[OutputBuffer]
        val ctxab = mb.newField[ByteArrayArrayBuilder]
        val encRes = mb.newField[Array[Array[Byte]]]

        val contextT = {
          val sctxb = new StagedRegionValueBuilder(mb, ctxTypeTuple)
          contextAE.arrayEmitter { (m: Code[Boolean], v: Code[_]) =>
            Code(
              baos.invoke[Unit]("reset"),
              sctxb.start(),
              m.mux(
                sctxb.setMissing(),
                sctxb.addIRIntermediate(ctxType)(v)),
              cEnc(region, sctxb.offset, buf),
              buf.invoke[Unit]("flush"),
              ctxab.invoke[Array[Byte], Unit]("add", baos.invoke[Array[Byte]]("toByteArray")))
          }
        }

        val addGlobals = {
          val sgb = new StagedRegionValueBuilder(mb, gTypeTuple)
          Code(
            globalsT.setup,
            sgb.start(),
            globalsT.m.mux(
              sgb.setMissing(),
              sgb.addIRIntermediate(gType)(globalsT.v)),
            gEnc(region, sgb.offset, buf),
            buf.invoke[Unit]("flush"))
        }

        val decodeResult = {
          val sab = new StagedRegionValueBuilder(mb, x.pType)
          val bais = Code.newInstance[ByteArrayInputStream, Array[Byte]](encRes(sab.arrayIdx))
          val eltTupled = mb.newField[Long]
          Code(
            sab.start(encRes.length()),
            Code.whileLoop(sab.arrayIdx < encRes.length(),
              eltTupled := bDec(region, bCodec.buildCodeInputBuffer(bais)),
              bTypeTuple.isFieldMissing(region, eltTupled, 0).mux(
                sab.setMissing(),
                sab.addIRIntermediate(bType)(Region.loadIRIntermediate(bType)(bTypeTuple.fieldOffset(eltTupled, 0)))),
              sab.advance()),
            sab.end())
        }

        EmitTriplet(
          contextT.setup,
          contextT.m.getOrElse(false),
          Code(
            baos := Code.newInstance[ByteArrayOutputStream](),
            buf := cCodec.buildCodeOutputBuffer(baos), // TODO: take a closer look at whether we need two codec buffers?
            ctxab := Code.newInstance[ByteArrayArrayBuilder, Int](16),
            contextAE.calcLength,
            contextT.addElements,
            baos.invoke[Unit]("reset"),
            addGlobals,
            encRes := spark.invoke[String, Array[Array[Byte]], Array[Byte], Array[Array[Byte]]](
              "collectDArray", functionID,
              ctxab.invoke[Array[Array[Byte]]]("result"),
              baos.invoke[Array[Byte]]("toByteArray")),
            decodeResult))
    }
  }

  private def capturedReferences(ir: IR): (IR, (Emit.E, DependentEmitFunction[_]) => Emit.E) = {
    var ids = Set[String]()

    def getReferenced: IR => IR = {
      case node@Ref(id, typ) =>
        ids += id
        node
      case node => MapIR(getReferenced)(node)
    }

    (getReferenced(ir), { (env: Emit.E, f: DependentEmitFunction[_]) =>
      Env[(TypeInfo[_], Code[Boolean], Code[_])](ids.toFastSeq.flatMap { id: String =>
         env.lookupOption(id).map { e =>
           val (ti, m, v) = e
           (id, (ti, f.addField[Boolean](m).load(), f.addField(v)(ti.asInstanceOf[TypeInfo[Any]]).load()))
        }
      }: _*)
    })
  }

  private def makeDependentSortingFunction[T: TypeInfo](
    ir: IR, env: Emit.E): DependentEmitFunction[AsmFunction2[T, T, Boolean]] = {
    val (newIR, getEnv) = capturedReferences(ir)
    val f = mb.fb.newDependentFunction[T, T, Boolean]
    val fregion = f.addField[Region](region)
    val newEnv = getEnv(env, f)

    val sort = f.newMethod[Region, T, Boolean, T, Boolean, Boolean]
    val EmitTriplet(setup, m, v) = new Emit(ctx, sort, 1).emit(newIR, newEnv, EmitRegion.default(sort), None)

    sort.emit(Code(setup, m.mux(Code._fatal("Result of sorting function cannot be missing."), v)))
    f.apply_method.emit(Code(sort.invoke(fregion, f.getArg[T](1), false, f.getArg[T](2), false)))
    f
  }

  private def getAsDependentFunction[A1: TypeInfo, R: TypeInfo](
    ir: IR, argEnv: Env[IR], env: Emit.E, errorMsg: String
  ): DependentFunction[AsmFunction3[Region, A1, Boolean, R]] = {
    val (newIR, getEnv) = capturedReferences(Subst(ir, BindingEnv(argEnv)))
    val f = mb.fb.newDependentFunction[Region, A1, Boolean, R]

    val newEnv = getEnv(env, f)

    // FIXME: This shouldn't take aggs but might want to?
    val foo = new Emit(ctx, f.apply_method, 1)
    val EmitTriplet(setup, m, v) = foo.emit(newIR, newEnv, EmitRegion.default(f.apply_method), None)

    val call = Code(
      setup,
      m.mux(Code._fatal(errorMsg), v))
    f.emit(call)
    f
  }

  private def emitArrayIterator(ir: IR, env: E, er: EmitRegion, container: Option[AggContainer]): ArrayIteratorTriplet =
    Streamify(ir) match {
      case ToStream(x) =>
        emitOldArrayIterator(x, env, er, container)
      case x => EmitStream(this, x, env, er, container).toArrayIterator(mb)
    }

  private def emitOldArrayIterator(ir: IR, env: E, er: EmitRegion, container: Option[AggContainer]): ArrayIteratorTriplet = {
    def emit(ir: IR, env: E = env) = this.emit(ir, env, er, container)

    def emitArrayIterator(ir: IR, env: E = env) =
      this.emitOldArrayIterator(ir, env, er, container)

    ir match {
      case x@ArrayRange(startir, stopir, stepir) =>
        val codeStart = emit(startir)
        val codeStop = emit(stopir)
        val codeStep = emit(stepir)

        val start = mb.newLocal[Int]("ar_start")
        val stop = mb.newLocal[Int]("ar_stop")
        val step = mb.newLocal[Int]("ar_step")

        val i = mb.newLocal[Int]("ar_i")
        val len = mb.newLocal[Int]("ar_len")
        val llen = mb.newLocal[Long]("ar_llen")

        val calcLength = Code(
          start := coerce[Int](codeStart.v),
          stop := coerce[Int](codeStop.v),
          step := coerce[Int](codeStep.v),
          step.ceq(0).mux(
            Code._fatal("Array range cannot have step size 0."),
            Code._empty[Unit]),
          llen := (step < 0).mux(
            (start <= stop).mux(0L, (start.toL - stop.toL - 1L) / (-step).toL + 1L),
            (start >= stop).mux(0L, (stop.toL - start.toL - 1L) / step.toL + 1L)),
          (llen > const(Int.MaxValue.toLong)).mux(
            Code._fatal("Array range cannot have more than MAXINT elements."),
            len := (llen < 0L).mux(0L, llen).toI)
        )

        ArrayIteratorTriplet(calcLength, Some(len.load()), { continuation: F =>
          EmitArrayTriplet(
            coerce[Unit](Code(codeStart.setup, codeStop.setup, codeStep.setup)),
            Some(codeStart.m || codeStop.m || codeStep.m),
            Code(
              i := 0,
              Code.whileLoop(i < len,
                continuation(false, start),
                i := i + 1,
                start := start + step)))
        })
      case x@ArrayFilter(a, name, condition) =>
        val elementTypeInfoA = coerce[Any](typeToTypeInfo(x.typ.elementType))
        val xmv = mb.newField[Boolean]()
        val xvv = mb.newField(name)(elementTypeInfoA)
        val condenv = env.bind(name, (elementTypeInfoA, xmv.load(), xvv.load()))
        val codeCond = emit(condition, condenv)

        val filterCont = { (cont: F, m: Code[Boolean], v: Code[_]) =>
          Code(
            xmv := m,
            xvv := xmv.mux(
              defaultValue(x.typ.elementType),
              v),
            codeCond.setup,
            (codeCond.m || !coerce[Boolean](codeCond.v)).mux(
              Code._empty,
              cont(xmv, xvv)))
        }
        emitArrayIterator(a).copy(length = None).wrapContinuation(filterCont)

      case x@ArrayFlatMap(a, name, body) =>
        val elementTypeInfoA = coerce[Any](typeToTypeInfo(coerce[TStreamable](a.typ).elementType))
        val xmv = mb.newField[Boolean]()
        val xvv = mb.newField(name)(elementTypeInfoA)
        val bodyenv = env.bind(name, (elementTypeInfoA, xmv.load(), xvv.load()))
        val bodyIt = emitArrayIterator(body, bodyenv)

        val bodyCont = { (cont: F, m: Code[Boolean], v: Code[_]) =>
          val bodyArray = bodyIt.arrayEmitter(cont)
          Code(
            xmv := m,
            xmv.mux(
              Code._empty,
              Code(
                xvv := v,
                bodyArray.setup,
                bodyArray.m.map(_.mux(
                  Code._empty,
                  Code(bodyIt.calcLength, bodyArray.addElements))).getOrElse(Code(bodyIt.calcLength, bodyArray.addElements)))))
        }

        emitArrayIterator(a).copy(length = None).wrapContinuation(bodyCont)

      case x@ArrayMap(a, name, body) =>
        val elt = coerce[TStreamable](a.typ).elementType
        val elementTypeInfoA = coerce[Any](typeToTypeInfo(elt))
        val xmv = mb.newField[Boolean]()
        val xvv = mb.newField(name)(elementTypeInfoA)
        val bodyenv = env.bind(name, (elementTypeInfoA, xmv.load(), xvv.load()))
        val codeB = emit(body, bodyenv)
        val mapCont = { (continuation: F, m: Code[Boolean], v: Code[_]) =>
          Code(
            xmv := m,
            xvv := xmv.mux(defaultValue(elt), v),
            codeB.setup,
            continuation(codeB.m, codeB.v))
        }
        emitArrayIterator(a).wrapContinuation(mapCont)

      case x@ArrayScan(a, zero, accumName, eltName, body) =>
        val elt = coerce[TStreamable](a.typ).elementType
        val accumTypeInfo = coerce[Any](typeToTypeInfo(zero.typ))
        val elementTypeInfoA = coerce[Any](typeToTypeInfo(elt))
        val xmbody = mb.newField[Boolean]()
        val xmaccum = mb.newField[Boolean]()
        val xvaccum = mb.newField(accumName)(accumTypeInfo)
        val xmv = mb.newField[Boolean]()
        val xvv = mb.newField(eltName)(elementTypeInfoA)

        val bodyenv = env
          .bind(accumName, (accumTypeInfo, xmaccum.load(), xvaccum.load()))
          .bind(eltName, (elementTypeInfoA, xmv.load(), xvv.load()))
        val codeB = emit(body, bodyenv)
        val z = emit(zero)
        val aIt = emitArrayIterator(a)

        ArrayIteratorTriplet(
          Code(
            z.setup,
            xmaccum := z.m,
            xvaccum := xmaccum.mux(defaultValue(zero.typ), z.v),
            aIt.calcLength),
          aIt.length.map(_ + 1),
          (cont: F) => {
            val aet = aIt.arrayEmitter { (m, v) =>
              Code(
                xmv := m,
                xvv := xmv.mux(defaultValue(elt), v),
                codeB.setup,
                xmbody := codeB.m,
                xvaccum := xmbody.mux(defaultValue(zero.typ), codeB.v),
                xmaccum := xmbody,
                cont(xmaccum, xvaccum))
            }
            EmitArrayTriplet(aet.setup, aet.m,
              Code(cont(xmaccum, xvaccum), aet.addElements))
          })

      case MakeArray(args, _) =>
        val f = { cont: F =>
          EmitArrayTriplet(Code._empty[Unit], None, coerce[Unit](Code(
            for (elt <- args) yield {
              val codeElt = emit(elt)
              Code(codeElt.setup, cont(codeElt.m, codeElt.v))
            })))
        }
        ArrayIteratorTriplet(Code._empty, Some(const(args.length)), f)

      case If(cond, cnsq, altr) =>
        assert(cnsq.typ == altr.typ)

        val codeCond = emit(cond)
        val mout = mb.newLocal[Boolean]()
        val cnsqArray = emitArrayIterator(cnsq)
        val altrArray = emitArrayIterator(altr)

        val f = { cont: F =>
          def addElements(array: ArrayIteratorTriplet): Code[Unit] = {
            val emitter = array.arrayEmitter(cont)
            Code(
              emitter.setup,
              mout := emitter.m.getOrElse(false),
              mout.mux(Code._empty, Code(array.calcLength, emitter.addElements)))
          }

          val setup = Code(
            codeCond.setup,
            codeCond.m.mux(
              mout := true,
              coerce[Boolean](codeCond.v).mux(
                addElements(cnsqArray), addElements(altrArray))))
          EmitArrayTriplet(setup, Some(mout.load()), Code._empty)
        }

        ArrayIteratorTriplet(Code._empty, None, f)

      case x@ArrayLeftJoinDistinct(left, right, l, r, compKey, join) =>
        // no missing
        val lelt = coerce[TStreamable](left.typ).elementType
        val relt = coerce[TStreamable](right.typ).elementType
        val rEltPType = coerce[PStreamable](right.pType).elementType
        val rtyp = coerce[PStreamable](right.pType).asPArray

        val larray = emitArrayIterator(left)
        val rarray = emitArrayIterator(right).toEmitTriplet(mb, rtyp)

        val rm = mb.newField[Boolean]("join_rarray_m")
        val rv = mb.newField[Long]("join_rarray_v")
        val ri = mb.newField[Int]("join_rarray_i")
        val rlen = mb.newField[Int]("join_rarray_len")

        val lm = mb.newField[Boolean]("join_left_m")
        val lv = mb.newField("join_left_v")(typeToTypeInfo(lelt))

        val fenv = env.bind(
          (l, (typeToTypeInfo(lelt), lm.load(), lv.load())),
          (r, (typeToTypeInfo(relt), rm.load() || rtyp.isElementMissing(region, rv, ri), rtyp.loadElement(region, rv, ri))))

        val compKeyF = mb.fb.newMethod(typeInfo[Region], typeInfo[Int])
        val et = new Emit(ctx, compKeyF, 1).emit(compKey, fenv, er, container)
        compKeyF.emit(Code(et.setup, et.m.mux(Code._fatal("ArrayLeftJoinDistinct: comp can't be missing"), et.value[Int])))
        val joinF = mb.fb.newMethod(typeInfo[Region], typeToTypeInfo(relt), typeInfo[Boolean], typeToTypeInfo(join.typ))
        val jet = new Emit(ctx, joinF, 1).emit(Subst(join, BindingEnv(Env[IR](r -> In(0, rEltPType)))), fenv, er, container)
        joinF.emit(Code(jet.setup, jet.m.mux(Code._fatal("ArrayLeftJoinDistinct: joined can't be missing"), jet.v)))

        val ae = { cont: Emit.F =>
          val aet = larray.arrayEmitter { (m: Code[Boolean], v: Code[_]) =>
            Code(
              lm := m,
              lv.storeAny(v),
              Code.whileLoop((ri < rlen) && (coerce[Int](compKeyF.invoke(region)) > 0),
                ri := ri + 1),
              cont(
                lm.load(),
                ((ri < rlen) && coerce[Int](compKeyF.invoke(region)).ceq(0)).mux(
                  joinF.invoke(region, rtyp.loadElement(region, rv, ri), rm.load() || rtyp.isElementMissing(region, rv, ri)),
                  joinF.invoke(region, defaultValue(relt), true))))
          }
          val setup = Code(
            rarray.setup,
            ri := 0,
            rm := rarray.m,
            rm.mux(
              rlen := 0,
              Code(
                rv := rarray.value[Long],
                rlen := rtyp.loadLength(region, rv))))
          EmitArrayTriplet(Code(setup, aet.setup), aet.m, aet.addElements)
        }

        ArrayIteratorTriplet(larray.calcLength, larray.length, ae)

      case ArrayAggScan(a, name, query) =>
        assert(!ContainsAggIntermediate(query))
        val elt = coerce[TStreamable](a.typ).elementType
        val elementTypeInfoA = coerce[Any](typeToTypeInfo(elt))
        val xmv = mb.newField[Boolean]()
        val xvv = mb.newField(name)(elementTypeInfoA)
        val bodyEnv = env.bind(name, (elementTypeInfoA, xmv.load(), xvv.load()))

        val res = genUID()
        val extracted = agg.Extract(agg.Extract.liftScan(query), res)
        val aggSigs = extracted.aggs

        val (newContainer, aggSetup, aggCleanup) = AggContainer.fromFunctionBuilder(aggSigs, mb.fb, "array_agg_scan")

        val init = Optimize(extracted.init, noisy = true,
          context = "ArrayAggScan/StagedExtractAggregators/postAggIR", ctx)
        val perElt = Optimize(extracted.seqPerElt, noisy = true,
          context = "ArrayAggScan/StagedExtractAggregators/init", ctx)
        val postAgg = Optimize[IR](Let(res, extracted.results, extracted.postAggIR), noisy = true,
          context = "ArrayAggScan/StagedExtractAggregators/perElt", ctx)

        val codeInit = this.emit(init, env, er, Some(newContainer))
        val codeSeq = this.emit(perElt, bodyEnv, er, Some(newContainer))
        val newElt = this.emit(postAgg, bodyEnv, er, Some(newContainer))

        val it = emitArrayIterator(a)
        val ae = { cont: Emit.F =>
          val aet = it.arrayEmitter { (m, v) =>
            Code(
              xmv := m,
              xvv := v,
              newElt.setup,
              cont(newElt.m, newElt.v),
              codeSeq.setup)
          }

          EmitArrayTriplet(aet.setup, aet.m, Code(aggSetup, codeInit.setup, aet.addElements, aggCleanup))
        }

        ArrayIteratorTriplet(it.calcLength, it.length, ae)

      case ReadPartition(path, spec, rowType) =>
        val p = emit(path)
        val (returnedRowPType, rowDec) = spec.buildEmitDecoderF[Long](rowType, mb.fb)
        val rowBuf = mb.newField[InputBuffer]
        val pathString = Code.invokeScalaObject[Region, Long, String](
          PString.getClass, "loadString", region, p.value[Long])

        ArrayIteratorTriplet(Code._empty, None, { cont: F =>
          EmitArrayTriplet(p.setup, Some(p.m), Code(
            rowBuf := spec.buildCodeInputBuffer(mb.fb.getUnsafeReader(pathString, true)),
            Code.whileLoop(rowBuf.load().readByte().toZ,
              cont(false, rowDec(region, rowBuf)))))
        })

      case _ =>
        val t: PArray = coerce[PStreamable](ir.pType).asPArray
        val i = mb.newField[Int]("i")
        val len = mb.newField[Int]("len")
        val aoff = mb.newField[Long]("aoff")
        val codeV = emit(ir, env)
        val calcLength = Code(
          aoff := coerce[Long](codeV.v),
          len := t.loadLength(region, aoff))
        ArrayIteratorTriplet(calcLength, Some(len.load()), { continuation: F =>
          EmitArrayTriplet(codeV.setup, Some(codeV.m), Code(
            i := 0,
            Code.whileLoop(i < len,
              continuation(t.isElementMissing(region, aoff, i),
                Region.loadIRIntermediate(t.elementType)(t.elementOffsetInRegion(region, aoff, i))),
              i := i + 1)))
        })
    }
  }

  private def present(x: Code[_]): EmitTriplet =
    EmitTriplet(Code._empty, const(false), x)

  private def void(x: Code[Unit]*): EmitTriplet = EmitTriplet(coerce[Unit](Code(x: _*)), false, Code._empty)

  private def strict(value: Code[_], args: EmitTriplet*): EmitTriplet = {
    EmitTriplet(
      coerce[Unit](Code(args.map(_.setup): _*)),
      if (args.isEmpty) false else args.map(_.m).reduce(_ || _),
      value)
  }

  private def normalArgumentPosition(idx: Int): Int = {
    1 + nSpecialArguments + idx * 2
  }

  def deforestNDArray(er: EmitRegion, x: IR, env: Emit.E): NDArrayEmitter = {
    def deforest(nd: IR): NDArrayEmitter = deforestNDArray(er, nd, env)

    val xType = coerce[PNDArray](x.pType)
    val nDims = xType.nDims

    x match {
      case NDArrayMap(child, elemName, body) =>
        val childP = child.pType.asInstanceOf[PNDArray]
        val elemPType = childP.elementType
        val vti = typeToTypeInfo(elemPType.virtualType)
        val elemRef = coerce[Any](mb.newField(elemName)(vti))
        val bodyEnv = env.bind(name=elemName, v=(vti, false, elemRef.load()))
        val bodyt = this.emit(body, bodyEnv, er, None)

        val childEmitter = deforest(child)
        val setup = Code(childEmitter.setupShape)

        new NDArrayEmitter(mb, childEmitter.nDims, childEmitter.outputShape,
          childP.shape.pType, body.pType, setup, childEmitter.setupMissing, childEmitter.missing) {
          override def outputElement(idxVars: Array[Code[Long]]): Code[_] = {
            Code(
              elemRef := childEmitter.outputElement(idxVars),
              bodyt.setup,
              bodyt.m.orEmpty(Code._fatal("NDArray map body cannot be missing")),
              bodyt.v
            )
          }
        }
      case NDArrayMap2(lChild, rChild, lName, rName, body) =>
        val lP = coerce[PNDArray](lChild.pType)
        val rP = coerce[PNDArray](rChild.pType)

        val lVti = typeToTypeInfo(lP.elementType.virtualType)
        val rVti = typeToTypeInfo(rP.elementType.virtualType)

        val lElemRef = coerce[Any](mb.newField(lName)(lVti))
        val rElemRef = coerce[Any](mb.newField(rName)(rVti))

        val bodyEnv = env.bind(name=lName, v=(lVti, false, lElemRef.load()))
                         .bind(name=rName, v=(rVti, false, rElemRef.load()))
        val bodyt = this.emit(body, bodyEnv, er, None)

        val leftChildEmitter = deforest(lChild)
        val rightChildEmitter = deforest(rChild)

        val shapeArray = NDArrayEmitter.unifyShapes2(leftChildEmitter.outputShape, rightChildEmitter.outputShape)

        val setupMissing = Code(leftChildEmitter.setupMissing, rightChildEmitter.setupMissing)
        val setupShape = Code(leftChildEmitter.setupShape, rightChildEmitter.setupShape)

        new NDArrayEmitter(mb, lP.shape.pType.size, shapeArray, lP.shape.pType, body.pType, setupShape, setupMissing, leftChildEmitter.missing || rightChildEmitter.missing) {
          override def outputElement(idxVars: Array[Code[Long]]): Code[_] = {

            val lIdxVars2 = NDArrayEmitter.zeroBroadcastedDims2(mb, idxVars, nDims, leftChildEmitter.outputShape)
            val rIdxVars2 = NDArrayEmitter.zeroBroadcastedDims2(mb, idxVars, nDims, rightChildEmitter.outputShape)

            Code(
              lElemRef := leftChildEmitter.outputElement(lIdxVars2),
              rElemRef := rightChildEmitter.outputElement(rIdxVars2),
              bodyt.setup,
              bodyt.m.orEmpty(Code._fatal("NDArray map body cannot be missing")),
              bodyt.v
            )
          }
        }

      case x@NDArrayReindex(child, indexExpr) =>
        val childEmitter = deforest(child)
        val childPType = child.pType.asInstanceOf[PNDArray]

        val outputPType = x.pType
        val outputShapePType = outputPType.shape.pType


        val shapeSeq = indexExpr.map {childIndex =>
          if (childIndex < childPType.nDims) {
            childEmitter.outputShape(childIndex)
          }
          else {
            const(1L)
          }
        }.toArray

        new NDArrayEmitter(mb, indexExpr.length, shapeSeq, outputShapePType, outputPType.elementType, childEmitter.setupShape, childEmitter.setupMissing, childEmitter.missing) {
          override def outputElement(idxVars: Array[Code[Long]]): Code[_] = {
            val concreteIdxsForChild = Array.tabulate(childEmitter.nDims) { childDim =>
              val parentDim = indexExpr.indexOf(childDim)
              idxVars(parentDim)
            }
            childEmitter.outputElement(concreteIdxsForChild)
          }
        }

      case x@NDArrayReshape(childND, shape) =>

        // Need to take this shape, which may have a -1 in it, and turn it into a compatible shape if possible.
        def compatibleShape(numElements: Code[Long], requestedShape: Array[Code[Long]]): (Code[Unit], Array[Code[Long]]) = {
          val hasNegativeOne = mb.newLocal[Boolean]
          val runningProduct = mb.newLocal[Long]
          val quotient = mb.newLocal[Long]
          val tempShapeElement = mb.newLocal[Long]

          val newShapeVars = (0 until requestedShape.length).map(_ => mb.newField[Long]).toArray

          val setupShape = coerce[Unit](Code(
            hasNegativeOne := false,
            runningProduct := 1L,

            Code.foreach(requestedShape) { requestedShapeElement => Code(
              tempShapeElement := requestedShapeElement,
              (tempShapeElement <= 0L).mux(
                (tempShapeElement ceq -1L).mux(
                  hasNegativeOne.mux(
                    Code._fatal("Can't infer shape, more than one -1"),
                    hasNegativeOne := true
                  ),
                  Code._fatal("Can't reshape, new shape must contain only positive numbers or -1")),
                runningProduct := runningProduct * tempShapeElement
              )
            )},
            hasNegativeOne.mux(
              (numElements % runningProduct) > 0L,
              numElements cne runningProduct
            ).orEmpty(Code._fatal("Can't reshape since requested shape is incompatible with number of elements")),
            quotient := numElements / runningProduct,
            Code(newShapeVars.zip(requestedShape).map { case (variable, shapeElement) =>
              variable := (shapeElement ceq -1L).mux(quotient, shapeElement)}:_*)
          ))

          (setupShape, newShapeVars.map(_.load()))
        }

        val childEmitter = deforest(childND)

        val requestedShapet = emit(shape, env, resultRegion, None)
        val requestedShapeAddress = mb.newField[Long]
        val requestedShapePType = coerce[PTuple](shape.pType)
        val requestedShapeTuple = new CodePTuple(requestedShapePType, region, requestedShapeAddress)
        val requestedShapeArray = (0 until requestedShapePType.size).map(i => requestedShapeTuple[Long](i)).toArray

        val (childShapeCachingCode, childShapeCached) = childEmitter.outputShape.cacheEntries(mb, LongInfo)

        val numElements = mb.newField[Long]

        val (reshapeSetup, reshapedShapeArray) = compatibleShape(numElements, requestedShapeArray)

        val setupMissing = Code(
          childEmitter.setupMissing,
          requestedShapet.setup
        )

        val setupShape = Code(
          childEmitter.setupShape,
          childShapeCachingCode,
          requestedShapeAddress := requestedShapet.value[Long],
          numElements := coerce[PNDArray](childND.pType).numElements(childShapeCached, mb),
          reshapeSetup
        )

        new NDArrayEmitter(mb, reshapedShapeArray.length, reshapedShapeArray, requestedShapePType.setRequired(true).asInstanceOf[PTuple],
          childEmitter.outputElementPType, setupShape, setupMissing, childEmitter.missing || requestedShapet.m) {
          override def outputElement(idxVars: Array[Code[Long]]): Code[_] = {
            val storeElementIndex = mb.newField[Long]

            val (newIdxVarsSetup, newIdxVars) = x.pType.unlinearizeIndexRowMajor(storeElementIndex, childShapeCached, region, mb)

            assert(newIdxVars.length == childEmitter.nDims)

            Code(
              storeElementIndex := x.pType.linearizeIndicesRowMajor(idxVars, reshapedShapeArray, region, mb),
              newIdxVarsSetup,
              childEmitter.outputElement(newIdxVars)
            )
          }
        }

      case x@NDArraySlice(child, slicesIR) =>
        val childEmitter = deforest(child)

        val slicest = emit(slicesIR, env, resultRegion, None)
        val slicesValueAddress = mb.newField[Long]
        val slices = new CodePTuple(coerce[PTuple](slicesIR.pType), region, slicesValueAddress)

        val slicers = slices.withTypes.collect {
          case (t: PTuple, slice) => new CodePTuple(t, region, slice)
        }

        val missingSliceElements = slicers.map(_.missingnessPattern.reduce(_ || _)).fold(const(false))(_ || _)
        val anyMissingness = missingSliceElements || slices.missingnessPattern.fold(const(false))(_ || _)

        val codeSlices = slicers.map(_.values[Long, Long, Long])

        val outputShape = codeSlices.map { case (start, stop, step) =>
          (step >= 0L && start <= stop).mux(
            const(1L) + ((stop - start) - 1L) / step,
            (step < 0L && start >= stop).mux(
              (((stop - start) + 1L) / step) + 1L,
              0L)
          )
        }.toArray

        val setupMissing = Code(
          slicest.setup,
          slicesValueAddress := slicest.value[Long],
          childEmitter.setupMissing
        )

        val missing = childEmitter.missing || anyMissingness

        new NDArrayEmitter(mb, x.pType.nDims, outputShape, x.pType.shape.pType, x.pType.elementType, childEmitter.setupShape, setupMissing, missing) {
          override def outputElement(idxVars: Array[Code[Long]]): Code[_] = {
            val oldIdxVarsIter = idxVars.iterator

            val sliceIdxVars2 = slices.withTypes.map {
              case (_: PInt64, indexer) =>
                coerce[Long](indexer)
              case (t: PTuple, slicer) =>
                val (start, _, step) = new CodePTuple(t, region, slicer).values[Long, Long, Long]
                start + oldIdxVarsIter.next() * step
            }

            childEmitter.outputElement(sliceIdxVars2.toArray)
          }
        }

      case _ =>
        val ndt = emit(x, env, er, None)
        val ndAddress = mb.newField[Long]
        val setup = Code(
          ndAddress := ndt.value[Long]
        )
        val xP = x.pType.asInstanceOf[PNDArray]

        val shapeAddress = xP.shape.load(er.region, ndAddress)
        val shapeTuple = new CodePTuple(xP.shape.pType, er.region, shapeAddress)

        val shapeArray = (0 until xP.shape.pType.nFields).map(i => shapeTuple.apply[Long](i)).toArray

        new NDArrayEmitter(mb, nDims, shapeArray,
          xP.shape.pType, xP.elementType, setup, ndt.setup, ndt.m) {
          override def outputElement(idxVars: Array[Code[Long]]): Code[_] = {
            val elementLocation = xP.getElementAddress(idxVars, ndAddress, er.region, mb)
            Region.loadIRIntermediate(outputElementPType)(elementLocation)
          }
        }
    }
  }
}

object NDArrayEmitter {

  def zeroBroadcastedDims2(mb: MethodBuilder, loopVars: Array[Code[Long]], nDims: Int, shapeArray: Array[Code[Long]]): Array[Code[Long]] = {
    val broadcasted = 0L
    val notBroadcasted = 1L
    Array.tabulate(nDims)(dim => (shapeArray(dim) > 1L).mux(notBroadcasted, broadcasted) * loopVars(dim))
  }

  def broadcastMask(shapeArray: Array[Code[Long]]): Array[Code[Long]] = {
    val broadcasted = 0L
    val notBroadcasted = 1L
    shapeArray.map(shapeElement => (shapeElement > 1L).mux(notBroadcasted, broadcasted))
  }

  def zeroBroadcastedDims(indices: Array[Code[Long]], broadcastMask: Array[Code[Long]]): Array[Code[Long]] = {
    indices.zip(broadcastMask).map { case (index, flag) => index * flag }
  }

  def unifyShapes2(leftShape: Array[Code[Long]], rightShape: Array[Code[Long]]): Array[Code[Long]] = {
    leftShape.zip(rightShape).map{case (left, right) =>
      val notSameAndNotBroadcastable = !((left ceq right) || (left ceq 1L) || (right ceq 1L))
      coerce[Long](Code(
        notSameAndNotBroadcastable.mux(
          Code._fatal("Incompatible NDArray shapes"),
          (left > right).mux(left, right)
        )
      ))
    }
  }

  def matmulShape(leftShape: Array[Code[Long]], rightShape: Array[Code[Long]]): (Code[Unit], Array[Code[Long]]) = {
    val ((lK, rK), shape) = (leftShape.toSeq, rightShape.toSeq) match {
      case (Seq(l), Seq(r)) =>
        ((l, r), Array[Code[Long]]())
      case (Seq(l), rs :+ r2 :+ r1) =>
        ((l, r2), (rs :+ r1).toArray)
      case (ls :+ l2 :+ l1, Seq(r)) =>
        ((l1, r), (ls :+ l1).toArray)
      case (
        ls :+ l2 :+ l1,
        rs :+ r2 :+ r1
        ) => ((l1, r2), unifyShapes2(ls.toArray, rs.toArray) :+ l2 :+ r1)
      case (l, r) =>
        fatal(s"Matrix multiply compiler bug: $l $r")
    }

    val dimCheck = (lK cne rK).orEmpty(
      Code._fatal(const("Matrix dimensions incompatible: ").concat(lK.toS).concat(" ").concat(rK.toS)))

    (dimCheck, shape)
  }
}

abstract class NDArrayEmitter(
   val mb: MethodBuilder,
   val nDims: Int,
   val outputShape: Array[Code[Long]],
   val outputShapePType: PTuple,
   val outputElementPType: PType,
   val setupShape: Code[_],
   val setupMissing: Code[Unit] = Code._empty[Unit],
   val missing: Code[Boolean] = false) {

  private val outputShapeVariables = (0 until nDims).map(_ => mb.newField[Long]).toArray

  def outputElement(idxVars: Array[Code[Long]]): Code[_]

  def emit(targetType: PNDArray): EmitTriplet = {
    val dataSrvb = new StagedRegionValueBuilder(mb, targetType.data.pType)

    val dataAddress: Code[Long] =
      Code(
        dataSrvb.start(targetType.numElements(outputShapeVariables.map(_.load()), mb).toI),
        emitLoops(dataSrvb),
        dataSrvb.end()
      )

    def shapeBuilder(srvb: StagedRegionValueBuilder): Code[Unit] = {
      coerce[Unit](Code(
        srvb.start(),
        Code.foreach(outputShapeVariables){ shapeElement =>
          Code(
            srvb.addLong(shapeElement),
            srvb.advance()
          )
        }
      ))
    }

    val fullSetup = Code(
      setupMissing,
      missing.mux(
        Code._empty,
        Code(
          setupShape,
          Code.foreach(0 until nDims)(index => outputShapeVariables(index) := outputShape(index))
        )
      )
    )

    EmitTriplet(fullSetup, missing, targetType.construct(0, 0, shapeBuilder, targetType.makeDefaultStridesBuilder(outputShapeVariables.map(_.load()), mb), dataAddress, mb))
  }

  private def emitLoops(srvb: StagedRegionValueBuilder): Code[_] = {
    val idxVars = Array.tabulate(nDims) {_ => mb.newField[Long]}
    val loadedIdxVars = idxVars.map(_.load())
    val storeElement = mb.newLocal(typeToTypeInfo(outputElementPType.virtualType)).asInstanceOf[LocalRef[Double]]
    val body =
      Code(
        storeElement := outputElement(loadedIdxVars).asInstanceOf[Code[Double]],
        srvb.addIRIntermediate(outputElementPType)(storeElement),
        srvb.advance()
      )
    idxVars.zipWithIndex.foldRight(body) { case((dimVar, dimIdx), innerLoops) =>
      Code(
        dimVar := 0L,
        Code.whileLoop(dimVar < outputShapeVariables(dimIdx),
          innerLoops,
          dimVar := dimVar + 1L
        )
      )
    }
  }
}
