package is.hail.expr.ir

import is.hail.utils._
import is.hail.annotations.MemoryBuffer
import is.hail.asm4s.FunctionBuilder.methodNodeToGrowable
import is.hail.asm4s._
import is.hail.expr
import is.hail.expr.{TInt32, TInt64, TArray, TContainer, TStruct, TFloat32, TFloat64, TBoolean}
import is.hail.annotations.StagedRegionValueBuilder
import scala.collection.generic.Growable

import org.objectweb.asm.Opcodes._
import org.objectweb.asm.Type
import org.objectweb.asm.tree._

import scala.reflect.classTag
import scala.reflect.ClassTag

object Compile2 {
  private def dummyValue(t: expr.Type): Code[_] = t match {
    case TBoolean => false
    case TInt32 => 0
    case TInt64 => 0L
    case TFloat32 => 0.0f
    case TFloat64 => 0.0
    case _ => 0L // reference types
  }
  private def typeToTypeInfo(t: expr.Type): TypeInfo[_] = t match {
    case TInt32 => typeInfo[Int]
    case TInt64 => typeInfo[Long]
    case TFloat32 => typeInfo[Float]
    case TFloat64 => typeInfo[Double]
    case TBoolean => typeInfo[Boolean]
    case _ => typeInfo[Long] // reference types
  }

  private def loadAnnotation(region: Code[MemoryBuffer], typ: expr.Type): Code[Long] => Code[_] = typ match {
    case TInt32 =>
      region.loadInt(_)
    case TInt64 =>
      region.loadLong(_)
    case TFloat32 =>
      region.loadFloat(_)
    case TFloat64 =>
      region.loadDouble(_)
    case _ =>
      off => off
  }

  private def storeAnnotation(region: Code[MemoryBuffer], typ: expr.Type): (Code[Long], Code[_]) => Code[_] = typ match {
    case TInt32 =>
      (off, v) => region.storeInt32(off, v.asInstanceOf[Code[Int]])
    case TInt64 =>
      (off, v) => region.storeInt64(off, v.asInstanceOf[Code[Long]])
    case TFloat32 =>
      (off, v) => region.storeFloat32(off, v.asInstanceOf[Code[Float]])
    case TFloat64 =>
      (off, v) => region.storeFloat64(off, v.asInstanceOf[Code[Double]])
    case _ =>
      (off, ptr) => region.storeAddress(off, ptr.asInstanceOf[Code[Long]])
  }

  type E = Env[(TypeInfo[_], Code[Boolean], Code[_])]

  def apply(ir: IR, fb: FunctionBuilder[_]) {
    apply(ir, fb, new Env())
  }

  def apply(ir: IR, fb: FunctionBuilder[_], env: E) {
    fb.emit(expression(ir, fb, env, new StagedBitSet(fb))._2)
  }

  private def present(x: Code[_]) = (const(false), x)
  private def coerce[T](x: Code[_]): Code[T] = x.asInstanceOf[Code[T]]
  private def tcoerce[T <: expr.Type](x: expr.Type): T = x.asInstanceOf[T]

  def expression(ir: IR, fb: FunctionBuilder[_], env: E, mb: StagedBitSet): (Code[Boolean], Code[_]) = {
    val region = fb.getArg[MemoryBuffer](1).load()
    def expression(ir: IR, fb: FunctionBuilder[_] = fb, env: E = env, mb: StagedBitSet = mb): (Code[Boolean], Code[_]) =
      Compile2.expression(ir, fb, env, mb)
    ir match {
      case I32(x) =>
        present(const(x))
      case I64(x) =>
        present(const(x))
      case F32(x) =>
        present(const(x))
      case F64(x) =>
        present(const(x))
      case True() =>
        present(const(true))
      case False() =>
        present(const(false))

      case NA(typ) =>
        (const(true), dummyValue(typ))
      case IsNA(v) =>
        present(expression(v)._1)
      case MapNA(name, value, body, typ) =>
        val vti = typeToTypeInfo(value.typ)
        val bti = typeToTypeInfo(typ)
        val (mvalue, vvalue) = expression(value)
        val mx = mb.newBit()
        fb.emit(mx := mvalue)
        val x = fb.newLocal(name)(vti).asInstanceOf[LocalRef[Any]]
        fb.emit(x := mx.mux(dummyValue(value.typ), vvalue))
        val bodyenv = env.bind(name -> (vti, mx, x))
        val (mbody, vbody) = expression(body, env = bodyenv)
        (mvalue || mbody, mvalue.mux(dummyValue(typ), vbody))

      case expr.ir.If(cond, cnsq, altr, typ) =>
        val (mcond, vcond) = expression(cond)
        val xvcond = mb.newBit()
        fb.emit(xvcond := coerce[Boolean](vcond))
        val (mcnsq, vcnsq) = expression(cnsq)
        val (maltr, valtr) = expression(altr)

        (mcond || (xvcond && mcnsq) || (!xvcond && maltr),
          xvcond.asInstanceOf[Code[Boolean]].mux(vcnsq, valtr))

      case expr.ir.Let(name, value, body, typ) =>
        val vti = typeToTypeInfo(value.typ)
        fb.newLocal(name)(vti) match { case x: LocalRef[v] =>
          val (mvalue, vvalue) = expression(value)
          val xmvalue = mb.newBit()
          fb.emit(xmvalue := mvalue)
          fb.emit(x := coerce[v](vvalue))
          val bodyenv = env.bind(name -> (vti, xmvalue, x))
          expression(body, env = bodyenv)
        }
      case Ref(name, typ) =>
        val ti = typeToTypeInfo(typ)
        val (t, m, v) = env.lookup(name)
        assert(t == ti, s"$name type annotation, $typ, doesn't match typeinfo: $ti")
        (m, v)

      case ApplyPrimitive(op, args, typ) =>
        val typs = args.map(_.typ)
        val (margs, vargs) = args.map(expression(_)).unzip
        val m = if (margs.isEmpty) const(false) else margs.reduce(_ || _)
        (m, Primitives.lookup(op, typs, vargs))
      case LazyApplyPrimitive(op, args, typ) =>
        ???
      case expr.ir.Lambda(names, body, typ) =>
        ???
      case MakeArray(args, typ) =>
        val srvb = new StagedRegionValueBuilder(fb, typ)
        val addElement = srvb.addAnnotation(typ.elementType)
        val mvargs = args.map(expression(_))

        present(Code(
          srvb.start(args.length, init = true),
          Code(mvargs.map { case (m, v) =>
            Code(m.mux(srvb.setMissing(), addElement(v)), srvb.advance())
          }: _*),
          srvb.offset))
      case x@MakeArrayN(len, elementType) =>
        val srvb = new StagedRegionValueBuilder(fb, x.typ)
        val (mlen, vlen) = expression(len)

        (mlen, mlen.mux(
          dummyValue(x.typ),
          Code(srvb.start(coerce[Int](vlen), init = true),
            srvb.offset)))
      case ArrayRef(a, i, typ) =>
        val ti = typeToTypeInfo(typ)
        val tarray = TArray(typ)
        val ati = typeToTypeInfo(tarray).asInstanceOf[TypeInfo[Long]]
        val (ma, va) = expression(a)
        val (mi, vi) = expression(i)

        val xma = mb.newBit()
        val xa = fb.newLocal()(ati)
        val xi = fb.newLocal[Int]
        val xmi = mb.newBit()
        val xmv = mb.newBit()
        fb.emit(Code(
          xma := ma,
          xa := coerce[Long](xma.mux(dummyValue(tarray), va)),
          xmi := mi,
          xi := coerce[Int](xmi.mux(dummyValue(TInt32), vi)),
          xmv := xma || xmi || !tarray.isElementDefined(region, xa, xi)))

        (xmv, xmv.mux(dummyValue(typ), loadAnnotation(region, typ)(tarray.loadElement(region, xa, xi))))
      case ArrayMissingnessRef(a, i) =>
        val tarray = tcoerce[TArray](a.typ)
        val ati = typeToTypeInfo(tarray).asInstanceOf[TypeInfo[Long]]
        val (ma, va) = expression(a)
        val (mi, vi) = expression(i)


        val xma = mb.newBit()
        val xa = fb.newLocal()(ati)
        val xi = fb.newLocal[Int]
        val xmi = mb.newBit()
        val xmv = mb.newBit()
        fb.emit(Code(
          xma := ma,
          xa := coerce[Long](xma.mux(dummyValue(tarray), va)),
          xmi := mi,
          xi := coerce[Int](xmi.mux(dummyValue(TInt32), vi))))

        present(xma || xmi || !tarray.isElementDefined(region, xa, xi))
      case ArrayLen(a) =>
        val (ma, va) = expression(a)
        (ma, TContainer.loadLength(region, coerce[Long](va)))
      case x@ArrayMap(a, Lambda(Array((name,_)), body, _), elementTyp) =>
        val tin = a.typ.asInstanceOf[TArray]
        val tout = x.typ.asInstanceOf[TArray]
        val srvb = new StagedRegionValueBuilder(fb, tout)
        val addElement = srvb.addAnnotation(tout.elementType)
        typeToTypeInfo(elementTyp) match { case eti: TypeInfo[t] =>
          val xma = mb.newBit()
          val xa = fb.newLocal[Long]("am_a")
          val xmv = mb.newBit()
          val xvv = fb.newLocal(name)(eti)
          val i = fb.newLocal[Int]("am_i")
          val len = fb.newLocal[Int]("am_len")
          val out = fb.newLocal[Long]("am_out")
          val bodyenv = env.bind(name -> (eti, xmv, xvv))

          val lmissing = new LabelNode()
          val lnonmissing = new LabelNode()

          val ltop = new LabelNode()
          val lnext = new LabelNode()
          val lend = new LabelNode()

          val (ma, va) = expression(a)
          fb.emit(xma := ma)
          fb.emit(xvv := coerce[t](dummyValue(elementTyp)))
          fb.emit(out := coerce[Long](dummyValue(tout)))
          xma.toConditional.emitConditional(fb.l, lmissing, lnonmissing)
          fb.emit(Code(
            lnonmissing,
            xa := coerce[Long](va),
            len := TContainer.loadLength(region, xa),
            i := 0,
            srvb.start(len, init = true),
            ltop))
          (i < len).toConditional.emitConditional(fb.l, lnext, lend)
          fb.emit(Code(
            lnext,
            xmv := !tin.isElementDefined(region, xa, i),
            xvv := coerce[t](xmv.mux(
              dummyValue(elementTyp),
              loadAnnotation(region, tin.elementType)(
                tin.loadElement(region, xa, i))))))
          val (mbody, vbody) = expression(body, env = bodyenv)
          fb.emit(Code(
            mbody.mux(
              srvb.setMissing(),
              addElement(vbody)),
            srvb.advance(),
            i := i + 1,
            new JumpInsnNode(GOTO, ltop),
            lend,
            out := srvb.offset,
            lmissing))

          (xma, out.load())
        }
      case ArrayMap(_, _, _) =>
        throw new UnsupportedOperationException(s"bad arraymap $ir")
      case ArrayFold(a, zero, Lambda(Array((name1, _), (name2, _)), body, _), typ) =>
        val tarray = a.typ.asInstanceOf[TArray]
        assert(tarray != null, s"tarray is null! $ir")

        (typeToTypeInfo(typ), typeToTypeInfo(tarray.elementType)) match { case (tti: TypeInfo[t], uti: TypeInfo[u]) =>
          val xma = mb.newBit()
          val xa = fb.newLocal[Long]("af_array")
          val xmv = mb.newBit()
          val xvv = fb.newLocal(name2)(uti)
          val xmout = mb.newBit()
          val xvout = fb.newLocal(name1)(tti)
          val i = fb.newLocal[Int]("af_i")
          val len = fb.newLocal[Int]("af_len")
          val bodyenv = env.bind(
            name1 -> (tti, xmout, xvout.load()),
            name2 -> (uti, xmv, xvv.load()))

          val lmissing = new LabelNode()
          val lnonmissing = new LabelNode()

          val ltop = new LabelNode()
          val lnext = new LabelNode()
          val lend = new LabelNode()

          val (ma, va) = expression(a)
          fb.emit(xma := ma)
          fb.emit(xvout := coerce[t](dummyValue(typ)))
          fb.emit(xvv := coerce[u](dummyValue(tarray.elementType)))
          xma.toConditional.emitConditional(fb.l, lmissing, lnonmissing)
          fb.emit(Code(
            lnonmissing,
            xa := coerce[Long](va),
            len := TContainer.loadLength(region, xa),
            i := 0))
          val (mzero, vzero) = expression(zero)
          fb.emit(Code(
            xmout := mzero,
            xvout := coerce[t](xmout.mux(dummyValue(typ), vzero)),
            ltop))
          (i < len).toConditional.emitConditional(fb.l, lnext, lend)
          fb.emit(Code(
            lnext,
            xmv := !tarray.isElementDefined(region, xa, i),
            xvv := coerce[u](xmv.mux(
              dummyValue(tarray.elementType),
              loadAnnotation(region, tarray.elementType)(
                tarray.loadElement(region, xa, i))))))
          val (mbody, vbody) = expression(body, env = bodyenv)
          fb.emit(Code(
            xmout := mbody,
            xvout := coerce[t](xmout.mux(dummyValue(typ), vbody)),
            i := i + 1,
            new JumpInsnNode(GOTO, ltop),
            lend,
            lmissing))
          (xmout, xvout)
        }
      case ArrayFold(_,  _, _, _) =>
        throw new UnsupportedOperationException(s"bad arrayfold $ir")
      case MakeStruct(fields, missingness) =>
        assert(missingness != null, "run explicit missingness first")
        val t = TStruct(fields.map { case (name, t, _) => (name, t) }: _*)
        val initializers = fields.map { case (_, t, v) => (t, expression(v)) }
        val srvb = new StagedRegionValueBuilder(fb, t)

        present(Code(
          srvb.start(false),
          Code(initializers.map { case (t, (mv, vv)) =>
            Code(
              mv.mux(srvb.setMissing(), srvb.addAnnotation(t)(vv)),
              srvb.advance()) }: _*),
          srvb.offset))
      case GetField(o, name, _) =>
        val t = o.typ.asInstanceOf[TStruct]
        val fieldIdx = t.fieldIdx(name)
        val (mo, vo) = expression(o)
        val xmo = mb.newBit()
        val xo = fb.newLocal[Long]
        fb.emit(xmo := mo)
        fb.emit(xo := coerce[Long](xmo.mux(dummyValue(t), vo)))
        (xmo || !t.isFieldDefined(region, xo, fieldIdx),
          loadAnnotation(region, t)(t.fieldOffset(xo, fieldIdx)))
      case GetFieldMissingness(o, name) =>
        val t = o.typ.asInstanceOf[TStruct]
        val fieldIdx = t.fieldIdx(name)
        val (mo, vo) = expression(o)
        val xmo = mb.newBit()
        val xo = fb.newLocal[Long]
        fb.emit(xmo := mo)
        fb.emit(xo := coerce[Long](xmo.mux(dummyValue(t), vo)))
        (xmo, !t.isFieldDefined(region, xo, fieldIdx))
      case Seq(stmts, typ) =>
        ???
      case In(i, typ) =>
        (fb.getArg[Boolean](i*2 + 3), fb.getArg(i*2 + 2)(typeToTypeInfo(typ)))
      case InMissingness(i) =>
        present(fb.getArg[Boolean](i*2 + 3))
      case Out(v) =>
        val (mv, vv) = expression(v)
        typeToTypeInfo(v.typ) match { case ti: TypeInfo[t] =>
          present(mv.mux(
            Code._throw(Code.newInstance[RuntimeException, String]("cannot return empty")),
            Code._return(coerce[t](vv))(ti)))
        }
      case Die(m) =>
        present(Code._throw(Code.newInstance[RuntimeException, String](m)))
    }
  }
}
