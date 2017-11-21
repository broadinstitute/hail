package is.hail.expr.ir

import is.hail.annotations._
import is.hail.asm4s._
import is.hail.expr
import is.hail.expr.{Type, Aggregator, TAggregable, TArray, TBoolean, TContainer, TFloat32, TFloat64, TInt32, TInt64, TStruct}
import is.hail.utils._
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.tree._

import scala.language.existentials

object ExtractAggregators {

  private case class IRAgg(in: In, t: Type, z: IR, seq: (IR, IR) => IR, comb: (IR, IR) => IR) { }

  trait Aggregable {
    def aggregate(zero: Long, seq: (Long, Long) => Long, comb: (Long, Long) => Long): Long
  }

  def apply(ir: IR, t: TAggregable): (IR, TStruct, (MemoryBuffer, Aggregable) => Long) = {
    val (ir2, aggs) = extract(ir)
    val fields = aggs.map(_.t).zipWithIndex.map { case (t, i) => (i.toString -> t) }
    val tT: TStruct = TStruct("x" -> t.elementType, t.bindingTypes:_*)
    val tU: TStruct = TStruct(fields:_*)
    def zipValues(irs: Iterable[IR]): IR =
      MakeStruct(fields.zip(irs).map { case ((n, t), v) => (n, t, v) })
    val zeroFb = FunctionBuilder.functionBuilder[Long]
    Compile(zipValues(aggs.map(_.z)), zeroFb)
    val seqFb = FunctionBuilder.functionBuilder[Long, Long, Long]
    Compile(zipValues(aggs.zipWithIndex.map { case (x, i) =>
      x.seq(GetField(In(0, tU), i.toString()), In(1, tT)) }), seqFb)
    val combFb = FunctionBuilder.functionBuilder[Long, Long, Long]
    Compile(zipValues(aggs.zipWithIndex.map { case (x, i) =>
      x.comb(GetField(In(0, tU), i.toString()), GetField(In(0, tU), i.toString())) }), combFb)

    // update all the references to the intermedaite
    aggs.map(_.in).foreach(_.typ = tU)

    val zero = zeroFb.result()
    val seq = seqFb.result()
    val comb = combFb.result()

    (ir2, tU, { (r, agg) =>
      // load classes into JVM
      val f = seq()
      val g = comb()
      agg.aggregate(zero()(), (t, u) => f(t, u), (l, r) => g(l, r))
    })
  }

  private def extract(ir: IR): (IR, Array[IRAgg]) = {
    val ab = new ArrayBuilder[IRAgg]()
    val ir2 = extract(ir, ab)
    (ir2, ab.result())
  }

  private def extract(ir: IR, ab: ArrayBuilder[IRAgg]): IR = {
    def extract(ir: IR): IR = this.extract(ir, ab)
    ir match {
      case I32(x) => ir
      case I64(x) => ir
      case F32(x) => ir
      case F64(x) => ir
      case True() => ir
      case False() => ir
      case Cast(v, typ) =>
        extract(v)
      case NA(typ) => ir
      case MapNA(name, value, body, typ) =>
        MapNA(name, extract(value), extract(body), typ)
      case IsNA(value) =>
        IsNA(extract(value))
      case If(cond, cnsq, altr, typ) =>
        If(extract(cond), extract(cnsq), extract(altr), typ)
      case Let(name, value, body, typ) =>
        Let(name, extract(value), extract(body), typ)
      case Ref(name, typ) =>
        assert(typ.isRealizable)
        Ref(name, typ)
      case ApplyBinaryPrimOp(op, l, r, typ) =>
        ApplyBinaryPrimOp(op, extract(l), extract(r), typ)
      case ApplyUnaryPrimOp(op, x, typ) =>
        ApplyUnaryPrimOp(op, extract(x), typ)
      case MakeArray(args, typ) =>
        MakeArray(args map extract, typ)
      case MakeArrayN(len, elementType) =>
        MakeArrayN(extract(len), elementType)
      case ArrayRef(a, i, typ) =>
        ArrayRef(extract(a), extract(i), typ)
      case ArrayMissingnessRef(a, i) =>
        ArrayMissingnessRef(extract(a), extract(i))
      case ArrayLen(a) =>
        ArrayLen(extract(a))
      case ArrayMap(a, name, body, elementTyp) =>
        ArrayMap(extract(a), name, noAgg(body), elementTyp)
      case ArrayFold(a, zero, accumName, valueName, body, typ) =>
        ArrayFold(extract(a), extract(zero), accumName, valueName, noAgg(body), typ)
      case AggMap(a, name, body, typ) =>
        throw new RuntimeException(s"AggMap must be used inside an AggSum, but found: $ir")
      case x@AggSum(a) =>
        val in = In(0, null)
        ab += IRAgg(in, x.typ, zeroValue(x.typ), (u, t) => ApplyBinaryPrimOp(Add(), u, lower(a, t)), (l, r) => ApplyBinaryPrimOp(Add(), l, r))
        GetField(in, ab.length.toString())
      case MakeStruct(fields) =>
        MakeStruct(fields map { case (x,y,z) => (x,y,extract(z)) })
      case GetField(o, name, typ) =>
        GetField(extract(o), name, typ)
      case GetFieldMissingness(o, name) =>
        GetFieldMissingness(extract(o), name)
      case In(i, typ) =>
        In(i, typ)
      case InMissingness(i) =>
        InMissingness(i)
      case Die(message) =>
        Die(message)
    }
  }

  private def lower(ir: IR, aggIn: IR): IR = {
    def lower(ir: IR): IR = this.lower(ir, aggIn)
    ir match {
      case I32(x) => ir
      case I64(x) => ir
      case F32(x) => ir
      case F64(x) => ir
      case True() => ir
      case False() => ir
      case Cast(v, typ) =>
        Cast(lower(v), typ)
      case NA(typ) => ir
      case MapNA(name, value, body, typ) =>
        MapNA(name, lower(value), lower(body), typ)
      case IsNA(value) =>
        IsNA(lower(value))
      case If(cond, cnsq, altr, typ) =>
        If(lower(cond), lower(cnsq), lower(altr), typ)
      case Let(name, value, body, typ) =>
        Let(name, lower(value), lower(body), typ)
      case Ref(name, typ) => ir
      case ApplyBinaryPrimOp(op, l, r, typ) =>
        ApplyBinaryPrimOp(op, lower(l), lower(r), typ)
      case ApplyUnaryPrimOp(op, x, typ) =>
        ApplyUnaryPrimOp(op, lower(x), typ)
      case MakeArray(args, typ) =>
        MakeArray(args map lower, typ)
      case MakeArrayN(len, elementType) =>
        MakeArrayN(lower(len), elementType)
      case ArrayRef(a, i, typ) =>
        ArrayRef(lower(a), lower(i), typ)
      case ArrayMissingnessRef(a, i) =>
        ArrayMissingnessRef(lower(a), lower(i))
      case ArrayLen(a) =>
        ArrayLen(lower(a))
      case ArrayMap(a, name, body, elementTyp) =>
        ArrayMap(lower(a), name, lower(body), elementTyp)
      case ArrayFold(a, zero, accumName, valueName, body, typ) =>
        ArrayFold(lower(a), lower(zero), accumName, valueName, lower(body), typ)
      case AggIn(typ) =>
        aggIn
      case AggMap(a, name, body, typ) =>
        Let(name, lower(a), lower(body), typ)
      case AggSum(a) =>
        throw new RuntimeException(s"Found aggregator inside an aggregator: $ir")
      case MakeStruct(fields) =>
        MakeStruct(fields.map { case (x,y,z) => (x,y,lower(z)) })
      case GetField(o, name, typ) =>
        GetField(lower(o), name, typ)
      case GetFieldMissingness(o, name) =>
        GetFieldMissingness(lower(o), name)
      case In(i, typ) =>
        throw new RuntimeException(s"Referenced input inside an aggregator, that's a no-no: $ir")
      case InMissingness(i) =>
        throw new RuntimeException(s"Referenced input inside an aggregator, that's a no-no: $ir")
      case Die(message) => ir
    }
  }

  private def noAgg(ir: IR): IR = {
    // FIXME: assert no children reference AggMap or AggSum
    ir
  }

  private def zeroValue(t: Type): IR = {
    t match {
      case _: TBoolean => False()
      case _: TInt32 => I32(0)
      case _: TInt64 => I64(0L)
      case _: TFloat32 => F32(0.0f)
      case _: TFloat64 => F64(0.0)
    }
  }
}
