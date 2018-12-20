package is.hail.expr.ir

import is.hail.annotations.Annotation
import is.hail.expr.types._
import is.hail.expr.ir.functions._
import is.hail.expr.types.physical.{PStruct, PType}
import is.hail.expr.types.virtual._
import is.hail.utils.{ExportType, FastIndexedSeq}

import scala.language.existentials

sealed trait IR extends BaseIR {
  def typ: Type

  def pType: PType = PType.canonical(typ)

  override def children: IndexedSeq[BaseIR] =
    Children(this)

  override def copy(newChildren: IndexedSeq[BaseIR]): IR =
    Copy(this, newChildren)

  def size: Int = 1 + children.map {
      case x: IR => x.size
      case _ => 0
    }.sum

  private[this] def _unwrap: IR => IR = {
    case node: ApplyIR => MapIR(_unwrap)(node.explicitNode)
    case node => MapIR(_unwrap)(node)
  }

  def unwrap: IR = _unwrap(this)
}

object Literal {
  def coerce(t: Type, x: Any): IR = {
    if (x == null)
      return NA(t)
    t match {
      case _: TInt32 => I32(x.asInstanceOf[Int])
      case _: TInt64 => I64(x.asInstanceOf[Long])
      case _: TFloat32 => F32(x.asInstanceOf[Float])
      case _: TFloat64 => F64(x.asInstanceOf[Double])
      case _: TBoolean => if (x.asInstanceOf[Boolean]) True() else False()
      case _: TString => Str(x.asInstanceOf[String])
      case _ => Literal(t, x)
    }
  }
}

final case class Literal(typ: Type, value: Annotation) extends IR {
  require(!CanEmit(typ))
  require(value != null)
}

sealed trait InferIR extends IR {
  var _ptype: PType = null

  override def pType: PType = {
    if (_ptype == null)
      _ptype = Infer(this)
    _ptype
  }

  def typ: Type = pType.virtualType
}

final case class I32(x: Int) extends IR { val typ = TInt32() }
final case class I64(x: Long) extends IR { val typ = TInt64() }
final case class F32(x: Float) extends IR { val typ = TFloat32() }
final case class F64(x: Double) extends IR { val typ = TFloat64() }
final case class Str(x: String) extends IR { val typ = TString() }
final case class True() extends IR { val typ = TBoolean() }
final case class False() extends IR { val typ = TBoolean() }
final case class Void() extends IR { val typ = TVoid }

final case class Cast(v: IR, typ: Type) extends IR

final case class NA(typ: Type) extends IR { assert(!typ.required) }
final case class IsNA(value: IR) extends IR { val typ = TBoolean() }

object If {
  def unify(cond: IR, cnsq: IR, altr: IR, unifyType: Option[Type] = None): If = {
    if (cnsq.typ == altr.typ)
      If(cond, cnsq, altr)
    else {
      val t = unifyType.getOrElse(cnsq.typ.deepOptional())
      If(cond,
        PruneDeadFields.upcast(cnsq, t),
        PruneDeadFields.upcast(altr, t))
    }
  }
}

final case class If(cond: IR, cnsq: IR, altr: IR) extends InferIR

final case class Let(name: String, value: IR, body: IR) extends InferIR
final case class Ref(name: String, var typ: Type) extends IR

final case class ApplyBinaryPrimOp(op: BinaryOp, l: IR, r: IR) extends InferIR
final case class ApplyUnaryPrimOp(op: UnaryOp, x: IR) extends InferIR
final case class ApplyComparisonOp(op: ComparisonOp, l: IR, r: IR) extends InferIR

object MakeArray {
  def unify(args: Seq[IR], typ: TArray = null): MakeArray = {
    assert(typ != null || args.nonEmpty)
    var t = typ
    if (t == null) {
      t = if (args.tail.forall(_.typ == args.head.typ)) {
        TArray(args.head.typ)
      } else TArray(args.head.typ.deepOptional())
    }
    assert(t.elementType.deepOptional() == t.elementType ||
      args.forall(a => a.typ == t.elementType),
      s"${ t.parsableString() }: ${ args.map(a => "\n    " + a.typ.parsableString()).mkString } ")

    MakeArray(args.map { arg =>
      if (arg.typ == t.elementType)
        arg
      else {
        val upcast = PruneDeadFields.upcast(arg, t.elementType)
        assert(upcast.typ == t.elementType)
        upcast
      }
    }, t)
  }
}

final case class MakeArray(args: Seq[IR], typ: TArray) extends IR {
}

final case class ArrayRef(a: IR, i: IR) extends InferIR
final case class ArrayLen(a: IR) extends IR { val typ = TInt32() }
final case class ArrayRange(start: IR, stop: IR, step: IR) extends IR { val typ: TArray = TArray(TInt32()) }

final case class ArraySort(a: IR, ascending: IR, onKey: Boolean = false) extends InferIR
final case class ToSet(a: IR) extends InferIR
final case class ToDict(a: IR) extends InferIR
final case class ToArray(a: IR) extends InferIR

final case class LowerBoundOnOrderedCollection(orderedCollection: IR, elem: IR, onKey: Boolean) extends IR { val typ: Type = TInt32() }

final case class GroupByKey(collection: IR) extends InferIR

final case class ArrayMap(a: IR, name: String, body: IR) extends InferIR {
  override def typ: TArray = coerce[TArray](super.typ)
  def elementTyp: Type = typ.elementType
}
final case class ArrayFilter(a: IR, name: String, cond: IR) extends InferIR {
  override def typ: TArray = super.typ.asInstanceOf[TArray]
}
final case class ArrayFlatMap(a: IR, name: String, body: IR) extends InferIR {
  override def typ: TArray = coerce[TArray](super.typ)
}
final case class ArrayFold(a: IR, zero: IR, accumName: String, valueName: String, body: IR) extends InferIR

final case class ArrayScan(a: IR, zero: IR, accumName: String, valueName: String, body: IR) extends InferIR

final case class ArrayFor(a: IR, valueName: String, body: IR) extends IR {
  val typ = TVoid
}

final case class AggFilter(cond: IR, aggIR: IR) extends InferIR

final case class AggExplode(array: IR, name: String, aggBody: IR) extends InferIR

final case class AggGroupBy(key: IR, aggIR: IR) extends InferIR

final case class ApplyAggOp(constructorArgs: IndexedSeq[IR], initOpArgs: Option[IndexedSeq[IR]], seqOpArgs: IndexedSeq[IR], aggSig: AggSignature) extends InferIR {
  assert(!(seqOpArgs ++ constructorArgs ++ initOpArgs.getOrElse(FastIndexedSeq.empty[IR])).exists(ContainsScan(_)))
  assert(constructorArgs.map(_.typ) == aggSig.constructorArgs)
  assert(initOpArgs.map(_.map(_.typ)) == aggSig.initOpArgs)

  def nSeqOpArgs = seqOpArgs.length

  def nConstructorArgs = constructorArgs.length

  def hasInitOp = initOpArgs.isDefined

  def op: AggOp = aggSig.op
}

final case class ApplyScanOp(constructorArgs: IndexedSeq[IR], initOpArgs: Option[IndexedSeq[IR]], seqOpArgs: IndexedSeq[IR], aggSig: AggSignature) extends InferIR {
  assert(!(seqOpArgs ++ constructorArgs ++ initOpArgs.getOrElse(FastIndexedSeq.empty[IR])).exists(ContainsAgg(_)))
  assert(constructorArgs.map(_.typ) == aggSig.constructorArgs)
  assert(initOpArgs.map(_.map(_.typ)) == aggSig.initOpArgs)

  def nSeqOpArgs = seqOpArgs.length

  def nConstructorArgs = constructorArgs.length

  def hasInitOp = initOpArgs.isDefined

  def op: AggOp = aggSig.op
}

final case class InitOp(i: IR, args: IndexedSeq[IR], aggSig: AggSignature) extends IR {
  val typ = TVoid
}
final case class SeqOp(i: IR, args: IndexedSeq[IR], aggSig: AggSignature) extends IR {
  val typ = TVoid
}

final case class Begin(xs: IndexedSeq[IR]) extends IR {
  val typ = TVoid
}

final case class MakeStruct(fields: Seq[(String, IR)]) extends InferIR
final case class SelectFields(old: IR, fields: Seq[String]) extends InferIR
final case class InsertFields(old: IR, fields: Seq[(String, IR)]) extends InferIR {
  override def typ: TStruct = coerce[TStruct](super.typ)

  override def pType: PStruct = coerce[PStruct](super.pType)
}

final case class GetField(o: IR, name: String) extends InferIR

final case class MakeTuple(types: Seq[IR]) extends InferIR
final case class GetTupleElement(o: IR, idx: Int) extends InferIR

final case class StringSlice(s: IR, start: IR, end: IR) extends IR {
  val typ = TString()
}
final case class StringLength(s: IR) extends IR {
  val typ = TInt32()
}

final case class In(i: Int, typ: Type) extends IR
// FIXME: should be type any

object Die {
  def apply(message: String, typ: Type): Die = Die(Str(message), typ)
}

final case class Die(message: IR, typ: Type) extends IR

final case class ApplyIR(function: String, args: Seq[IR], conversion: Seq[IR] => IR) extends IR {
  lazy val explicitNode: IR = {
    val refs = args.map(a => Ref(genUID(), a.typ)).toArray
    var body = conversion(refs)

    // foldRight because arg1 should be at the top so it is evaluated first
    refs.zip(args).foldRight(body) { case ((ref, arg), bodyIR) => Let(ref.name, arg, bodyIR) }
  }

  def typ: Type = explicitNode.typ
}

sealed abstract class AbstractApplyNode[F <: IRFunction] extends IR {
  def function: String
  def args: Seq[IR]
  def argTypes: Seq[Type] = args.map(_.typ)
  lazy val implementation: F = IRFunctionRegistry.lookupFunction(function, argTypes).get.asInstanceOf[F]
  def typ: Type = {
    // convert all arg types before unifying
    val argTypes = args.map(_.typ)
    implementation.unify(argTypes)
    implementation.returnType.subst()
  }

}

final case class Apply(function: String, args: Seq[IR]) extends AbstractApplyNode[IRFunctionWithoutMissingness]

final case class ApplySeeded(function: String, args: Seq[IR], seed: Long) extends AbstractApplyNode[SeededIRFunction]

final case class ApplySpecial(function: String, args: Seq[IR]) extends AbstractApplyNode[IRFunctionWithMissingness]

final case class Uniroot(argname: String, function: IR, min: IR, max: IR) extends IR { val typ: Type = TFloat64() }

final case class TableCount(child: TableIR) extends IR { val typ: Type = TInt64() }
final case class TableAggregate(child: TableIR, query: IR) extends InferIR
final case class MatrixAggregate(child: MatrixIR, query: IR) extends InferIR
final case class TableWrite(
  child: TableIR,
  path: String,
  overwrite: Boolean = true,
  stageLocally: Boolean = false,
  codecSpecJSONStr: String = null) extends IR {
  val typ: Type = TVoid
}
final case class TableExport(
  child: TableIR,
  path: String,
  typesFile: String = null,
  header: Boolean = true,
  exportType: Int = ExportType.CONCATENATED) extends IR {
  val typ: Type = TVoid
}

final case class MatrixWrite(
  child: MatrixIR,
  writer: MatrixWriter) extends IR {
  val typ: Type = TVoid
}

final case class TableGetGlobals(child: TableIR) extends InferIR
final case class TableCollect(child: TableIR) extends InferIR

class PrimitiveIR(val self: IR) extends AnyVal {
  def +(other: IR): IR = ApplyBinaryPrimOp(Add(), self, other)
  def -(other: IR): IR = ApplyBinaryPrimOp(Subtract(), self, other)
  def *(other: IR): IR = ApplyBinaryPrimOp(Multiply(), self, other)
  def /(other: IR): IR = ApplyBinaryPrimOp(FloatingPointDivide(), self, other)
  def floorDiv(other: IR): IR = ApplyBinaryPrimOp(RoundToNegInfDivide(), self, other)

  def &&(other: IR): IR = invoke("&&", self, other)
  def ||(other: IR): IR = invoke("||", self, other)

  def toI: IR = Cast(self, TInt32())
  def toL: IR = Cast(self, TInt64())
  def toF: IR = Cast(self, TFloat32())
  def toD: IR = Cast(self, TFloat64())

  def unary_-(): IR = ApplyUnaryPrimOp(Negate(), self)
  def unary_!(): IR = ApplyUnaryPrimOp(Bang(), self)

  def ceq(other: IR): IR = ApplyComparisonOp(EQWithNA(self.typ, other.typ), self, other)
  def cne(other: IR): IR = ApplyComparisonOp(NEQWithNA(self.typ, other.typ), self, other)
  def <(other: IR): IR = ApplyComparisonOp(LT(self.typ, other.typ), self, other)
  def >(other: IR): IR = ApplyComparisonOp(GT(self.typ, other.typ), self, other)
  def <=(other: IR): IR = ApplyComparisonOp(LTEQ(self.typ, other.typ), self, other)
  def >=(other: IR): IR = ApplyComparisonOp(GTEQ(self.typ, other.typ), self, other)
}
