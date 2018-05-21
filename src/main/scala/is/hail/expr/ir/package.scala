package is.hail.expr

import is.hail.asm4s
import is.hail.asm4s._
import is.hail.expr.ir.functions.IRFunctionRegistry
import is.hail.expr.types._

import scala.language.implicitConversions

package object ir {
  var uidCounter: Long = 0

  def genUID(): String = {
    val uid = s"__iruid_$uidCounter"
    uidCounter += 1
    uid
  }

  def typeToTypeInfo(t: Type): TypeInfo[_] = t.fundamentalType match {
    case _: TInt32 => typeInfo[Int]
    case _: TInt64 => typeInfo[Long]
    case _: TFloat32 => typeInfo[Float]
    case _: TFloat64 => typeInfo[Double]
    case _: TBoolean => typeInfo[Boolean]
    case _: TBinary => typeInfo[Long]
    case _: TArray => typeInfo[Long]
    case _: TBaseStruct => typeInfo[Long]
    case TVoid => typeInfo[Unit]
    case _ => throw new RuntimeException(s"unsupported type found, $t")
  }

  def defaultValue(t: Type): Code[_] = typeToTypeInfo(t) match {
    case UnitInfo => Code._empty[Unit]
    case BooleanInfo => false
    case IntInfo => 0
    case LongInfo => 0L
    case FloatInfo => 0.0f
    case DoubleInfo => 0.0
    case ti => throw new RuntimeException(s"unsupported type found: $t whose type info is $ti")
  }

  // Build consistent expression for a filter-condition with keep polarity,
  // using Let to manage missing-ness.
  def filterPredicateWithKeep(irPred: ir.IR, keep: Boolean): ir.IR = {
    val pred = genUID()
    ir.Let(pred,
      if (keep) irPred else ir.ApplyUnaryPrimOp(ir.Bang(), irPred),
      ir.If(ir.IsNA(ir.Ref(pred, TBoolean())),
        ir.False(),
        ir.Ref(pred, TBoolean())))
  }

  private[ir] def coerce[T](c: Code[_]): Code[T] = asm4s.coerce(c)

  private[ir] def coerce[T](lr: Settable[_]): Settable[T] = lr.asInstanceOf[Settable[T]]

  private[ir] def coerce[T](ti: TypeInfo[_]): TypeInfo[T] = ti.asInstanceOf[TypeInfo[T]]

  private[ir] def coerce[T <: Type](x: Type): T = types.coerce[T](x)

  def invoke(name: String, args: IR*): IR =
    IRFunctionRegistry.lookupConversion(name, args.map(_.typ)).get(args)

  case class IRBinding(v: String, t: Type)

  implicit def irBindingToIR(b: IRBinding): IR = Ref(b.v, b.t)

  def bind(x: IR, f: (IRBinding) => IR): IR = {
    val v = genUID()
    val b = IRBinding(v, x.typ)
    Let(v, x, f(b))
  }

  def bind(x1: IR, x2: IR, f: (IRBinding, IRBinding) => IR): IR = {
    val v1 = genUID()
    val v2 = genUID()
    val b1 = IRBinding(v1, x1.typ)
    val b2 = IRBinding(v2, x2.typ)
    Let(v1, x1, Let(v2, x2, f(b1, b2)))
  }

  def nonstrictEQ(l: IR, r: IR): IR = {
    // FIXME better as a (non-strict) BinaryOp?
    assert(l.typ == r.typ)
    bind(l, r, (lx, rx) =>
      If(IsNA(lx),
        IsNA(rx),
        If(IsNA(rx),
          False(),
          ApplyComparisonOp(EQ(l.typ), lx, rx))))
  }
}
