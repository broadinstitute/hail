package is.hail.expr.ir

import is.hail.expr.types._
import is.hail.expr.types.virtual._

import scala.language.{dynamics, implicitConversions}

object IRBuilder {
  type E = Env[Type]

  implicit def funcToIRProxy(ir: E => IR): IRProxy = new IRProxy(ir)

  implicit def tableIRToProxy(tir: TableIR): TableIRProxy =
    new TableIRProxy(tir)

  implicit def irToProxy(ir: IR): IRProxy = (_: E) => ir

  implicit def intToProxy(i: Int): IRProxy = I32(i)

  implicit def booleanToProxy(b: Boolean): IRProxy = if (b) True() else False()

  implicit def ref(s: Symbol): IRProxy = (env: E) =>
    Ref(s.name, env.lookup(s.name))

  implicit def symbolToSymbolProxy(s: Symbol): SymbolProxy = new SymbolProxy(s)

  implicit def arrayToProxy(seq: Seq[IRProxy]): IRProxy = (env: E) => {
    val irs = seq.map(_(env))
    val elType = irs.head.typ
    MakeArray(irs, TArray(elType))
  }

  implicit def arrayIRToProxy(seq: Seq[IR]): IRProxy = arrayToProxy(seq.map(irToProxy))

  def irRange(start: IRProxy, end: IRProxy, step: IRProxy = 1): IRProxy = (env: E) =>
    ArrayRange(start(env), end(env), step(env))

  def irArrayLen(a: IRProxy): IRProxy = (env: E) => ArrayLen(a(env))


  def irIf(cond: IRProxy)(cnsq: IRProxy)(altr: IRProxy): IRProxy = (env: E) =>
    If(cond(env), cnsq(env), altr(env))

  def makeArray(first: IRProxy, rest: IRProxy*): IRProxy = arrayToProxy(first +: rest)

  def makeStruct(fields: (Symbol, IRProxy)*): IRProxy = (env: E) =>
    MakeStruct(fields.map { case (s, ir) => (s.name, ir(env)) })

  def makeTuple(values: IRProxy*): IRProxy = (env: E) =>
    MakeTuple(values.map(_(env)))

  class TableIRProxy(val tir: TableIR) extends AnyVal {
    def empty: E = Env.empty
    def globalEnv: E = typ.globalEnv
    def env: E = typ.rowEnv

    def typ: TableType = tir.typ

    def getGlobals: IR = TableGetGlobals(tir)

    def mapGlobals(newGlobals: IRProxy): TableIR =
      TableMapGlobals(tir, newGlobals(globalEnv))

    def mapRows(newRow: IRProxy): TableIR =
      TableMapRows(tir, newRow(env))

    def keyBy(keys: IndexedSeq[String], isSorted: Boolean = false): TableIR =
      TableKeyBy(tir, keys, isSorted)

    def rename(rowMap: Map[String, String], globalMap: Map[String, String] = Map.empty): TableIR =
      TableRename(tir, rowMap, globalMap)

    def renameGlobals(globalMap: Map[String, String]): TableIR =
      rename(Map.empty, globalMap)

    def filter(ir: IRProxy): TableIR =
      TableFilter(tir, ir(env))

    def distinct(): TableIR = TableDistinct(tir)

    def collect(): IRProxy = TableCollect(tir)

    def collectAsDict(): IRProxy = {
      val uid = genUID()
      val keyFields = tir.typ.key
      val valueFields = tir.typ.valueType.fieldNames
      collect()
        .apply('rows)
        .map(Symbol(uid) ~> makeTuple(Symbol(uid).selectFields(keyFields: _*), Symbol(uid).selectFields(valueFields: _*)))
        .toDict
    }

    def aggregate(ir: IRProxy): IRProxy =
      TableAggregate(tir, ir(env))
  }

  class IRProxy(val ir: E => IR) extends AnyVal with Dynamic {
    def apply(idx: IRProxy): IRProxy = (env: E) =>
      ArrayRef(ir(env), idx(env))

    def invoke(name: String, args: IRProxy*): IRProxy = { env: E =>
      val irArgs = Array(ir(env)) ++ args.map(_(env))
      is.hail.expr.ir.invoke(name, irArgs: _*)
    }

    def selectDynamic(field: String): IRProxy = (env: E) =>
      GetField(ir(env), field)

    def unary_! = new IRProxy((env: E) => ApplyUnaryPrimOp(Bang(), ir(env)))

    def apply(lookup: Symbol): IRProxy = (env: E) => {
      val eval = ir(env)
      eval.typ match {
        case _: TStruct =>
          GetField(eval, lookup.name)
        case _: TArray =>
          ArrayRef(ir(env), ref(lookup)(env))
      }
    }

    def typecheck(t: Type): IRProxy = (env: E) => {
      val eval = ir(env)
      TypeCheck(eval, env, None)
      assert(eval.typ == t, t._toPretty + " " + eval.typ._toPretty)
      eval
    }

    def insertFields(fields: (Symbol, IRProxy)*): IRProxy = (env: E) =>
      InsertFields(ir(env), fields.map { case (s, fir) => (s.name, fir(env)) })

    def selectFields(fields: String*): IRProxy = (env: E) =>
      SelectFields(ir(env), fields)

    def dropFieldList(fields: Seq[String]): IRProxy = (env: E) => {
      val struct = ir(env)
      val typ = struct.typ.asInstanceOf[TStruct]
      SelectFields(struct, typ.fieldNames.diff(fields))
    }

    def dropFields(fields: Symbol*): IRProxy = dropFieldList(fields.map(_.name))

    def len: IRProxy = (env: E) => ArrayLen(ir(env))

    def orElse(alt: IRProxy): IRProxy = { env: E =>
      val uid = genUID()
      val eir = ir(env)
      Let(uid, eir, If(IsNA(Ref(uid, eir.typ)), alt(env), Ref(uid, eir.typ)))
    }

    def filter(pred: LambdaProxy): IRProxy = (env: E) => {
      val array = ir(env)
      val eltType = array.typ.asInstanceOf[TArray].elementType
      ArrayFilter(array, pred.s.name, pred.body(env.bind(pred.s.name -> eltType)))
    }

    def map(f: LambdaProxy): IRProxy = (env: E) => {
      val array = ir(env)
      val eltType = array.typ.asInstanceOf[TArray].elementType
      ArrayMap(array, f.s.name, f.body(env.bind(f.s.name -> eltType)))
    }

    def flatMap(f: LambdaProxy): IRProxy = (env: E) => {
      val array = ir(env)
      val eltType = array.typ.asInstanceOf[TArray].elementType
      ArrayFlatMap(array, f.s.name, f.body(env.bind(f.s.name -> eltType)))
    }

    def arrayAgg(f: LambdaProxy): IRProxy = (env: E) => {
      val array = ir(env)
      val eltType = array.typ.asInstanceOf[TArray].elementType
      ArrayAgg(array, f.s.name, f.body(env.bind(f.s.name -> eltType)))
    }

    def sort(ascending: IRProxy, onKey: Boolean = false): IRProxy = (env: E) => ArraySort(ir(env), ascending(env), onKey)

    def groupByKey: IRProxy = (env: E) => GroupByKey(ir(env))

    def toArray: IRProxy = (env: E) => ToArray(ir(env))

    def toDict: IRProxy = (env: E) => ToDict(ir(env))

    def parallelize(nPartitions: Option[Int] = None): TableIR = TableParallelize(ir(Env.empty), nPartitions)

    def arrayStructToDict(fields: IndexedSeq[String]): IRProxy = {
      val element = Symbol(genUID())
      ir
        .map(element ~>
          makeTuple(
            element.selectFields(fields: _*),
            element.dropFieldList(fields)))
        .toDict
    }

    private[ir] def apply(env: E): IR = ir(env)
  }

  class LambdaProxy(val s: Symbol, val body: IRProxy)

  class SymbolProxy(val s: Symbol) extends AnyVal {
    def ~> (body: IRProxy): LambdaProxy = new LambdaProxy(s, body)
  }

  case class BindingProxy(s: Symbol, value: IRProxy)

  object LetProxy {
    def bind(bindings: Seq[BindingProxy], body: IRProxy, env: E): IR =
      bindings match {
        case BindingProxy(sym, binding) +: rest =>
          val name = sym.name
          val value = binding(env)
          Let(name, value, bind(rest, body, env.bind(name -> value.typ)))
        case Seq() =>
          body(env)
      }
  }

  object let extends Dynamic {
    def applyDynamicNamed(method: String)(args: (String, IRProxy)*): LetProxy = {
      assert(method == "apply")
      new LetProxy(args.map { case (s, b) => BindingProxy(Symbol(s), b) })
    }
  }

  class LetProxy(val bindings: Seq[BindingProxy]) extends AnyVal with Dynamic {
    def apply(body: IRProxy): IRProxy = in(body)

    def in(body: IRProxy): IRProxy = { (env: E) =>
      LetProxy.bind(bindings, body, env)
    }
  }

  object MapIRProxy {
    def apply(f: (IRProxy) => IRProxy)(x: IRProxy): IRProxy = (e: E) => {
        MapIR(x => f(x)(e))(x(e))
      }
  }

  def subst(x: IRProxy, env: Env[IRProxy], aggEnv: Env[IRProxy] = Env.empty): IRProxy = (e: E) => {
    Subst(x(e), env.mapValues(_(e)), aggEnv.mapValues(_(e)))
  }

  def lift(f: (IR) => IRProxy)(x: IRProxy): IRProxy = (e: E) => f(x(e))(e)
}
