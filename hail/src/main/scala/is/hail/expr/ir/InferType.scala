package is.hail.expr.ir

import is.hail.expr.Nat
import is.hail.expr.types.virtual._
import is.hail.utils._

// FIXME: strip all requiredness logic when possible
object InferType {
  def apply(ir: IR): Type = {
    ir match {
      case I32(_) => TInt32()
      case I64(_) => TInt64()
      case F32(_) => TFloat32()
      case F64(_) => TFloat64()
      case Str(_) => TString()
      case Literal(t, _) => t
      case True() | False() => TBoolean()
      case Void() => TVoid
      case Cast(_, t) => t
      case CastRename(_, t) => t
      case NA(t) => t
      case IsNA(_) => TBoolean()
      case Coalesce(values) => values.head.typ
      case Ref(_, t) => t
      case RelationalRef(_, t) => t
      case RelationalLet(_, _, body) => body.typ
      case In(_, t) => t.virtualType
      case MakeArray(_, t) => t
      case MakeStream(_, t) => t
      case MakeNDArray(data, shape, _) =>
        TNDArray(coerce[TArray](data.typ).elementType.setRequired(true), Nat(shape.typ.asInstanceOf[TTuple].size))
      case _: ArrayLen => TInt32()
      case _: StreamRange => TStream(TInt32())
      case _: LowerBoundOnOrderedCollection => TInt32()
      case _: ArrayFor => TVoid
      case _: InitOp => TVoid
      case _: SeqOp => TVoid
      case _: CombOp => TVoid
      case ResultOp(_, aggSigs) =>
        TTuple(aggSigs.map(_.resultType): _*)
      case AggStateValue(i, sig) => TBinary()
      case _: CombOpValue => TVoid
      case _: SerializeAggs => TVoid
      case _: DeserializeAggs => TVoid
      case _: Begin => TVoid
      case Die(_, t) => t
      case If(cond, cnsq, altr) =>
        assert(cond.typ.isOfType(TBoolean()))
        assert(cnsq.typ.isOfType(altr.typ))
        if (cnsq.typ != altr.typ)
          cnsq.typ.deepOptional()
        else
          cnsq.typ
      case Let(name, value, body) =>
        body.typ
      case AggLet(name, value, body, _) =>
        body.typ
      case TailLoop(_, _, body) =>
        body.typ
      case Recur(_, _, typ) =>
        typ
      case ApplyBinaryPrimOp(op, l, r) =>
        BinaryOp.getReturnType(op, l.typ, r.typ).setRequired(l.typ.required && r.typ.required)
      case ApplyUnaryPrimOp(op, v) =>
        UnaryOp.getReturnType(op, v.typ).setRequired(v.typ.required)
      case ApplyComparisonOp(op, l, r) =>
        assert(l.typ isOfType r.typ)
        op match {
          case _: Compare => TInt32(l.pType.required && r.pType.required)
          case _ => TBoolean(l.pType.required && r.pType.required)
        }
      case a: ApplyIR => a.explicitNode.typ
      case a: AbstractApplyNode[_] =>
        val argTypes = a.args.map(_.typ)
        a.implementation.unify(argTypes :+ a.returnType)
        a.returnType
      case ArrayRef(a, i, s) =>
        assert(i.typ.isOfType(TInt32()))
        coerce[TStreamable](a.typ).elementType.setRequired(a.typ.required && i.typ.required)
      case ArraySort(a, _, _, compare) =>
        assert(compare.typ.isOfType(TBoolean()))
        val et = coerce[TStreamable](a.typ).elementType
        TArray(et, a.typ.required)
      case ToSet(a) =>
        val et = coerce[TIterable](a.typ).elementType
        TSet(et, a.typ.required)
      case ToDict(a) =>
        val elt = coerce[TBaseStruct](coerce[TIterable](a.typ).elementType)
        TDict(elt.types(0), elt.types(1), a.typ.required)
      case ToArray(a) =>
        val elt = coerce[TIterable](a.typ).elementType
        TArray(elt, a.typ.required)
      case ToStream(a) =>
        val elt = coerce[TIterable](a.typ).elementType
        TStream(elt, a.typ.required)
      case GroupByKey(collection) =>
        val elt = coerce[TBaseStruct](coerce[TStreamable](collection.typ).elementType)
        TDict(elt.types(0), TArray(elt.types(1)), collection.typ.required)
      case ArrayMap(a, name, body) =>
        coerce[TStreamable](a.typ).copyStreamable(body.typ.setRequired(false))
      case ArrayZip(as, _, body, _) => as.head.typ.asInstanceOf[TStreamable].copyStreamable(body.typ, false)
      case ArrayFilter(a, name, cond) =>
        a.typ
      case ArrayFlatMap(a, name, body) =>
        coerce[TStreamable](a.typ).copyStreamable(coerce[TIterable](body.typ).elementType)
      case ArrayFold(a, zero, accumName, valueName, body) =>
        assert(body.typ == zero.typ)
        zero.typ
      case ArrayFold2(_, _, _, _, result) => result.typ
      case ArrayScan(a, zero, accumName, valueName, body) =>
        assert(body.typ == zero.typ)
        coerce[TStreamable](a.typ).copyStreamable(zero.typ)
      case ArrayAgg(_, _, query) =>
        query.typ
      case ArrayAggScan(a, _, query) =>
        a.typ.asInstanceOf[TStreamable].copyStreamable(query.typ)
      case RunAgg(body, result, _) =>
        result.typ
      case RunAggScan(a, _, _, _, result, _) =>
        a.typ.asInstanceOf[TStreamable].copyStreamable(result.typ)
      case ArrayLeftJoinDistinct(left, right, l, r, compare, join) =>
        coerce[TStreamable](left.typ).copyStreamable(join.typ)
      case NDArrayShape(nd) =>
        val ndType = nd.typ.asInstanceOf[TNDArray]
        ndType.representation.fieldType("shape").asInstanceOf[TTuple].setRequired(ndType.required)
      case NDArrayReshape(nd, shape) =>
        TNDArray(coerce[TNDArray](nd.typ).elementType, Nat(shape.typ.asInstanceOf[TTuple].size), nd.typ.required)
      case NDArrayConcat(nds, _) =>
        coerce[TStreamable](nds.typ).elementType
      case NDArrayMap(nd, _, body) =>
        TNDArray(body.typ.setRequired(true), coerce[TNDArray](nd.typ).nDimsBase, nd.typ.required)
      case NDArrayMap2(l, _, _, _, body) =>
        TNDArray(body.typ.setRequired(true), coerce[TNDArray](l.typ).nDimsBase, l.typ.required)
      case NDArrayReindex(nd, indexExpr) =>
        TNDArray(coerce[TNDArray](nd.typ).elementType, Nat(indexExpr.length), nd.typ.required)
      case NDArrayAgg(nd, axes) =>
        val childType = coerce[TNDArray](nd.typ)
        TNDArray(childType.elementType, Nat(childType.nDims - axes.length), childType.required)
      case NDArrayRef(nd, idxs) =>
        assert(idxs.forall(_.typ.isOfType(TInt64())))
        coerce[TNDArray](nd.typ).elementType.setRequired(nd.typ.required && idxs.forall(_.typ.required))
      case NDArraySlice(nd, slices) =>
        val childTyp = coerce[TNDArray](nd.typ)
        val slicesTyp = coerce[TTuple](slices.typ)
        val tuplesOnly = slicesTyp.types.collect { case x: TTuple => x}
        val remainingDims = Nat(tuplesOnly.length)
        TNDArray(childTyp.elementType, remainingDims)
      case NDArrayMatMul(l, r) =>
        val lTyp = coerce[TNDArray](l.typ)
        val rTyp = coerce[TNDArray](r.typ)
        TNDArray(lTyp.elementType, Nat(TNDArray.matMulNDims(lTyp.nDims, rTyp.nDims)), lTyp.required && rTyp.required)
      case NDArrayQR(nd, mode) =>
        if (Array("complete", "reduced").contains(mode)) {
          TTuple(TNDArray(TFloat64(), Nat(2), false), TNDArray(TFloat64(), Nat(2), false))
        } else if (mode == "raw") {
          TTuple(TNDArray(TFloat64(), Nat(2), false), TNDArray(TFloat64(), Nat(1), false))
        } else if (mode == "r") {
          TNDArray(TFloat64(), Nat(2), false)
        } else {
          throw new NotImplementedError(s"Cannot infer type for mode $mode")
        }
      case NDArrayWrite(_, _) => TVoid
      case AggFilter(_, aggIR, _) =>
        aggIR.typ
      case AggExplode(array, name, aggBody, _) =>
        aggBody.typ
      case AggGroupBy(key, aggIR, _) =>
        TDict(key.typ, aggIR.typ)
      case AggArrayPerElement(a, _, _, aggBody, _, _) => TArray(aggBody.typ)
      case ApplyAggOp(_, _, aggSig) =>
        aggSig.returnType
      case ApplyScanOp(_, _, aggSig) =>
        aggSig.returnType
      case MakeStruct(fields) =>
        TStruct(fields.map { case (name, a) =>
          (name, a.typ)
        }: _*)
      case SelectFields(old, fields) =>
        val tbs = coerce[TStruct](old.typ)
        tbs.select(fields.toFastIndexedSeq)._1
      case InsertFields(old, fields, fieldOrder) =>
        val tbs = coerce[TStruct](old.typ)
        val s = tbs.insertFields(fields.map(f => (f._1, f._2.typ)))
        fieldOrder.map { fds =>
          assert(fds.length == s.size)
          TStruct(fds.map(f => f -> s.fieldType(f)): _*)
        }.getOrElse(s)
      case GetField(o, name) =>
        val t = coerce[TStruct](o.typ)
        if (t.index(name).isEmpty)
          throw new RuntimeException(s"$name not in $t")
        val fd = t.field(name).typ
        fd.setRequired(t.required && fd.required)
      case MakeTuple(values) =>
        TTuple(values.map { case (i, value) => TupleField(i, value.typ) }.toFastIndexedSeq, required = false)
      case GetTupleElement(o, idx) =>
        val t = coerce[TTuple](o.typ)
        val fd = t.fields(t.fieldIndex(idx)).typ
        fd.setRequired(t.required && fd.required)
      case TableCount(_) => TInt64()
      case MatrixCount(_) => TTuple(TInt64(), TInt32())
      case TableAggregate(child, query) =>
        query.typ
      case MatrixAggregate(child, query) =>
        query.typ
      case _: TableWrite => TVoid
      case _: TableMultiWrite => TVoid
      case _: MatrixWrite => TVoid
      case _: MatrixMultiWrite => TVoid
      case _: BlockMatrixCollect => TNDArray(TFloat64(), Nat(2))
      case _: BlockMatrixWrite => TVoid
      case _: BlockMatrixMultiWrite => TVoid
      case _: UnpersistBlockMatrix => TVoid
      case TableGetGlobals(child) => child.typ.globalType
      case TableCollect(child) => TStruct("rows" -> TArray(child.typ.rowType), "global" -> child.typ.globalType)
      case TableToValueApply(child, function) => function.typ(child.typ)
      case MatrixToValueApply(child, function) => function.typ(child.typ)
      case BlockMatrixToValueApply(child, function) => function.typ(child.typ)
      case CollectDistributedArray(_, _, _, _, body) => TArray(body.typ)
      case ReadPartition(_, _, rowType) => TStream(rowType)
      case ReadValue(_, _, typ) => typ
      case WriteValue(value, pathPrefix, spec) => TString()
      case LiftMeOut(child) => child.typ
    }
  }
}
