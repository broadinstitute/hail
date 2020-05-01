package is.hail.expr.ir.agg

import is.hail.annotations.{Region, RegionUtils, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitClassBuilder, EmitCode, EmitFunctionBuilder, typeToTypeInfo}
import is.hail.expr.types.physical._
import is.hail.utils._

class PrevNonNullAggregator(typ: PType) extends StagedAggregator {
  type State = TypedRegionBackedAggState
  assert(PType.canonical(typ) == typ)
  val resultType: PType = typ

  def createState(kb: EmitClassBuilder[_]): State =
    new TypedRegionBackedAggState(typ.setRequired(false), kb)

  protected def _initOp(state: State, init: Array[EmitCode]): Code[Unit] = {
    assert(init.length == 0)
    state.storeMissing()
  }

  protected def _seqOp(state: State, seq: Array[EmitCode]): Code[Unit] = {
    val Array(elt: EmitCode) = seq
    val v = state.kb.genFieldThisRef()(typeToTypeInfo(typ))
    Code(
      elt.setup,
      elt.m.mux(Code._empty,
        Code(v := elt.value, state.storeNonmissing(v)))
    )
  }

  protected def _combOp(state: State, other: State): Code[Unit] = {
    val t = other.get()
    val v = state.kb.genFieldThisRef()(typeToTypeInfo(typ))
    Code(
      t.setup,
      t.m.mux(Code._empty,
        Code(v := t.value, state.storeNonmissing(v))))
  }

  protected def _result(state: State, srvb: StagedRegionValueBuilder): Code[Unit] = {
    val t = state.get()
    Code(
      t.setup,
      t.m.mux(
        srvb.setMissing(),
        srvb.addWithDeepCopy(resultType, t.v)))
  }
}
