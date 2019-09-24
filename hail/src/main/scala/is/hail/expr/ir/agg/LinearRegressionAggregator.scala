package is.hail.expr.ir.agg

import breeze.linalg.{DenseMatrix, DenseVector, diag, inv}

import is.hail.annotations.{Region, RegionValueBuilder, StagedRegionValueBuilder, UnsafeRow}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitFunctionBuilder, EmitTriplet}
import is.hail.expr.types.physical.{PArray, PFloat64, PStruct, PTuple, PInt32}

object LinearRegressionAggregator extends StagedAggregator {
  type State = TypedRVAState

  val vector = PArray(PFloat64(true), true)
  val scalar = PFloat64(true)
  val stateType: PTuple = PTuple(true, vector, vector, PInt32())
  def resultType = PStruct(true, "xty" -> vector, "beta" -> vector, "diag_inv" -> vector, "beta0" -> vector)

  def createState(fb: EmitFunctionBuilder[_]): State =
    new TypedRVAState(stateType, fb)

  def initOpF(state: State)(mb: MethodBuilder, k: Code[Int], k0: Code[Int]): Code[Unit] = Code(
    state.off := stateType.allocate(state.region),
    Region.storeAddress(
      stateType.fieldOffset(state.off, 0),
      vector.allocate(state.region, k)),
    Region.setMemory(
      stateType.loadField(state.off, 0),
      coerce[Long](k)*scalar.byteSize,
      0: Byte),
    Region.storeAddress(
      stateType.fieldOffset(state.off, 1),
      vector.allocate(state.region, k*k)
    ),
    Region.setMemory(
      stateType.loadField(state.off, 1),
      coerce[Long](k)*coerce[Long](k)*scalar.byteSize,
      0: Byte),
    Region.storeInt(
      stateType.loadField(state.off, 2),
      k0)
  )

  def initOp(state: State, init: Array[EmitTriplet], dummy: Boolean): Code[Unit] = {
    val _initOpF = state.fb.newMethod[Int, Int, Unit]("initOp")(initOpF(state))
    val Array(kt, k0t) = init
    (Code(kt.setup, kt.m) || Code(k0t.setup, k0t.m)).mux(
      Code._fatal("linreg: init args may not be missing"),
      _initOpF(coerce[Int](kt.v), coerce[Int](k0t.v)))
  }

  def seqOpF(state: State)(mb: MethodBuilder, y: Code[Double], x: Code[Long]): Code[Unit] = {
    val n = mb.newLocal[Int]
    val i = mb.newLocal[Int]
    val j = mb.newLocal[Int]
    val sptr = mb.newLocal[Long]
    val xptr = mb.newLocal[Long]
    val xptr2 = mb.newLocal[Long]
    val xty = stateType.loadField(state.off, 0)
    val xtx = stateType.loadField(state.off, 1)

    Code(
      n := vector.loadLength(xty),
      i := 0,
      sptr := vector.firstElementOffset(xty),
      xptr := vector.firstElementOffset(x),
      Code.whileLoop(i < n, Code(
        Region.storeDouble(sptr, Region.loadDouble(sptr)
          + (Region.loadDouble(xptr) * y)),
        i := i + 1,
        sptr := sptr + scalar.byteSize,
        xptr := xptr + scalar.byteSize
      )),

      i := 0,
      sptr := vector.firstElementOffset(xtx),
      xptr := vector.firstElementOffset(x),
      Code.whileLoop(i < n, Code(
        j := 0,
        xptr2 := vector.firstElementOffset(x),
        Code.whileLoop(j < n, Code(
          Region.storeDouble(sptr, Region.loadDouble(sptr)
            + (Region.loadDouble(xptr) * Region.loadDouble(xptr2))),
          j += 1,
          sptr := sptr + scalar.byteSize,
          xptr2 := xptr2 + scalar.byteSize)),
        i += 1,
        sptr := sptr + scalar.byteSize,
        xptr := xptr + scalar.byteSize)))
  }

  def seqOp(state: State, seq: Array[EmitTriplet], dummy: Boolean): Code[Unit] = {
    val _seqOpF = state.fb.newMethod[Double, Long, Unit]("seqOp")(seqOpF(state))
    val Array(y, x) = seq
    (Code(y.setup, y.m) || Code(x.setup, x.m)).mux(
      Code._empty,
      _seqOpF(coerce[Double](y.v), coerce[Long](x.v)))
  }

  def combOpF(state: State, other: State)(mb: MethodBuilder): Code[Unit] = {
    val n = mb.newLocal[Int]
    val i = mb.newLocal[Int]
    val sptr = mb.newLocal[Long]
    val optr = mb.newLocal[Long]
    val xty = stateType.loadField(state.off, 0)
    val xtx = stateType.loadField(state.off, 1)
    val oxty = stateType.loadField(other.off, 0)
    val oxtx = stateType.loadField(other.off, 1)

    Code(
      n := vector.loadLength(xty),
      i := 0,
      sptr := vector.firstElementOffset(xty),
      optr := vector.firstElementOffset(oxty),
      Code.whileLoop(i < n, Code(
        Region.storeDouble(sptr, Region.loadDouble(sptr) + Region.loadDouble(optr)),
        i := i + 1,
        sptr := sptr + scalar.byteSize,
        optr := optr + scalar.byteSize)),

      n := vector.loadLength(xtx),
      i := 0,
      sptr := vector.firstElementOffset(xtx),
      optr := vector.firstElementOffset(oxtx),
      Code.whileLoop(i < n, Code(
        Region.storeDouble(sptr, Region.loadDouble(sptr) + Region.loadDouble(optr)),
        i := i + 1,
        sptr := sptr + scalar.byteSize,
        optr := optr + scalar.byteSize)))
  }.asInstanceOf[Code[Unit]]

  def combOp(state: State, other: State, dummy: Boolean): Code[Unit] =
    state.fb.newMethod[Unit]("combOp")(combOpF(state, other))

  def computeResult(region: Region, xtyPtr: Long, xtxPtr: Long, k0: Int): Long = {
    // FIXME: are the toArrays necessary?
    val xty = DenseVector(UnsafeRow.readArray(vector, null, xtyPtr).asInstanceOf[IndexedSeq[Double]].toArray[Double])
    val k = xty.length
    val xtx = DenseMatrix.create(k, k, UnsafeRow.readArray(vector, null, xtxPtr).asInstanceOf[IndexedSeq[Double]].toArray[Double])
    val b = xtx \ xty
    val diagInv = diag(inv(xtx))

    val xtx0 = xtx(0 until k0, 0 until k0)
    val xty0 = xty(0 until k0)
    val b0 = xtx0 \ xty0

    val rvb = new RegionValueBuilder(region)
    rvb.start(resultType)
    rvb.startStruct()

    rvb.startArray(k)
    var i = 0
    while (i < k) {
      rvb.addDouble(xty(i))
      i += 1
    }
    rvb.endArray()

    rvb.startArray(k)
    i = 0
    while (i < k) {
      rvb.addDouble(b(i))
      i += 1
    }
    rvb.endArray()

    rvb.startArray(k)
    i = 0
    while (i < k) {
      rvb.addDouble(diagInv(i))
      i += 1
    }
    rvb.endArray()

    rvb.startArray(k0)
    i = 0
    while (i < k) {
      rvb.addDouble(b0(i))
      i += 1
    }
    rvb.endArray()

    rvb.endStruct()
    rvb.end()
  }

  def result(state: State, srvb: StagedRegionValueBuilder, dummy: Boolean): Code[Unit] = {
    Code.invokeScalaObject[Region, Long, Long, Int, Unit](getClass, "computeResult",
      srvb.region,
      stateType.loadField(state.off, 0),
      stateType.loadField(state.off, 1),
      Region.loadInt(stateType.loadField(state.off, 2))
    )
  }
}
