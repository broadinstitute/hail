package is.hail.expr.ir

import is.hail.expr._
import is.hail.expr.types._
import is.hail.utils._
import is.hail.TestUtils._
import org.testng.annotations.Test
import is.hail.utils.{FastIndexedSeq, FastSeq}
import is.hail.variant.Call2
import org.apache.spark.sql.Row

class AggregatorsSuite {
  def runAggregator(op: AggOp, t: Type, a: IndexedSeq[Any], expected: Any, args: IndexedSeq[IR] = FastIndexedSeq(),
    initOpArgs: Option[IndexedSeq[IR]] = None, seqOpArgs: IndexedSeq[IR] = FastIndexedSeq()) {
    val aggSig = AggSignature(op, t, args.map(_.typ), initOpArgs.map(_.map(_.typ)), seqOpArgs.map(_.typ))
    assertEvalsTo(ApplyAggOp(
      SeqOp(Ref("x", t), I32(0), aggSig, seqOpArgs),
      args, initOpArgs, aggSig),
      (a.map(i => Row(i)), TStruct("x" -> t)),
      expected)
  }

  def runAggregator(
    op: AggOp,
    t: TStruct,
    a: IndexedSeq[Row],
    expected: Any,
    aggIR: IR,
    args: IndexedSeq[IR],
    initOpArgs: Option[IndexedSeq[IR]],
    seqOpArgs: IndexedSeq[IR]) {
    val aggSig = AggSignature(op, aggIR.typ, args.map(_.typ), initOpArgs.map(_.map(_.typ)), seqOpArgs.map(_.typ))
    assertEvalsTo(ApplyAggOp(
      SeqOp(aggIR, I32(0), aggSig, seqOpArgs),
      args, initOpArgs, aggSig),
      (a, t),
      expected)
  }

  @Test def sumFloat64() {
    runAggregator(Sum(), TFloat64(), (0 to 100).map(_.toDouble), 5050.0)
    runAggregator(Sum(), TFloat64(), FastIndexedSeq(), 0.0)
    runAggregator(Sum(), TFloat64(), FastIndexedSeq(42.0), 42.0)
    runAggregator(Sum(), TFloat64(), FastIndexedSeq(null, 42.0, null), 42.0)
    runAggregator(Sum(), TFloat64(), FastIndexedSeq(null, null, null), 0.0)
  }

  def sumInt64() {
    runAggregator(Sum(), TInt64(), FastIndexedSeq(-1L, 2L, 3L), 4L)

  }

  @Test def fraction() {
    runAggregator(Fraction(), TBoolean(), FastIndexedSeq(true, false, null, true, false), 2.0 / 5.0)
  }

  @Test def collectBoolean() {
    runAggregator(Collect(), TBoolean(), FastIndexedSeq(true, false, null, true, false), FastIndexedSeq(true, false, null, true, false))
  }

  @Test def collectInt() {
    runAggregator(Collect(), TInt32(), FastIndexedSeq(10, null, 5), FastIndexedSeq(10, null, 5))
  }

  @Test def collectLong() {
    runAggregator(Collect(), TInt64(), FastIndexedSeq(10L, null, 5L), FastIndexedSeq(10L, null, 5L))
  }

  @Test def collectFloat() {
    runAggregator(Collect(), TFloat32(), FastIndexedSeq(10f, null, 5f), FastIndexedSeq(10f, null, 5f))
  }

  @Test def collectDouble() {
    runAggregator(Collect(), TFloat64(), FastIndexedSeq(10d, null, 5d), FastIndexedSeq(10d, null, 5d))
  }

  @Test def collectString() {
    runAggregator(Collect(), TString(), FastIndexedSeq("hello", null, "foo"), FastIndexedSeq("hello", null, "foo"))
  }

  @Test def collectArray() {
    runAggregator(Collect(), TArray(TInt32()), FastIndexedSeq(FastIndexedSeq(1, 2, 3), null, FastIndexedSeq()), FastIndexedSeq(FastIndexedSeq(1, 2, 3), null, FastIndexedSeq()))
  }

  @Test def collectStruct(): Unit = {
    runAggregator(Collect(),
      TStruct("a" -> TInt32(), "b" -> TBoolean()),
      FastIndexedSeq(Row(5, true), Row(3, false), null, Row(0, false), null),
      FastIndexedSeq(Row(5, true), Row(3, false), null, Row(0, false), null))
  }

  @Test def callStats() {
    runAggregator(CallStats(), TCall(),
      FastIndexedSeq(Call2(0, 0), Call2(0, 1), null, Call2(0, 2)),
      Row(FastIndexedSeq(4, 1, 1), FastIndexedSeq(4.0 / 6.0, 1.0 / 6.0, 1.0 / 6.0), 6, FastIndexedSeq(1, 0, 0)),
      initOpArgs = Some(FastIndexedSeq(I32(3))))
  }

  @Test def inbreeding() {
    runAggregator(
      Inbreeding(),
      TStruct("x" -> TCall(), "y" -> TFloat64()),
      FastIndexedSeq(Row(Call2(0, 0), 0d), Row(Call2(0, 1), 0.1),
        Row(Call2(0, 1), 0.2), Row(null, 0.3),
        Row(Call2(1, 1), 0.4), Row(Call2(0, 0), null)),
      Row(-1.040816, 4L, 3.02, 2L),
      Ref("x", TCall()),
      FastIndexedSeq(),
      None,
      seqOpArgs = FastIndexedSeq(Ref("y", TFloat64())))
  }

  // FIXME Max Boolean not supported by old-style MaxAggregator

  @Test def maxInt32() {
    runAggregator(Max(), TInt32(), FastIndexedSeq(), null)
    runAggregator(Max(), TInt32(), FastIndexedSeq(null), null)
    runAggregator(Max(), TInt32(), FastIndexedSeq(-2, null, 7), 7)
  }

  @Test def maxInt64() {
    runAggregator(Max(), TInt64(), FastIndexedSeq(-2L, null, 7L), 7L)
  }

  @Test def maxFloat32() {
    runAggregator(Max(), TFloat32(), FastIndexedSeq(-2.0f, null, 7.2f), 7.2f)
  }

  @Test def maxFloat64() {
    runAggregator(Max(), TFloat64(), FastIndexedSeq(-2.0, null, 7.2), 7.2)
  }

  @Test def takeInt32() {
    runAggregator(Take(), TInt32(), FastIndexedSeq(2, null, 7), FastIndexedSeq(2, null),
      args = FastIndexedSeq(I32(2)))
  }

  @Test def takeInt64() {
    runAggregator(Take(), TInt64(), FastIndexedSeq(2L, null, 7L), FastIndexedSeq(2L, null),
      args = FastIndexedSeq(I32(2)))
  }

  @Test def takeFloat32() {
    runAggregator(Take(), TFloat32(), FastIndexedSeq(2.0f, null, 7.2f), FastIndexedSeq(2.0f, null),
      args = FastIndexedSeq(I32(2)))
  }

  @Test def takeFloat64() {
    runAggregator(Take(), TFloat64(), FastIndexedSeq(2.0, null, 7.2), FastIndexedSeq(2.0, null),
      args = FastIndexedSeq(I32(2)))
  }

  @Test def testHist() {
    runAggregator(Histogram(), TFloat64(),
      FastIndexedSeq(-10.0, 0.5, 2.0, 2.5, 4.4, 4.6, 9.5, 20.0, 20.0),
      Row((0 to 10).map(_.toDouble).toFastIndexedSeq, FastIndexedSeq(1L, 0L, 2L, 0L, 2L, 0L, 0L, 0L, 0L, 1L), 1L, 2L),
      args = FastIndexedSeq(F64(0.0), F64(10.0), I32(10)))
  }

  @Test
  def ifInApplyAggOp() {
    val aggSig = AggSignature(Sum(), TFloat64(), FastSeq(), None, FastSeq())
    assertEvalsTo(
      ApplyAggOp(
        If(
          ApplyComparisonOp(NEQ(TFloat64()), Ref("a", TFloat64()), F64(10.0)),
          SeqOp(ApplyBinaryPrimOp(Multiply(), Ref("a", TFloat64()), Ref("b", TFloat64())),
            I32(0), aggSig),
          Begin(FastSeq())),
        FastSeq(), None, aggSig),
      (FastIndexedSeq(Row(1.0, 10.0), Row(10.0, 10.0), Row(null, 10.0)), TStruct("a" -> TFloat64(), "b" -> TFloat64())),
      10.0)
  }

  @Test
  def sumMultivar() {
    val aggSig = AggSignature(Sum(), TFloat64(), FastSeq(), None, FastSeq())
    assertEvalsTo(ApplyAggOp(
      SeqOp(ApplyBinaryPrimOp(Multiply(), Ref("a", TFloat64()), Ref("b", TFloat64())), I32(0), aggSig),
      FastSeq(), None, aggSig),
      (FastIndexedSeq(Row(1.0, 10.0), Row(10.0, 10.0), Row(null, 10.0)), TStruct("a" -> TFloat64(), "b" -> TFloat64())),
      110.0)
  }

  private[this] def assertArraySumEvalsTo[T: HailRep](
    a: IndexedSeq[Seq[T]],
    expected: Seq[T]
  ): Unit = {
    val aggSig = AggSignature(Sum(), TArray(hailType[T]), FastSeq(), None, FastSeq())
    val aggregable = a.map(Row(_))
    assertEvalsTo(
      ApplyAggOp(SeqOp(Ref("a", TArray(hailType[T])), I32(0), aggSig), FastSeq(), None, aggSig),
      (aggregable, TStruct("a" -> TArray(hailType[T]))),
      expected)
  }

  @Test
  def arraySumFloat64OnEmpty(): Unit =
    assertArraySumEvalsTo[Double](
      FastIndexedSeq(),
      null
    )

  @Test
  def arraySumFloat64OnSingletonMissing(): Unit =
    assertArraySumEvalsTo[Double](
      FastIndexedSeq(null),
      null
    )

  @Test
  def arraySumFloat64OnAllMissing(): Unit =
    assertArraySumEvalsTo[Double](
      FastIndexedSeq(null, null, null),
      null
    )

  @Test
  def arraySumInt64OnEmpty(): Unit =
    assertArraySumEvalsTo[Long](
      FastIndexedSeq(),
      null
    )

  @Test
  def arraySumInt64OnSingletonMissing(): Unit =
    assertArraySumEvalsTo[Long](
      FastIndexedSeq(null),
      null
    )

  @Test
  def arraySumInt64OnAllMissing(): Unit =
    assertArraySumEvalsTo[Long](
      FastIndexedSeq(null, null, null),
      null
    )

  @Test
  def arraySumFloat64OnSmallArray(): Unit =
    assertArraySumEvalsTo(
      FastIndexedSeq(
        FastSeq(1.0, 2.0),
        FastSeq(10.0, 20.0),
        null),
      FastSeq(11.0, 22.0)
    )

  @Test
  def arraySumInt64OnSmallArray(): Unit =
    assertArraySumEvalsTo(
      FastIndexedSeq(
        FastSeq(1L, 2L),
        FastSeq(10L, 20L),
        null),
      FastSeq(11L, 22L)
    )

  @Test
  def arraySumInt64FirstElementMissing(): Unit =
    assertArraySumEvalsTo(
      FastIndexedSeq(
        null,
        FastSeq(1L, 33L),
        FastSeq(42L, 3L)),
      FastSeq(43L, 36L)
    )

  private[this] def assertTakeByEvalsTo(aggType: Type, keyType: Type, n: Int, a: IndexedSeq[Row], expected: IndexedSeq[Any]) {
    runAggregator(TakeBy(), TStruct("x" -> aggType, "y" -> keyType),
      a,
      expected,
      Ref("x", aggType),
      args = FastIndexedSeq(I32(n)),
      initOpArgs = None,
      seqOpArgs = FastIndexedSeq(Ref("y", keyType)))
  }

  @Test def takeByNGreater() {
    assertTakeByEvalsTo(TInt32(), TInt32(), 5,
      FastIndexedSeq(Row(3, 4)),
      FastIndexedSeq(3))
  }

  @Test def takeByIntBoolean() {
    assertTakeByEvalsTo(TInt32(), TBoolean(), 2,
      FastIndexedSeq(Row(3, true), Row(null, null), Row(null, false)),
      FastIndexedSeq(null, 3))
  }

  @Test def takeByIntLong() {
    assertTakeByEvalsTo(TInt32(), TInt64(), 3,
      FastIndexedSeq(Row(3, 4L), Row(null, null), Row(null, 2L), Row(11, 0L), Row(45, 1L), Row(3, null)),
      FastIndexedSeq(11, 45, null))
  }

  @Test def takeByIntFloat() {
    assertTakeByEvalsTo(TInt32(), TFloat32(), 3,
      FastIndexedSeq(Row(3, 4F), Row(null, null), Row(null, 2F), Row(11, 0F), Row(45, 1F), Row(3, null)),
      FastIndexedSeq(11, 45, null))
  }

  @Test def takeByIntDouble() {
    assertTakeByEvalsTo(TInt32(), TFloat64(), 3,
      FastIndexedSeq(Row(3, 4D), Row(null, null), Row(null, 2D), Row(11, 0D), Row(45, 1D), Row(3, null)),
      FastIndexedSeq(11, 45, null))
  }

  @Test def takeByIntAnnotation() {
    assertTakeByEvalsTo(TInt32(), TString(), 3,
      FastIndexedSeq(Row(3, "hello"), Row(null, null), Row(null, "foo"), Row(11, "a"), Row(45, "b"), Row(3, null)),
      FastIndexedSeq(11, 45, null))
  }
}
