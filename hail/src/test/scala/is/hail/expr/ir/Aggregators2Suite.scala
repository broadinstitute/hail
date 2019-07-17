package is.hail.expr.ir

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import is.hail.HailSuite
import is.hail.annotations.{Region, SafeRow, ScalaToRegionValue, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.agg._
import is.hail.expr.types.physical._
import is.hail.expr.types.virtual._
import is.hail.io.{CodecSpec, InputBuffer, OutputBuffer}
import is.hail.utils._
import org.apache.spark.sql.Row
import org.testng.annotations.Test

object TUtils {
  def printBytes(region: Region, off: Long, n: Int, header: String = null): String = {
    region.loadBytes(off, n).zipWithIndex
      .grouped(16)
      .map(bs => bs.map { case (b, _) => "%02x".format(b) }.mkString("  %016x: ".format(off + bs(0)._2), " ", ""))
      .mkString(if (header != null) s"$header\n" else "\n", "\n", "")
  }

  def printBytes(region: Code[Region], off: Code[Long], n: Int): Code[String] =
    Code.invokeScalaObject[Region, Long, Int, String, String](TUtils.getClass, "printBytes", region, off, n, "")

  def printRow(region: Region, off: Long): String =
    SafeRow(TStruct("a" -> TString(), "b" -> TInt32()).physicalType, region, off).toString

  def printRow(region: Code[Region], off: Code[Long]): Code[String] =
    Code.invokeScalaObject[Region, Long, String](TUtils.getClass, "printRow", region, off)
}

class Aggregators2Suite extends HailSuite {

  val rowType = PStruct("a" -> PString(), "b" -> PInt64())
  val arrayType = PArray(rowType)
  val streamType = PArray(arrayType)

  val pnnAggSig = AggSignature(PrevNonnull(), FastSeq[Type](), None, FastSeq[Type](rowType.virtualType))
  val pnnAgg = agg.Extract.getAgg(pnnAggSig)
  val countAggSig = AggSignature(Count(), FastSeq[Type](), None, FastSeq[Type]())
  val countAgg = agg.Extract.getAgg(countAggSig)
  val sumAggSig = AggSignature(Sum(), FastSeq[Type](), None, FastSeq[Type](TInt64()))
  val sumAgg = agg.Extract.getAgg(sumAggSig)

  val aggSigs = FastIndexedSeq(pnnAggSig, countAggSig, sumAggSig)
  val aggs: Array[StagedRegionValueAggregator] = Array(pnnAgg, countAgg, sumAgg)


  val lcAggSig = AggSignature(AggElementsLengthCheck2(aggSigs, false), FastSeq[Type](), Some(FastSeq[Type]()), FastSeq[Type](TInt32()))
  val lcAgg: ArrayElementLengthCheckAggregator = agg.Extract.getAgg(lcAggSig).asInstanceOf[ArrayElementLengthCheckAggregator]
  val eltAggSig = AggSignature(AggElements2(aggSigs), FastSeq[Type](), None, FastSeq[Type](TInt32(), TVoid))
  val eltAgg: ArrayElementwiseOpAggregator = agg.Extract.getAgg(eltAggSig).asInstanceOf[ArrayElementwiseOpAggregator]

  val resType = PTuple(FastSeq(lcAgg.resultType))

  val value = FastIndexedSeq(
    FastIndexedSeq(Row("a", 0L), Row("b", 0L), Row("c", 0L), Row("f", 0L)),
    FastIndexedSeq(Row("a", 1L), null, Row("c", 1L), null),
    FastIndexedSeq(Row("a", 2L), Row("b", 2L), null, Row("f", 2L)),
    FastIndexedSeq(Row("a", 3L), Row("b", 3L), Row("c", 3L), Row("f", 3L)),
    FastIndexedSeq(Row("a", 4L), Row("b", 4L), Row("c", 4L), null),
    FastIndexedSeq(null, null, null, Row("f", 5L)))

  val expected =
    FastIndexedSeq(
      Row(Row("a", 4), 6L, 10L),
      Row(Row("b", 4), 6L, 9L),
      Row(Row("c", 4), 6L, 8L),
      Row(Row("f", 5), 6L, 10L))

  def rowVar(r: Code[Region], a: Code[Long], i: Code[Int]): RVAVariable =
    RVAVariable(EmitTriplet(Code._empty,
      arrayType.isElementMissing(r, a, i),
      arrayType.loadElement(r, a, i)), rowType)

  def bVar(r: Code[Region], a: Code[Long], i: Code[Int]): RVAVariable = {
    val RVAVariable(row, _) = rowVar(r, a, i)
    RVAVariable(EmitTriplet(row.setup,
      row.m || rowType.isFieldMissing(r, row.value[Long], 1),
      r.loadLong(rowType.loadField(r, row.value[Long], 1))), PInt64())
  }

  def seqOne(s: Array[RVAState], a: Code[Long], i: Code[Int]): Code[Unit] = {
    val r = s(0).r
    Code(
      pnnAgg.seqOp(s(0), Array(rowVar(r, a, i))),
      countAgg.seqOp(s(1), Array()),
      sumAgg.seqOp(s(2), Array(bVar(r, a, i))))
  }

  def initAndSeq(s: ArrayElementState, off: Code[Long]): Code[Unit] = {
    val streamLen = s.mb.newField[Int]
    val streamIdx = s.mb.newField[Int]

    val aidx = s.mb.newField[Int]
    val alen = s.mb.newField[Int]

    val a = s.mb.newField[Long]
    val r = s.region

    val lenVar = RVAVariable(EmitTriplet(Code._empty, false, alen), PInt32())
    val idxVar = RVAVariable(EmitTriplet(Code._empty, false, aidx), PInt32())

    val eltSeqOp = RVAVariable(EmitTriplet(seqOne(s.nested, a, aidx), false, Code._empty), PVoid)

    Code(
      lcAgg.initOp(s, Array()),
      streamIdx := 0,
      streamLen := streamType.loadLength(r, off),
      Code.whileLoop(streamIdx < streamLen,
        a := streamType.loadElement(r, off, streamIdx),
        alen := arrayType.loadLength(r, a),
        aidx := 0,
        lcAgg.seqOp(s, Array(lenVar)),
        Code.whileLoop(aidx < alen,
          eltAgg.seqOp(s, Array(idxVar, eltSeqOp)),
          aidx := aidx + 1),
        streamIdx := streamIdx + 1))
  }

  @Test def testInitSeqResult() {
    val firstCol = value.map(_(0))

    val fb = EmitFunctionBuilder[Region, Long, Long]
    val r = fb.apply_method.getArg[Region](1)
    val off = fb.apply_method.getArg[Long](2)

    val resType = PTuple(aggs.map(_.resultType))
    val states: Array[RVAState] = aggs.map(_.createState(fb.apply_method))
    val srvb = new StagedRegionValueBuilder(EmitRegion.default(fb.apply_method), resType)

    val aidx = fb.newField[Int]
    val alen = fb.newField[Int]

    fb.emit(
      Code(r.load().setNumParents(aggs.length),
        Code(Array.tabulate(aggs.length) { i =>
          Code(states(i).r := r.load().getParentReference(i, Region.REGULAR),
            aggs(i).initOp(states(i), Array()))
        }: _*),
        aidx := 0,
        alen := arrayType.loadLength(r, off),
        Code.whileLoop(aidx < alen,
          seqOne(states, off, aidx),
          aidx := aidx + 1),
        srvb.start(),
        Code(aggs.zip(states).map{ case (agg, s) => Code(agg.result(s, srvb), srvb.advance()) }: _*),
        srvb.offset))

    val aggf = fb.resultWithIndex()

    Region.scoped { region =>
      val offset = ScalaToRegionValue(region, arrayType.virtualType, firstCol)
      val res = aggf(0, region)(region, offset)
      assert(SafeRow(resType, region, res) == expected(0))
    }
  }

  @Test def testInitSeqResultArray() {
    val fb = EmitFunctionBuilder[Region, Long, Long]
    val r = fb.apply_method.getArg[Region](1)
    val off = fb.apply_method.getArg[Long](2)

    val s = lcAgg.createState(fb.apply_method)
    val srvb = new StagedRegionValueBuilder(EmitRegion.default(fb.apply_method), resType)

    fb.emit(
      Code(
        s.r := r,
        initAndSeq(s, off),
        srvb.start(),
        lcAgg.result(s, srvb),
        srvb.offset))

    val aggf = fb.resultWithIndex()

    Region.scoped { region =>
      val offset = ScalaToRegionValue(region, streamType.virtualType, value)
      val res = aggf(0, region)(region, offset)
      assert(SafeRow(resType, region, res) == Row(expected))
    }
  }

  @Test def serializeDeserializeAndCombOp() {
    val partitioned = value.grouped(3).toFastIndexedSeq

    val fb = EmitFunctionBuilder[Region, Long, Long]
    val r = fb.apply_method.getArg[Region](1)
    val off = fb.apply_method.getArg[Long](2)

    val s = lcAgg.createState(fb.apply_method)
    val s2 = lcAgg.createState(fb.apply_method)
    val srvb = new StagedRegionValueBuilder(EmitRegion.default(fb.apply_method), resType)

    val partitionIdx = fb.newField[Int]
    val nPart = fb.newField[Int]
    val soff = fb.newField[Long]
    val spec = CodecSpec.defaultUncompressed

    val serialized = fb.newField[Array[Array[Byte]]]
    val baos = fb.newField[ByteArrayOutputStream]
    val bais = fb.newField[ByteArrayInputStream]
    val ob = fb.newField[OutputBuffer]
    val ib = fb.newField[InputBuffer]

    fb.emit(
      Code(
        nPart := PArray(streamType).loadLength(r, off),
        partitionIdx := 0,
        serialized := Code.newArray[Array[Byte]](nPart),
        Code.whileLoop(partitionIdx < nPart,
          baos := Code.newInstance[ByteArrayOutputStream](),
          ob := spec.buildCodeOutputBuffer(baos),
          s.r := Code.newInstance[Region](),
          soff := PArray(streamType).loadElement(s.r, off, partitionIdx),
          initAndSeq(s, soff),
          s.serialize(spec)(ob),
          ob.invoke[Unit]("flush"),
          serialized.load().update(partitionIdx, baos.invoke[Array[Byte]]("toByteArray")),
          partitionIdx := partitionIdx + 1),
        bais := Code.newInstance[ByteArrayInputStream, Array[Byte]](serialized.load()(0)),
        ib := spec.buildCodeInputBuffer(bais),
        s.r := Code.newInstance[Region](),
        s.unserialize(spec)(ib),
        partitionIdx := 1,
        Code.whileLoop(partitionIdx < nPart,
          bais := Code.newInstance[ByteArrayInputStream, Array[Byte]](serialized.load()(partitionIdx)),
          ib := spec.buildCodeInputBuffer(bais),
          s2.r := Code.newInstance[Region](),
          s2.unserialize(spec)(ib),
          lcAgg.combOp(s, s2),
          partitionIdx := partitionIdx + 1),
        srvb.start(),
        lcAgg.result(s, srvb),
        srvb.offset))

    val aggf = fb.resultWithIndex()

    Region.scoped { region =>
      val offset = ScalaToRegionValue(region, TArray(streamType.virtualType), partitioned)
      val res = aggf(0, region)(region, offset)
      assert(SafeRow(resType, region, res) == Row(expected))
    }
  }

  @Test def testEmit() {
    val array = Ref("array", arrayType.virtualType)
    val idx = Ref("idx", TInt32())
    val elt = Ref("elt", rowType.virtualType)

    val spec = CodecSpec.defaultUncompressed
    val partitioned = value.grouped(3).toFastIndexedSeq

    val (_, initAndSeqF) = CompileWithAggregators2[Long, Unit](
      Array(lcAggSig),
      "stream", streamType,
      Begin(FastIndexedSeq(
        InitOp2(0, FastIndexedSeq(), lcAggSig),
        ArrayFor(Ref("stream", streamType.virtualType),
          array.name,
          Begin(FastIndexedSeq(
            SeqOp2(0, FastIndexedSeq(ArrayLen(array)), lcAggSig),
            ArrayFor(
              ArrayRange(I32(0), ArrayLen(array), I32(1)),
              idx.name,
              Let(elt.name,
                ArrayRef(array, idx),
                SeqOp2(0,
                  FastIndexedSeq(idx,
                    Begin(FastIndexedSeq(
                      SeqOp2(0, FastIndexedSeq(elt), pnnAggSig),
                      SeqOp2(1, FastIndexedSeq(), countAggSig),
                      SeqOp2(2, FastIndexedSeq(GetField(elt, "b")), sumAggSig)))),
                  eltAggSig)))))),
        SerializeAggs(0, 0, spec, FastIndexedSeq(lcAggSig)))))

    val (_, resultF) = CompileWithAggregators2[Long](
      Array(lcAggSig, lcAggSig), ResultOp2(0, FastIndexedSeq(lcAggSig)))

    val aggs = Region.scoped { region =>
      val f = initAndSeqF(0, region)

      partitioned.map { case lit =>
        val voff = ScalaToRegionValue(region, streamType.virtualType, lit)

        Region.scoped { aggRegion =>
          f.newAggState(aggRegion)
          f(region, voff, false)
          f.getSerializedAgg(0)
        }
      }
    }

    val (_, deserializeAndComb) = CompileWithAggregators2[Unit](
      Array(lcAggSig, lcAggSig),
      Begin(
        DeserializeAggs(0, 0, spec, FastIndexedSeq(lcAggSig)) +:
          Array.range(1, aggs.length).flatMap { i =>
            FastIndexedSeq(
              DeserializeAggs(1, i, spec, FastIndexedSeq(lcAggSig)),
              CombOp2(0, 1, lcAggSig))
          }))

    Region.scoped { region =>
      val comb = deserializeAndComb(0, region)
      val resF = resultF(0, region)

      Region.scoped { aggRegion =>
        comb.newAggState(aggRegion)
        aggs.zipWithIndex.foreach { case (agg, i) =>
          comb.setSerializedAgg(i, agg)
        }
        comb(region)
        resF.setAggState(aggRegion, comb.getAggOffset())
        val res = resF(region)

        assert(SafeRow(resType, region, res) == Row(expected))
      }
    }
  }
}
