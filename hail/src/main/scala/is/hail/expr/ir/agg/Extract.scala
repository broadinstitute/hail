package is.hail.expr.ir.agg

import is.hail.HailContext
import is.hail.annotations.{Region, RegionValue}
import is.hail.expr.ir
import is.hail.expr.ir._
import is.hail.expr.types.physical._
import is.hail.expr.types.virtual._
import is.hail.io.CodecSpec
import is.hail.rvd.{RVDContext, RVDType}
import is.hail.utils._

import scala.language.{existentials, postfixOps}

object TableMapIRNew {

  def apply(tv: TableValue, newRow: IR): TableValue = {
    val typ = tv.typ
    val gType = tv.globals.t

    val scanRef = genUID()
    val extracted = Extract.apply(CompileWithAggregators.liftScan(newRow), scanRef)
    val nAggs = extracted.nAggs

    if (extracted.aggs.isEmpty)
      throw new UnsupportedExtraction("no scans to extract in TableMapRows")

    val scanInitNeedsGlobals = Mentions(extracted.init, "global")
    val scanSeqNeedsGlobals = Mentions(extracted.seqPerElt, "global")
    val rowIterationNeedsGlobals = Mentions(extracted.postAggIR, "global")

    val globalsBc =
      if (rowIterationNeedsGlobals || scanInitNeedsGlobals || scanSeqNeedsGlobals)
        tv.globals.broadcast
      else
        null

    val spec = CodecSpec.defaultUncompressed
    val aggSlots = extracted.aggs ++ extracted.aggs

    // Order of operations:
    // 1. init op on all aggs and write out.
    // 2. load in init op on each partition, seq op over partition, write out.
    // 3. load in partition aggregations, comb op as necessary, write back out.
    // 4. load in partStarts, calculate newRow based on those results.

    val (_, initF) = ir.CompileWithAggregators2[Long, Unit](
      extracted.aggs,
      "global", gType,
      Begin(FastIndexedSeq(extracted.init, extracted.serializeSet(0, 0, spec))))

    val (_, deserializeFirstF) = ir.CompileWithAggregators2[Unit](
      aggSlots,
      extracted.deserializeSet(0, 0, spec))

    val (_, serializeFirstF) = ir.CompileWithAggregators2[Unit](
      aggSlots,
      extracted.serializeSet(0, 0, spec))

    val (_, eltSeqF) = ir.CompileWithAggregators2[Long, Long, Unit](
      aggSlots,
      "global", gType,
      "row", typ.rowType.physicalType,
      extracted.eltOp())

    val (_, combOpF) = ir.CompileWithAggregators2[Unit](
      aggSlots,
      Begin(
        extracted.deserializeSet(0, 0, spec) +:
        extracted.deserializeSet(1, 1, spec) +:
          Array.tabulate(nAggs)(i => CombOp2(i, nAggs + i, extracted.aggs(i))) :+
          extracted.serializeSet(0, 0, spec)))

    val (rTyp, f) = ir.CompileWithAggregators2[Long, Long, Long](
      aggSlots,
      "global", gType,
      "row", typ.rowType.physicalType,
      Let(scanRef, extracted.results, extracted.postAggIR))
    assert(rTyp.virtualType == newRow.typ)

    // 1. init op on all aggs and write out to initPath
    val initAgg = Region.scoped { aggRegion =>
      val init = initF(0)
      init.newAggState(aggRegion)
      Region.scoped { fRegion =>
        init(fRegion, tv.globals.value.offset, false)
      }
      init.getSerializedAgg(0)
    }

    // 2. load in init op on each partition, seq op over partition, write out.
    val scanPartitionAggs = SpillingCollectIterator(tv.rvd.mapPartitionsWithIndex { (i, ctx, it) =>
      val globalRegion = ctx.freshRegion
      val globals = if (scanSeqNeedsGlobals) globalsBc.value.readRegionValue(globalRegion) else 0

      Region.smallScoped { aggRegion =>
        val init = deserializeFirstF(i)
        val seq = eltSeqF(i)
        val write = serializeFirstF(i)
        init.newAggState(aggRegion)
        init.setSerializedAgg(0, initAgg)
        init(globalRegion)
        seq.setAggState(aggRegion, init.getAggOffset())
        it.foreach { rv =>
          seq(rv.region, globals, false, rv.offset, false)
          ctx.region.clear()
        }
        write.setAggState(aggRegion, seq.getAggOffset())
        write(globalRegion)
        Iterator.single(write.getSerializedAgg(0))

      }
    }, HailContext.get.flags.get("max_leader_scans").toInt)


    // 3. load in partition aggregations, comb op as necessary, write back out.
    val combOp = combOpF(0)

    var i = 0
    val partAggs = scanPartitionAggs.scanLeft(initAgg) { case (prev, current) =>
      i += 1
      Region.scoped { agg2 =>
        combOp.newAggState(agg2)
        combOp.setSerializedAgg(0, prev)
        combOp.setSerializedAgg(1, current)
        Region.scoped { fRegion =>
          combOp(fRegion)
        }
        combOp.getSerializedAgg(0)
      }
    }

    // 4. load in partStarts, calculate newRow based on those results.
    val itF = { (i: Int, ctx: RVDContext, partitionAggs: Array[Byte], it: Iterator[RegionValue]) =>
      val globalRegion = ctx.freshRegion
      val globals = if (rowIterationNeedsGlobals || scanSeqNeedsGlobals)
        globalsBc.value.readRegionValue(globalRegion)
      else
        0

      val aggRegion = ctx.freshRegion
      val read = deserializeFirstF(i)
      val newRow = f(i)
      val seq = eltSeqF(i)
      read.newAggState(aggRegion)
      read.setSerializedAgg(0, partitionAggs)
      read(globalRegion)
      var aggOff = read.getAggOffset()

      it.map { rv =>
        newRow.setAggState(aggRegion, aggOff)
        rv.setOffset(newRow(rv.region, globals, false, rv.offset, false))
        seq.setAggState(aggRegion, newRow.getAggOffset())
        seq(rv.region, globals, false, rv.offset, false)
        aggOff = seq.getAggOffset()
        rv
      }
    }
    tv.copy(
      typ = typ.copy(rowType = rTyp.virtualType.asInstanceOf[TStruct]),
      rvd = tv.rvd.mapPartitionsWithIndexAndValue(RVDType(rTyp.asInstanceOf[PStruct], typ.key), partAggs.toArray, itF))
  }
}

class UnsupportedExtraction(msg: String) extends Exception(msg)

case class Aggs(postAggIR: IR, init: IR, seqPerElt: IR, aggs: Array[AggSignature]) {
  val typ: PTuple = PTuple(aggs.map(Extract.getPType))
  val nAggs: Int = aggs.length

  def deserializeSet(i: Int, i2: Int, spec: CodecSpec): IR =
    DeserializeAggs(i * nAggs, i2, spec, aggs)

  def serializeSet(i: Int, i2: Int, spec: CodecSpec): IR =
    SerializeAggs(i * nAggs, i2, spec, aggs)

  def readSet(i: Int, path: IR, spec: CodecSpec): IR =
    ReadAggs(i * nAggs, path, spec, aggs)

  def writeSet(i: Int, path: IR, spec: CodecSpec): IR =
    WriteAggs(i * nAggs, path, spec, aggs)

  def eltOp(optimize: Boolean = true): IR = if (optimize) Optimize(seqPerElt) else seqPerElt

  def results: IR = ResultOp2(0, aggs)
}

object Extract {

  def addLets(ir: IR, lets: Array[AggLet]): IR = {
    assert(lets.areDistinct())
    lets.foldRight[IR](ir) { case (al, comb) => Let(al.name, al.value, comb)}
  }

  def compatible(sig1: AggSignature, sig2: AggSignature): Boolean = (sig1.op, sig2.op) match {
    case (AggElements2(nestedAggs1), AggElements2(nestedAggs2)) =>
      nestedAggs1.zip(nestedAggs2).forall { case (a1, a2) => compatible(a1, a2) }
    case (AggElementsLengthCheck2(nestedAggs1, _), AggElements2(nestedAggs2)) =>
      nestedAggs1.zip(nestedAggs2).forall { case (a1, a2) => compatible(a1, a2) }
    case (AggElements2(nestedAggs1), AggElementsLengthCheck2(nestedAggs2, _)) =>
      nestedAggs1.zip(nestedAggs2).forall { case (a1, a2) => compatible(a1, a2) }
    case (AggElementsLengthCheck2(nestedAggs1, _), AggElementsLengthCheck2(nestedAggs2, _)) =>
      nestedAggs1.zip(nestedAggs2).forall { case (a1, a2) => compatible(a1, a2) }
    case _ => sig1 == sig2
  }

  def getAgg(aggSig: AggSignature): StagedRegionValueAggregator = aggSig match {
    case AggSignature(Sum(), _, _, Seq(t)) =>
      new SumAggregator(t.physicalType)
    case AggSignature(Count(), _, _, _) =>
      CountAggregator
    case AggSignature(AggElementsLengthCheck2(nestedAggs, knownLength), _, _, _) =>
      new ArrayElementLengthCheckAggregator(nestedAggs.map(getAgg).toArray, knownLength)
    case AggSignature(AggElements2(nestedAggs), _, _, _) =>
      new ArrayElementwiseOpAggregator(nestedAggs.map(getAgg).toArray)
    case AggSignature(PrevNonnull(), _, _, Seq(t)) =>
      new PrevNonNullAggregator(t.physicalType)
    case _ => throw new UnsupportedExtraction(aggSig.toString)
  }

  def getPType(aggSig: AggSignature): PType = getAgg(aggSig).resultType

  def getType(aggSig: AggSignature): Type = getPType(aggSig).virtualType


  def apply(ir: IR, resultName: String): Aggs = {
    val ab = new ArrayBuilder[(AggSignature, IndexedSeq[IR])]()
    val seq = new ArrayBuilder[IR]()
    val let = new ArrayBuilder[AggLet]()
    val ref = Ref(resultName, null)
    val postAgg = extract(ir, ab, seq, let, ref)
    val (aggs, initArgs) = ab.result().unzip
    val rt = TTuple(aggs.map(Extract.getType): _*)
    ref._typ = rt

    val initOps = Array.tabulate(initArgs.length)(i => InitOp2(i, initArgs(i), aggs(i)))
    Aggs(postAgg, Begin(initOps), Begin(seq.result()), aggs)
  }

  private def extract(ir: IR, ab: ArrayBuilder[(AggSignature, IndexedSeq[IR])], seqBuilder: ArrayBuilder[IR], letBuilder: ArrayBuilder[AggLet], result: IR): IR = {
    def extract(node: IR): IR = this.extract(node, ab, seqBuilder, letBuilder, result)

    ir match {
      case Ref(name, typ) =>
        assert(typ.isRealizable)
        ir
      case x@AggLet(name, value, body, _) =>
        letBuilder += x
        extract(body)
      case x: ApplyAggOp =>
        val i = ab.length
        ab += x.aggSig -> (x.constructorArgs ++ x.initOpArgs.getOrElse[IndexedSeq[IR]](FastIndexedSeq()))
        seqBuilder += SeqOp2(i, x.seqOpArgs, x.aggSig)
        GetTupleElement(result, i)
      case AggFilter(cond, aggIR, _) =>
        val newSeq = new ArrayBuilder[IR]()
        val newLet = new ArrayBuilder[AggLet]()
        val transformed = this.extract(aggIR, ab, newSeq, newLet, result)

        seqBuilder += If(cond, addLets(Begin(newSeq.result()), newLet.result()), Begin(FastIndexedSeq[IR]()))
        transformed

      case AggExplode(array, name, aggBody, _) =>
        val newSeq = new ArrayBuilder[IR]()
        val newLet = new ArrayBuilder[AggLet]()
        val transformed = this.extract(aggBody, ab, newSeq, newLet, result)

        val (dependent, independent) = newLet.result().partition(l => Mentions(l.value, name))
        letBuilder ++= independent
        seqBuilder += ArrayFor(array, name, addLets(Begin(newSeq.result()), dependent))
        transformed

      case AggGroupBy(key, aggIR, _) =>
        throw new UnsupportedExtraction("group by")

      case AggArrayPerElement(a, elementName, indexName, aggBody, knownLength, _) =>
        val newAggs = new ArrayBuilder[(AggSignature, IndexedSeq[IR])]()
        val newSeq = new ArrayBuilder[IR]()
        val newLet = new ArrayBuilder[AggLet]()
        val newRef = Ref(genUID(), null)
        val transformed = this.extract(aggBody, newAggs, newSeq, newLet, newRef)

        val (dependent, independent) = newLet.result().partition(l => Mentions(l.value, elementName))
        letBuilder ++= independent

        val i = ab.length
        val (aggs, inits) = newAggs.result().unzip
        val rt = TArray(TTuple(aggs.map(Extract.getType): _*))
        newRef._typ = -rt.elementType
        val initArgs = knownLength.map(FastIndexedSeq(_)).getOrElse[IndexedSeq[IR]](FastIndexedSeq()) ++ inits.flatten.toFastIndexedSeq

        val aggSigCheck = AggSignature(AggElementsLengthCheck2(aggs, knownLength.isDefined), FastSeq[Type](),
          Some(initArgs.map(_.typ)), FastSeq(TInt32()))
        val aggSig = AggSignature(AggElements2(aggs), FastSeq[Type](), None, FastSeq(TInt32(), TVoid))

        val aRef = Ref(genUID(), a.typ)
        val iRef = Ref(genUID(), TInt32())

        ab += aggSigCheck -> initArgs
        seqBuilder +=
          Let(
            aRef.name, a,
            Begin(FastIndexedSeq(
              SeqOp2(i, FastIndexedSeq(ArrayLen(aRef)), aggSigCheck),
              ArrayFor(
                ArrayRange(I32(0), ArrayLen(aRef), I32(1)),
                iRef.name,
                Let(
                  elementName,
                  ArrayRef(aRef, iRef),
                  addLets(SeqOp2(i,
                    FastIndexedSeq(iRef, Begin(newSeq.result().toFastIndexedSeq)),
                    aggSig), dependent))))))

        val rUID = Ref(genUID(), rt)
        Let(
          rUID.name,
          GetTupleElement(result, i),
          ArrayMap(
            ArrayRange(0, ArrayLen(rUID), 1),
            indexName,
            Let(
              newRef.name,
              ArrayRef(rUID, Ref(indexName, TInt32())),
              transformed)))

      case x: ArrayAgg =>
        throw new UnsupportedExtraction("array agg")
      case x: ArrayAggScan =>
        throw new UnsupportedExtraction("array scan")
      case _ => MapIR(extract)(ir)
    }
  }
}
