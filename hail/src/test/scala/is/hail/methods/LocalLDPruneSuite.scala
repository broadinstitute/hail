package is.hail.methods

import breeze.linalg.{Vector => BVector}
import is.hail.{SparkSuite, TestUtils}
import is.hail.annotations.{Annotation, Region, RegionValue, RegionValueBuilder}
import is.hail.check.Prop._
import is.hail.check.{Gen, Properties}
import is.hail.expr.types._
import is.hail.expr.types.virtual.{TArray, TLocus, TString, TStruct}
import is.hail.variant._
import is.hail.utils._
import is.hail.testUtils._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.testng.annotations.Test
import is.hail.table.Table
import org.apache.spark.rdd.RDD

case class BitPackedVector(gs: Array[Long], nSamples: Int, mean: Double, stdDevRec: Double) {
  def unpack(): Array[Int] = {
    val gts = Array.ofDim[Int](nSamples)
    val nPacks = gs.length

    var packIndex = 0
    var i = 0
    val shiftInit = LocalLDPrune.genotypesPerPack * 2 - 2
    while (packIndex < nPacks && i < nSamples) {
      val l = gs(packIndex)
      var shift = shiftInit
      while (shift >= 0 && i < nSamples) {
        val gt = (l >> shift) & 3
        if (gt == 3)
          gts(i) = -1
        else
          gts(i) = gt.toInt
        shift -= 2
        i += 1
      }
      packIndex += 1
    }

    gts
  }
}

object LocalLDPruneSuite {
  val variantByteOverhead = 50
  val fractionMemoryToUse = 0.25
  val genotypesPerPack = LocalLDPrune.genotypesPerPack

  val rvRowType = TStruct(
    "locus" -> ReferenceGenome.GRCh37.locusType,
    "alleles" -> TArray(TString()),
    MatrixType.entriesIdentifier -> TArray(Genotype.htsGenotypeType)
  )

  val bitPackedVectorViewType = BitPackedVectorView.rvRowType(rvRowType.field("locus").typ,
    rvRowType.field("alleles").typ)

  def makeRV(gs: Iterable[Annotation]): RegionValue = {
    val gArr = gs.toFastIndexedSeq
    val rvb = new RegionValueBuilder(Region())
    rvb.start(rvRowType.physicalType)
    rvb.startStruct()
    rvb.addAnnotation(rvRowType.types(0), Locus("1", 1))
    rvb.addAnnotation(rvRowType.types(1), IndexedSeq("A", "T"))
    rvb.addAnnotation(TArray(Genotype.htsGenotypeType), gArr)
    rvb.endStruct()
    rvb.end()
    rvb.result()
  }

  def convertCallsToGs(calls: Array[BoxedCall]): Iterable[Annotation] = calls.map(Genotype(_)).toIterable

  // expecting iterable of Genotype with htsjdk schema
  def toBitPackedVectorView(gs: Iterable[Annotation], nSamples: Int): Option[BitPackedVectorView] = {
    val bpvv = new BitPackedVectorView(bitPackedVectorViewType)
    toBitPackedVectorRegionValue(gs, nSamples) match {
      case Some(rv) =>
        bpvv.setRegion(rv)
        Some(bpvv)
      case None => None
    }
  }

  // expecting iterable of Genotype with htsjdk schema
  def toBitPackedVectorRegionValue(gs: Iterable[Annotation], nSamples: Int): Option[RegionValue] = {
    toBitPackedVectorRegionValue(makeRV(gs), nSamples)
  }

  def toBitPackedVectorRegionValue(rv: RegionValue, nSamples: Int): Option[RegionValue] = {
    val rvb = new RegionValueBuilder(Region())
    val hcView = HardCallView(rvRowType.physicalType)
    hcView.setRegion(rv)

    rvb.start(bitPackedVectorViewType)
    rvb.startStruct()
    rvb.addAnnotation(rvRowType.types(0), Locus("1", 1))
    rvb.addAnnotation(rvRowType.types(1), IndexedSeq("A", "T"))
    val keep = LocalLDPrune.addBitPackedVector(rvb, hcView, nSamples)

    if (keep) {
      rvb.endStruct()
      rvb.end()
      Some(rvb.result())
    }
    else
      None
  }

  def toBitPackedVector(calls: Array[BoxedCall]): Option[BitPackedVector] = {
    val nSamples = calls.length
    toBitPackedVectorView(convertCallsToGs(calls), nSamples).map { bpvv =>
      BitPackedVector((0 until bpvv.getNPacks).map(bpvv.getPack).toArray, bpvv.getNSamples, bpvv.getMean, bpvv.getCenteredLengthRec)
    }
  }

  def correlationMatrix(gts: Array[Iterable[Annotation]], nSamples: Int) = {
    val bvi = gts.map { gs => LocalLDPruneSuite.toBitPackedVectorView(gs, nSamples) }
    val r2 = for (i <- bvi.indices; j <- bvi.indices) yield {
      (bvi(i), bvi(j)) match {
        case (Some(x), Some(y)) =>
          Some(LocalLDPrune.computeR2(x, y))
        case _ => None
      }
    }
    val nVariants = bvi.length
    new MultiArray2(nVariants, nVariants, r2.toArray)
  }

  def estimateMemoryRequirements(nVariants: Long, nSamples: Int, memoryPerCore: Long): Int = {
    val bytesPerVariant = math.ceil(8 * nSamples.toDouble / genotypesPerPack).toLong + variantByteOverhead
    val memoryAvailPerCore = memoryPerCore * fractionMemoryToUse

    val maxQueueSize = math.max(1, math.ceil(memoryAvailPerCore / bytesPerVariant).toInt)
    maxQueueSize
  }
}

class LocalLDPruneSuite extends SparkSuite {
  def toC2(i: Int): BoxedCall = if (i == -1) null else Call2.fromUnphasedDiploidGtIndex(i)

  @Test def testBitPackUnpack() {
    val calls1 = Array(-1, 0, 1, 2, 1, 1, 0, 0, 0, 0, 2, 2, -1, -1, -1, -1).map(toC2)
    val calls2 = Array(0, 1, 2, 2, 2, 0, -1, -1).map(toC2)
    val calls3 = calls1 ++ Array.ofDim[Int](32 - calls1.length).map(toC2) ++ calls2

    for (calls <- Array(calls1, calls2, calls3)) {
      assert(LocalLDPruneSuite.toBitPackedVector(calls).forall { bpv =>
        bpv.unpack().map(toC2) sameElements calls
      })
    }
  }

  @Test def testR2() {
    val calls = Array(
      Array(1, 0, 0, 0, 0, 0, 0, 0).map(toC2),
      Array(1, 1, 1, 1, 1, 1, 1, 1).map(toC2),
      Array(1, 2, 2, 2, 2, 2, 2, 2).map(toC2),
      Array(1, 0, 0, 0, 1, 1, 1, 1).map(toC2),
      Array(1, 0, 0, 0, 1, 1, 2, 2).map(toC2),
      Array(1, 0, 1, 1, 2, 2, 0, 1).map(toC2),
      Array(1, 0, 1, 0, 2, 2, 1, 1).map(toC2)
    )

    val actualR2 = new MultiArray2(7, 7, hadoopConf.readLines("src/test/resources/ldprune_corrtest.txt")(_.flatMap(_.map { line =>
      line.trim.split("\t").map(r2 => if (r2 == "NA") None else Some(r2.toDouble))
    }.value).toArray))

    val computedR2 = LocalLDPruneSuite.correlationMatrix(calls.map(LocalLDPruneSuite.convertCallsToGs), 8)

    val isSame = actualR2.indices.forall { case (i, j) =>
      val expected = actualR2(i, j)
      val computed = computedR2(i, j)

      (computed, expected) match {
        case (Some(x), Some(y)) =>
          val isSame = D_==(x, y)
          if (!isSame)
            info(s"i=$i j=$j r2Computed=$x r2Expected=$y")
          isSame
        case (None, None) => true
        case _ =>
          info(s"i=$i j=$j r2Computed=$computed r2Expected=$expected")
          false
      }
    }

    assert(isSame)

    val input = Array(0, 1, 2, 2, 2, 0, -1, -1).map(toC2)
    val bvi1 = LocalLDPruneSuite.toBitPackedVectorView(LocalLDPruneSuite.convertCallsToGs(input), input.length).get
    val bvi2 = LocalLDPruneSuite.toBitPackedVectorView(LocalLDPruneSuite.convertCallsToGs(input), input.length).get

    assert(D_==(LocalLDPrune.computeR2(bvi1, bvi2), 1d))
  }

  object Spec extends Properties("LDPrune") {
    val vectorGen = for (nSamples: Int <- Gen.choose(1, 1000);
    v1: Array[BoxedCall] <- Gen.buildableOfN[Array](nSamples, Gen.choose(-1, 2).map(toC2));
    v2: Array[BoxedCall] <- Gen.buildableOfN[Array](nSamples, Gen.choose(-1, 2).map(toC2))
    ) yield (nSamples, v1, v2)

    property("bitPacked pack and unpack give same as orig") =
      forAll(vectorGen) { case (nSamples: Int, v1: Array[BoxedCall], _) =>
        val bpv = LocalLDPruneSuite.toBitPackedVector(v1)

        bpv match {
          case Some(x) => LocalLDPruneSuite.toBitPackedVector(x.unpack().map(toC2)).get.gs sameElements x.gs
          case None => true
        }
      }

    property("R2 bitPacked same as BVector") =
      forAll(vectorGen) { case (nSamples: Int, v1: Array[BoxedCall], v2: Array[BoxedCall]) =>
        val v1Ann = LocalLDPruneSuite.convertCallsToGs(v1)
        val v2Ann = LocalLDPruneSuite.convertCallsToGs(v2)

        val bv1 = LocalLDPruneSuite.toBitPackedVectorView(v1Ann, nSamples)
        val bv2 = LocalLDPruneSuite.toBitPackedVectorView(v2Ann, nSamples)

        val view = HardCallView(LocalLDPruneSuite.rvRowType.physicalType)

        val rv1 = LocalLDPruneSuite.makeRV(v1Ann)
        view.setRegion(rv1)
        val sgs1 = TestUtils.normalizedHardCalls(view, nSamples).map(math.sqrt(1d / nSamples) * BVector(_))

        val rv2 = LocalLDPruneSuite.makeRV(v2Ann)
        view.setRegion(rv2)
        val sgs2 = TestUtils.normalizedHardCalls(view, nSamples).map(math.sqrt(1d / nSamples) * BVector(_))

        (bv1, bv2, sgs1, sgs2) match {
          case (Some(a), Some(b), Some(c: BVector[Double]), Some(d: BVector[Double])) =>
            val rBreeze = c.dot(d): Double
            val r2Breeze = rBreeze * rBreeze
            val r2BitPacked = LocalLDPrune.computeR2(a, b)

            val isSame = D_==(r2BitPacked, r2Breeze) && D_>=(r2BitPacked, 0d) && D_<=(r2BitPacked, 1d)
            if (!isSame) {
              println(s"breeze=$r2Breeze bitPacked=$r2BitPacked nSamples=$nSamples")
            }
            isSame
          case _ => true
        }
      }
  }

  @Test def testRandom() {
    Spec.check()
  }

  @Test def bitPackedVectorCorrectWhenOffsetNotZero() {
    Region.scoped { r =>
      val rvb = new RegionValueBuilder(r)
      val t = BitPackedVectorView.rvRowType(
        +TLocus(ReferenceGenome.GRCh37),
        +TArray(+TString()))
      val bpv = new BitPackedVectorView(t)
      r.appendInt(0xbeef)
      rvb.start(t)
      rvb.startStruct()
      rvb.startStruct()
      rvb.addString("X")
      rvb.addInt(42)
      rvb.endStruct()
      rvb.startArray(0)
      rvb.endArray()
      rvb.startArray(0)
      rvb.endArray()
      rvb.addInt(0)
      rvb.addDouble(0.0)
      rvb.addDouble(0.0)
      rvb.endStruct()
      bpv.setRegion(r, rvb.end())
      assert(bpv.getContig == "X")
      assert(bpv.getStart == 42)
    }
  }
}
