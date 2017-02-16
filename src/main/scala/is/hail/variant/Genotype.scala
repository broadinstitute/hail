package is.hail.variant

import java.util

import org.apache.commons.lang3.builder.HashCodeBuilder
import org.apache.commons.math3.distribution.BinomialDistribution
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import is.hail.utils._
import is.hail.check.{Arbitrary, Gen}
import is.hail.expr.{TArray, TBoolean, TInt, TStruct, Type}
import is.hail.utils.ByteIterator
import org.json4s._

import scala.collection.mutable
import scala.language.implicitConversions

object GenotypeType extends Enumeration {
  type GenotypeType = Value
  val HomRef = Value(0)
  val Het = Value(1)
  val HomVar = Value(2)
  val NoCall = Value(-1)
}

import is.hail.variant.GenotypeType.GenotypeType

object GTPair {
  def apply(j: Int, k: Int): GTPair = {
    require(j >= 0 && j <= 0xffff, s"GTPair invalid j value $j")
    require(k >= 0 && k <= 0xffff, s"GTPair invalid k value $k")
    new GTPair(j | (k << 16))
  }

  def fromNonNormalized(j: Int, k: Int): GTPair = {
    if (j <= k)
      GTPair(j, k)
    else
      GTPair(k, j)
  }
}

class GTPair(val p: Int) extends AnyVal {
  def j: Int = p & 0xffff

  def k: Int = (p >> 16) & 0xffff

  def nNonRefAlleles: Int =
    (if (j != 0) 1 else 0) + (if (k != 0) 1 else 0)

  def alleleIndices: Array[Int] = {
    Array(this.j, this.k)
  }
}

abstract class Genotype extends Serializable {
  def fakeRef: Boolean

  def isDosage: Boolean

  def check(nAlleles: Int) {
    val nGenotypes = triangle(nAlleles)
    assert(gt.forall(i => i >= 0 && i < nGenotypes))
    assert(ad.forall(a => a.length == nAlleles))
    assert(px.forall(a => a.length == nGenotypes))
  }

  def copy(gt: Option[Int] = this.gt,
    ad: Option[Array[Int]] = this.ad,
    dp: Option[Int] = this.dp,
    gq: Option[Int] = this.gq,
    px: Option[Array[Int]] = this.px,
    fakeRef: Boolean = this.fakeRef,
    isDosage: Boolean = this.isDosage): Genotype = Genotype(gt, ad, dp, gq, px, fakeRef, isDosage)


  override def equals(that: Any): Boolean = that match {
    case g: Genotype =>
      unboxedGT == g.unboxedGT &&
        ((ad.isEmpty && g.ad.isEmpty) || (ad.isDefined && g.ad.isDefined && ad.get.sameElements(g.ad.get))) &&
        dp == g.dp &&
        gq == g.gq &&
        ((px.isEmpty && g.px.isEmpty) || (px.isDefined && g.px.isDefined && px.get.sameElements(g.px.get))) &&
        fakeRef == g.fakeRef &&
        isDosage == g.isDosage

    case _ => false
  }

  override def hashCode: Int =
    new HashCodeBuilder(43, 19)
      .append(unboxedGT)
      .append(util.Arrays.hashCode(ad.orNull))
      .append(dp)
      .append(gq)
      .append(util.Arrays.hashCode(px.orNull))
      .append(fakeRef)
      .append(isDosage)
      .toHashCode

  def unboxedGT: Int

  def gt: Option[Int] =
    if (unboxedGT >= 0)
      Some(unboxedGT)
    else
      None

  def ad: Option[Array[Int]]

  def dp: Option[Int]

  def od: Option[Int]

  def gq: Option[Int]

  def px: Option[Array[Int]]

  def pl: Option[Array[Int]]

  def dosage: Option[Array[Double]]

  def isHomRef: Boolean = Genotype.isHomRef(unboxedGT)

  def isHet: Boolean = Genotype.isHet(unboxedGT)

  def isHomVar: Boolean = Genotype.isHomVar(unboxedGT)

  def isCalledNonRef: Boolean = Genotype.isCalledNonRef(unboxedGT)

  def isHetNonRef: Boolean = Genotype.isHetNonRef(unboxedGT)

  def isHetRef: Boolean = Genotype.isHetRef(unboxedGT)

  def isNotCalled: Boolean = Genotype.isNotCalled(unboxedGT)

  def isCalled: Boolean = Genotype.isCalled(unboxedGT)

  def gtType: GenotypeType = Genotype.gtType(unboxedGT)

  def nNonRefAlleles: Option[Int] = Genotype.nNonRefAlleles(unboxedGT)

  def oneHotAlleles(nAlleles: Int): Option[IndexedSeq[Int]] = {
    gt.map { call =>
      val gtPair = Genotype.gtPair(call)
      val j = gtPair.j
      val k = gtPair.k
      new IndexedSeq[Int] {
        def length: Int = nAlleles

        def apply(idx: Int): Int = {
          if (idx < 0 || idx >= nAlleles)
            throw new ArrayIndexOutOfBoundsException(idx)
          var r = 0
          if (idx == j)
            r += 1
          if (idx == k)
            r += 1
          r
        }
      }
    }
  }

  def oneHotAlleles(v: Variant): Option[IndexedSeq[Int]] = oneHotAlleles(v.nAlleles)

  def oneHotGenotype(v: Variant): Option[IndexedSeq[Int]] = oneHotGenotype(v.nGenotypes)

  def oneHotGenotype(nGenotypes: Int): Option[IndexedSeq[Int]] = {
    gt.map { call =>
      new IndexedSeq[Int] {
        def length: Int = nGenotypes

        def apply(idx: Int): Int = {
          if (idx < 0 || idx >= nGenotypes)
            throw new ArrayIndexOutOfBoundsException(idx)
          if (idx == call)
            1
          else
            0
        }
      }
    }
  }

  override def toString: String = {
    val b = new StringBuilder

    b.append(gt.map { gt =>
      val p = Genotype.gtPair(gt)
      s"${ p.j }/${ p.k }"
    }.getOrElse("./."))

    if (fakeRef) {
      b += '*'
    }

    b += ':'
    b.append(ad.map(_.mkString(",")).getOrElse("."))
    b += ':'
    b.append(dp.map(_.toString).getOrElse("."))
    b += ':'
    b.append(gq.map(_.toString).getOrElse("."))
    b += ':'
    if (!isDosage) {
      b.append("PL=" + pl.map(_.mkString(",")).getOrElse("."))
    } else {
      b.append("GP=" + dosage.map(_.mkString(",")).getOrElse("."))
    }

    b.result()
  }

  def pAB(theta: Double = 0.5): Option[Double] = ad.flatMap { arr =>
    if (isHet) {
      val gtPair = Genotype.gtPair(unboxedGT)
      val aDepth = arr(gtPair.j)
      val bDepth = arr(gtPair.k)
      val d = new BinomialDistribution(aDepth + bDepth, theta)
      val minDepth = aDepth.min(bDepth)
      val minp = d.probability(minDepth)
      val mincp = d.cumulativeProbability(minDepth)
      Some((2 * mincp - minp).min(1.0).max(0.0))
    } else None
  }

  def fractionReadsRef(): Option[Double] = ad.flatMap {
    arr => divOption(arr(0), arr.sum)
  }

  def toRow: Row =
    Row(gt.orNull, ad.map(_.toSeq).orNull, dp.orNull, gq.orNull, px.map(_.toSeq).orNull, fakeRef, isDosage)

  def toJSON: JValue = JObject(
    ("gt", gt.map(JInt(_)).getOrElse(JNull)),
    ("ad", ad.map(ads => JArray(ads.map(JInt(_)).toList)).getOrElse(JNull)),
    ("dp", dp.map(JInt(_)).getOrElse(JNull)),
    ("gq", gq.map(JInt(_)).getOrElse(JNull)),
    ("px", px.map(pxs => JArray(pxs.map(JInt(_)).toList)).getOrElse(JNull)),
    ("fakeRef", JBool(fakeRef)),
    ("isDosage", JBool(isDosage)))
}

object Genotype {
  def apply(gtx: Int): Genotype = new GenericGenotype(gtx, null, -1, -1, null, false, false)

  def apply(gt: Option[Int], fakeRef: Boolean): Genotype =
    new GenericGenotype(gt.getOrElse(-1), null, -1, -1, null, fakeRef, false)

  def apply(gt: Option[Int] = None,
    ad: Option[Array[Int]] = None,
    dp: Option[Int] = None,
    gq: Option[Int] = None,
    px: Option[Array[Int]] = None,
    fakeRef: Boolean = false,
    isDosage: Boolean = false): Genotype = {
    new GenericGenotype(gt.getOrElse(-1), ad.map(_.toArray).orNull, dp.getOrElse(-1), gq.getOrElse(-1), px.map(_.toArray).orNull, fakeRef, isDosage)
  }

  def schema: DataType = StructType(Array(
    StructField("gt", IntegerType),
    StructField("ad", ArrayType(IntegerType)),
    StructField("dp", IntegerType),
    StructField("gq", IntegerType),
    StructField("px", ArrayType(IntegerType)),
    StructField("fakeRef", BooleanType),
    StructField("isDosage", BooleanType)))

  def t: Type = TStruct(
    "gt" -> TInt,
    "ad" -> TArray(TInt),
    "dp" -> TInt,
    "gq" -> TInt,
    "px" -> TArray(TInt),
    "fakeRef" -> TBoolean,
    "isDosage" -> TBoolean)

  final val flagMultiHasGTBit = 0x1
  final val flagMultiGTRefBit = 0x2
  final val flagBiGTMask = 0x3
  final val flagHasADBit = 0x4
  final val flagHasDPBit = 0x8
  final val flagHasGQBit = 0x10
  final val flagHasPLBit = 0x20
  final val flagSimpleADBit = 0x40
  final val flagSimpleDPBit = 0x80
  final val flagSimpleGQBit = 0x100
  final val flagFakeRefBit = 0x200

  def flagHasGT(isBiallelic: Boolean, flags: Int) =
    if (isBiallelic)
      (flags & flagBiGTMask) != 0
    else
      (flags & flagMultiHasGTBit) != 0

  def flagStoresGT(isBiallelic: Boolean, flags: Int) =
    isBiallelic || ((flags & flagMultiGTRefBit) != 0)

  def flagGT(isBiallelic: Boolean, flags: Int) = {
    assert(flagStoresGT(isBiallelic, flags))
    if (isBiallelic)
      (flags & flagBiGTMask) - 1
    else {
      assert((flags & flagMultiGTRefBit) != 0)
      0
    }
  }

  def flagSetGT(isBiallelic: Boolean, flags: Int, gt: Int): Int = {
    if (isBiallelic) {
      assert(gt >= 0 && gt <= 2)
      flags | ((gt & flagBiGTMask) + 1)
    } else {
      if (gt == 0)
        flags | flagMultiHasGTBit | flagMultiGTRefBit
      else
        flags | flagMultiHasGTBit
    }
  }

  def flagHasAD(flags: Int): Boolean = (flags & flagHasADBit) != 0

  def flagHasDP(flags: Int): Boolean = (flags & flagHasDPBit) != 0

  def flagHasGQ(flags: Int): Boolean = (flags & flagHasGQBit) != 0

  def flagHasPX(flags: Int): Boolean = (flags & flagHasPLBit) != 0

  def flagSetHasAD(flags: Int): Int = flags | flagHasADBit

  def flagSetHasDP(flags: Int): Int = flags | flagHasDPBit

  def flagSetHasGQ(flags: Int): Int = flags | flagHasGQBit

  def flagSetHasPX(flags: Int): Int = flags | flagHasPLBit

  def flagSimpleAD(flags: Int): Boolean = (flags & flagSimpleADBit) != 0

  def flagSimpleDP(flags: Int): Boolean = (flags & flagSimpleDPBit) != 0

  def flagSimpleGQ(flags: Int): Boolean = (flags & flagSimpleGQBit) != 0

  def flagSetSimpleAD(flags: Int): Int = flags | flagSimpleADBit

  def flagSetSimpleDP(flags: Int): Int = flags | flagSimpleDPBit

  def flagSetSimpleGQ(flags: Int): Int = flags | flagSimpleGQBit

  def flagFakeRef(flags: Int): Boolean = (flags & flagFakeRefBit) != 0

  def flagSetFakeRef(flags: Int): Int = flags | flagFakeRefBit

  def flagUnsetFakeRef(flags: Int): Int = flags ^ flagFakeRefBit

  def isHomRef(gt: Int): Boolean = gt == 0

  def isHet(gt: Int): Boolean = gt > 0 && {
    val p = Genotype.gtPair(gt)
    p.j != p.k
  }

  def isHomVar(gt: Int): Boolean = gt > 0 && {
    val p = Genotype.gtPair(gt)
    p.j == p.k
  }

  def isCalledNonRef(gt: Int): Boolean = gt > 0

  def isHetNonRef(gt: Int): Boolean = gt > 0 && {
    val p = Genotype.gtPair(gt)
    p.j > 0 && p.j != p.k
  }

  def isHetRef(gt: Int): Boolean = gt > 0 && {
    val p = Genotype.gtPair(gt)
    p.j == 0 && p.k > 0
  }

  def isNotCalled(gt: Int): Boolean = gt == -1

  def isCalled(gt: Int): Boolean = gt >= 0

  def gtType(gt: Int): GenotypeType =
    if (isHomRef(gt))
      GenotypeType.HomRef
    else if (isHet(gt))
      GenotypeType.Het
    else if (isHomVar(gt))
      GenotypeType.HomVar
    else {
      assert(isNotCalled(gt))
      GenotypeType.NoCall
    }

  def nNonRefAlleles(gt: Int): Option[Int] =
    if (gt >= 0)
      Some(Genotype.gtPair(gt).nNonRefAlleles)
    else
      None

  def gqFromPL(pl: Array[Int]): Int = {
    var m = 99
    var m2 = 99
    var i = 0
    while (i < pl.length) {
      if (pl(i) < m) {
        m2 = m
        m = pl(i)
      } else if (pl(i) < m2)
        m2 = pl(i)
      i += 1
    }
    assert(m == 0, s"$m, $m2, [${ pl.mkString(",") }]")
    m2
  }

  def gtFromLinear(a: Array[Int]): Option[Int] = {
    def f(i: Int, m: Int, mi: Int, count: Int): Option[Int] = {
      if (i == a.length) {
        assert(count >= 1)
        if (count == 1)
          Some(mi)
        else
          None
      } else if (a(i) > m)
        f(i + 1, a(i), i, 1)
      else if (a(i) == m)
        f(i + 1, m, mi, count + 1)
      else
        f(i + 1, m, mi, count)
    }

    f(0, 0, a(0), 1)
  }

  def weightsToLinear[T: Numeric](a: Array[T]): Array[Int] = {
    import scala.math.Numeric.Implicits._

    val n = a.length
    val r = new Array[Int](a.length)
    val s = a.sum.toDouble
    assert(s >= 0)
    var aAcc = 0.0
    var rAcc = 0
    for (i <- 0 until n) {
      aAcc += a(i).toDouble
      val t = (aAcc * 32768 / s + 0.5).toInt
      r(i) = t - rAcc
      rAcc = t
    }
    assert(rAcc == 32768)
    r
  }

  def weightsToLinear(w0: Int, w1: Int, w2: Int): Array[Int] = {
    val sum = w0 + w1 + w2
    assert(sum > 0)

    val l0 = (w0.toDouble * 32768 / sum + 0.5).toInt
    val l1 = ((w0 + w1).toDouble * 32768 / sum + 0.5).toInt - l0
    val l2 = 32768 - l0 - l1
    Array(l0, l1, l2)
  }

  def weightsToLinear(w0: Double, w1: Double, w2: Double): Array[Int] = {
    val sum = w0 + w1 + w2
    assert(sum > 0)

    val l0 = (w0 * 32768 / sum + 0.5).toInt
    val l1 = ((w0 + w1) * 32768 / sum + 0.5).toInt - l0
    val l2 = 32768 - l0 - l1
    Array(l0, l1, l2)
  }

  lazy val phredConversionTable: Array[Double] = (0 to 65535).map { i => -10 * math.log10(if (i == 0) .25 else i) }.toArray

  def linearToPhred(a: Array[Int]): Array[Int] = {
    val x = a.map(phredConversionTable)
    x.map { d => (d - x.min + 0.5).toInt }
  }

  def phredToDosage(a: Array[Int]): Array[Double] = {
    val transformedProbs = a.map { i => math.pow(10, i / -10.0) }
    val s = transformedProbs.sum
    transformedProbs.map(_ / s)
  }

  val smallGTPair = Array(GTPair(0, 0), GTPair(0, 1), GTPair(1, 1),
    GTPair(0, 2), GTPair(1, 2), GTPair(2, 2),
    GTPair(0, 3), GTPair(1, 3), GTPair(2, 3), GTPair(3, 3),
    GTPair(0, 4), GTPair(1, 4), GTPair(2, 4), GTPair(3, 4), GTPair(4, 4),
    GTPair(0, 5), GTPair(1, 5), GTPair(2, 5), GTPair(3, 5), GTPair(4, 5), GTPair(5, 5),
    GTPair(0, 6), GTPair(1, 6), GTPair(2, 6), GTPair(3, 6), GTPair(4, 6), GTPair(5, 6),
    GTPair(6, 6),
    GTPair(0, 7), GTPair(1, 7), GTPair(2, 7), GTPair(3, 7), GTPair(4, 7),
    GTPair(5, 7), GTPair(6, 7), GTPair(7, 7))

  def gtPairRecursive(i: Int): GTPair = {
    def f(j: Int, k: Int): GTPair = if (j <= k)
      GTPair(j, k)
    else
      f(j - k - 1, k + 1)

    f(i, 0)
  }

  def gtPairSqrt(i: Int): GTPair = {
    val k: Int = (Math.sqrt(8 * i.toDouble + 1) / 2 - 0.5).toInt
    assert(k * (k + 1) / 2 <= i)
    val j = i - k * (k + 1) / 2
    assert(gtIndex(j, k) == i)
    GTPair(j, k)
  }

  def gtPair(i: Int): GTPair = {
    if (i < smallGTPair.length)
      smallGTPair(i)
    else
      gtPairSqrt(i)
  }

  def gtIndex(j: Int, k: Int): Int = {
    require(j >= 0 && j <= k, s"invalid gtIndex: ($j, $k)")
    k * (k + 1) / 2 + j
  }

  def gtIndex(p: GTPair): Int = gtIndex(p.j, p.k)

  def gtIndexWithSwap(i: Int, j: Int): Int = {
    if (j < i)
      gtIndex(j, i)
    else
      gtIndex(i, j)
  }

  def read(nAlleles: Int, isDosage: Boolean, a: ByteIterator): Genotype = {
    val isBiallelic = nAlleles == 2

    val flags = a.readULEB128()

    val gt: Int =
      if (flagHasGT(isBiallelic, flags)) {
        if (flagStoresGT(isBiallelic, flags))
          flagGT(isBiallelic, flags)
        else
          a.readULEB128()
      } else
        -1

    val ad: Array[Int] =
      if (flagHasAD(flags)) {
        val ada = new Array[Int](nAlleles)
        if (flagSimpleAD(flags)) {
          assert(gt >= 0)
          val p = Genotype.gtPair(gt)
          ada(p.j) = a.readULEB128()
          if (p.j != p.k)
            ada(p.k) = a.readULEB128()
        } else {
          for (i <- ada.indices)
            ada(i) = a.readULEB128()
        }
        ada
      } else
        null

    val dp =
      if (flagHasDP(flags)) {
        if (flagHasAD(flags)) {
          var i = 0
          var adsum = 0
          while (i < ad.length) {
            adsum += ad(i)
            i += 1
          }
          if (flagSimpleDP(flags))
            adsum
          else
            adsum + a.readULEB128()
        } else
          a.readULEB128()
      } else
        -1

    val px: Array[Int] =
      if (flagHasPX(flags)) {
        val pxa = new Array[Int](triangle(nAlleles))
        if (gt >= 0) {
          var i = 0
          while (i < gt) {
            pxa(i) = a.readULEB128()
            i += 1
          }
          i += 1
          while (i < pxa.length) {
            pxa(i) = a.readULEB128()
            i += 1
          }

          if (isDosage)
            pxa(gt) = 32768 - pxa.sum // original values summed to 32768 or 1.0 in probability

        } else {
          var i = 0
          while (i < pxa.length) {
            pxa(i) = a.readULEB128()
            i += 1
          }
        }

        pxa
      } else
        null

    val gq: Int =
      if (flagHasGQ(flags)) {
        if (flagSimpleGQ(flags))
          gqFromPL(px)
        else
          a.readULEB128()
      } else
        -1

    new GenericGenotype(gt, ad, dp, gq, px, flagFakeRef(flags), isDosage)
  }

  def hardCallRead(nAlleles: Int, isDosage: Boolean, a: ByteIterator): Int = {
    val isBiallelic = nAlleles == 2

    val flags = a.readULEB128()

    val gt: Int =
      if (flagHasGT(isBiallelic, flags)) {
        if (flagStoresGT(isBiallelic, flags))
          flagGT(isBiallelic, flags)
        else
          a.readULEB128()
      } else
        -1

    var count = 0

    if (flagHasAD(flags)) {
      if (flagSimpleAD(flags)) {
        count += 1
        val p = Genotype.gtPair(gt)
        if (p.j != p.k)
          count += 1
      } else
        count += nAlleles
    }

    if (flagHasDP(flags) && !(flagHasAD(flags) && flagSimpleDP(flags)))
      count += 1

    if (flagHasPX(flags))
      if (gt >= 0)
        count += triangle(nAlleles) - 1
      else
        count += triangle(nAlleles)

    if (flagHasGQ(flags) && !flagSimpleGQ(flags))
      count += 1

    a.skipLEB128(count)

    gt
  }

  def genDosage(nAlleles: Int): Gen[Genotype] = {
    val nGenotypes = triangle(nAlleles)
    for (px <- Gen.option(Gen.partition(nGenotypes, 32768))) yield {
      val gt = px.flatMap(gtFromLinear)
      val g = Genotype(gt = gt, px = px, isDosage = true)
      g.check(nAlleles)
      g
    }
  }

  def genExtreme(nAlleles: Int): Gen[Genotype] = {
    val m = Int.MaxValue / (nAlleles + 1)
    val nGenotypes = triangle(nAlleles)
    for (gt: Option[Int] <- Gen.option(Gen.choose(0, nGenotypes - 1));
      ad <- Gen.option(Gen.buildableOfN[Array, Int](nAlleles, Gen.choose(0, m)));
      dp <- Gen.option(Gen.choose(0, m));
      gq <- Gen.option(Gen.choose(0, 10000));
      pl <- Gen.oneOfGen(
        Gen.option(Gen.buildableOfN[Array, Int](nGenotypes, Gen.choose(0, m))),
        Gen.option(Gen.buildableOfN[Array, Int](nGenotypes, Gen.choose(0, 100))))) yield {
      gt.foreach { gtx =>
        pl.foreach { pla => pla(gtx) = 0 }
      }
      pl.foreach { pla =>
        val m = pla.min
        var i = 0
        while (i < pla.length) {
          pla(i) -= m
          i += 1
        }
      }
      val g = Genotype(gt, ad, dp.map(_ + ad.map(_.sum).getOrElse(0)), gq, pl)
      g.check(nAlleles)
      g
    }
  }

  def genRealistic(nAlleles: Int): Gen[Genotype] = {
    val nGenotypes = triangle(nAlleles)
    for (callRate <- Gen.choose(0d, 1d);
      alleleFrequencies <- Gen.buildableOfN[Array, Double](nAlleles, Gen.choose(1e-6, 1d)) // avoid divison by 0
        .map { rawWeights =>
        val sum = rawWeights.sum
        rawWeights.map(_ / sum)
      };
      gt <- Gen.option(Gen.zip(Gen.chooseWithWeights(alleleFrequencies), Gen.chooseWithWeights(alleleFrequencies))
        .map { case (gti, gtj) => gtIndexWithSwap(gti, gtj) }, callRate);
      ad <- Gen.option(Gen.buildableOfN[Array, Int](nAlleles,
        Gen.choose(0, 50)));
      dp <- Gen.choose(0, 30).map(d => ad.map(o => o.sum + d));
      pl <- Gen.option(Gen.buildableOfN[Array, Int](nGenotypes, Gen.choose(0, 1000)).map { arr =>
        gt match {
          case Some(i) =>
            arr(i) = 0
            arr
          case None =>
            val min = arr.min
            arr.map(_ - min)
        }
      });
      gq <- Gen.choose(-30, 30).map(i => pl.map(pls => math.max(0, gqFromPL(pls) + i)))
    ) yield
      Genotype(gt, ad, dp, gq, pl)
  }

  def genVariantGenotype: Gen[(Variant, Genotype)] =
    for (v <- Variant.gen;
      g <- Gen.oneOfGen(genExtreme(v.nAlleles), genRealistic(v.nAlleles), genDosage(v.nAlleles)))
      yield (v, g)

  def genArb: Gen[Genotype] =
    for (v <- Variant.gen;
      g <- Gen.oneOfGen(genExtreme(v.nAlleles), genRealistic(v.nAlleles), genDosage(v.nAlleles)))
      yield g

  implicit def arbGenotype = Arbitrary(genArb)
}

class GenericGenotype(private val _gt: Int,
  private val _ad: Array[Int],
  private val _dp: Int,
  private val _gq: Int,
  private val _px: Array[Int],
  val fakeRef: Boolean,
  val isDosage: Boolean) extends Genotype {

  require(_gt >= -1, s"invalid _gt value: ${ _gt }")
  require(_dp >= -1, s"invalid _dp value: ${ _dp }")

  if (isDosage) {
    require(_gq == -1)
    if (_px == null)
      require(_gt == -1)
    else {
      require(_px.sum == 32768)
      require(_gt == Genotype.gtFromLinear(_px).getOrElse(-1))
    }
  }

  def unboxedGT: Int = _gt

  def ad: Option[Array[Int]] = Option(_ad)

  def dp: Option[Int] =
    if (_dp >= 0)
      Some(_dp)
    else
      None

  def od: Option[Int] =
    if (_dp >= 0 && _ad != null)
      Some(_dp - _ad.sum)
    else
      None

  def gq: Option[Int] =
    if (_gq >= 0)
      Some(_gq)
    else
      None

  def px: Option[Array[Int]] = Option(_px)

  def pl: Option[Array[Int]] =
    if (_px == null)
      None
    else if (!isDosage)
      Some(_px)
    else
      Some(Genotype.linearToPhred(_px))

  def dosage: Option[Array[Double]] =
    if (_px == null)
      None
    else if (isDosage)
      Some(_px.map(_ / 32768.0))
    else
      Some(Genotype.phredToDosage(_px))
}

class MutableGenotype(nAlleles: Int) extends Genotype {

  private var _gt: Int = -1
  private val _ad: Array[Int] = Array.ofDim[Int](nAlleles)
  private var _dp: Int = -1
  private var _gq: Int = -1
  private val _px: Array[Int] = Array.ofDim[Int](triangle(nAlleles))
  private var _fakeRef: Boolean = false
  private var _isDosage: Boolean = false
  private var _hasAD = false
  private var _hasPX = false

  def unboxedGT: Int = _gt
  def fakeRef: Boolean = _fakeRef
  def isDosage: Boolean = _isDosage

  def ad: Option[Array[Int]] = if (_hasAD) Some(_ad) else None

  def dp: Option[Int] =
    if (_dp >= 0)
      Some(_dp)
    else
      None

  def od: Option[Int] =
    if (_dp >= 0 && _hasAD)
      Some(_dp - _ad.sum)
    else
      None

  def gq: Option[Int] =
    if (_gq >= 0)
      Some(_gq)
    else
      None

  def px: Option[Array[Int]] = if (_hasPX) Some(_px) else None

  def pl: Option[Array[Int]] =
    if (!_hasPX)
      None
    else if (!isDosage)
      Some(_px)
    else
      Some(Genotype.linearToPhred(_px))

  def dosage: Option[Array[Double]] =
    if (_hasPX)
      None
    else if (isDosage)
      Some(_px.map(_ / 32768.0))
    else
      Some(Genotype.phredToDosage(_px))

  def read(nAlleles: Int, isDosage: Boolean, a: ByteIterator) {
    val isBiallelic = nAlleles == 2

    val flags = a.readULEB128()

    _hasAD = Genotype.flagHasAD(flags)
    _hasPX = Genotype.flagHasPX(flags)

    _gt =
      if (Genotype.flagHasGT(isBiallelic, flags)) {
        if (Genotype.flagStoresGT(isBiallelic, flags))
          Genotype.flagGT(isBiallelic, flags)
        else
          a.readULEB128()
      } else
        -1

    if (_hasAD) {
      if (Genotype.flagSimpleAD(flags)) {
        assert(_gt >= 0)
        val p = Genotype.gtPair(_gt)
        _ad(p.j) = a.readULEB128()
        if (p.j != p.k)
          _ad(p.k) = a.readULEB128()
      } else {
        for (i <- _ad.indices)
          _ad(i) = a.readULEB128()
      }
    }

    _dp =
      if (Genotype.flagHasDP(flags)) {
        if (_hasAD) {
          var i = 0
          var adsum = 0
          while (i < _ad.length) {
            adsum += _ad(i)
            i += 1
          }
          if (Genotype.flagSimpleDP(flags))
            adsum
          else
            adsum + a.readULEB128()
        } else
          a.readULEB128()
      } else
        -1

    if (_hasPX) {
      if (_gt >= 0) {
        var i = 0
        while (i < _gt) {
          _px(i) = a.readULEB128()
          i += 1
        }
        i += 1
        while (i < _px.length) {
          _px(i) = a.readULEB128()
          i += 1
        }
        if (isDosage)
          _px(_gt) = 32768 - _px.sum // original values summed to 32768 or 1.0 in probability
        else
          _px(_gt) = 0
      } else {
        var i = 0
        while (i < _px.length) {
          _px(i) = a.readULEB128()
          i += 1
        }
      }
    }

    _gq =
      if (Genotype.flagHasGQ(flags)) {
        if (Genotype.flagSimpleGQ(flags))
          Genotype.gqFromPL(_px)
        else
          a.readULEB128()
      } else
        -1

    _fakeRef = Genotype.flagFakeRef(flags)

    _isDosage = isDosage
  }
}


class GenotypeBuilder(nAlleles: Int, isDosage: Boolean = false) {
  require(nAlleles > 0, s"tried to create genotype builder with $nAlleles ${ plural(nAlleles, "allele") }")
  val isBiallelic = nAlleles == 2
  val nGenotypes = triangle(nAlleles)

  var flags: Int = 0

  private var gt: Int = 0
  private var ad: Array[Int] = _
  private var dp: Int = 0
  private var gq: Int = 0
  private var px: Array[Int] = _

  def clear() {
    flags = 0
  }

  def hasGT: Boolean =
    Genotype.flagHasGT(isBiallelic, flags)

  def setGT(newGT: Int) {
    if (newGT < 0)
      fatal(s"invalid GT value `$newGT': negative value")
    if (newGT > nGenotypes)
      fatal(s"invalid GT value `$newGT': value larger than maximum number of genotypes $nGenotypes")
    if (hasGT)
      fatal(s"invalid GT, genotype already had GT")
    flags = Genotype.flagSetGT(isBiallelic, flags, newGT)
    gt = newGT
  }

  def setAD(newAD: Array[Int]) {
    if (newAD.length != nAlleles)
      fatal(s"invalid AD field `${ newAD.mkString(",") }': expected $nAlleles values, but got ${ newAD.length }.")
    flags = Genotype.flagSetHasAD(flags)
    ad = newAD
  }

  def setDP(newDP: Int) {
    if (newDP < 0)
      fatal(s"invalid DP field `$newDP': negative value")
    flags = Genotype.flagSetHasDP(flags)
    dp = newDP
  }

  def setGQ(newGQ: Int) {
    if (newGQ < 0)
      fatal(s"invalid GQ field `$newGQ': negative value")
    flags = Genotype.flagSetHasGQ(flags)
    gq = newGQ
  }

  def setPX(newPX: Array[Int]) {
    if (newPX.length != nGenotypes)
      fatal(s"invalid PL field `${ newPX.mkString(",") }': expected $nGenotypes values, but got ${ newPX.length }.")
    flags = Genotype.flagSetHasPX(flags)
    px = newPX
  }

  def setFakeRef() {
    flags = Genotype.flagSetFakeRef(flags)
  }

  def set(g: Genotype) {
    g.gt.foreach(setGT)
    g.ad.foreach(setAD)
    g.dp.foreach(setDP)
    g.gq.foreach(setGQ)
    g.px.foreach(setPX)

    if (g.fakeRef)
      setFakeRef()
  }

  def write(b: mutable.ArrayBuilder[Byte]) {
    val hasGT = Genotype.flagHasGT(isBiallelic, flags)
    val hasAD = Genotype.flagHasAD(flags)
    val hasDP = Genotype.flagHasDP(flags)
    val hasGQ = Genotype.flagHasGQ(flags)
    val hasPX = Genotype.flagHasPX(flags)

    if (isDosage) {
      if (hasPX) {
        Genotype.gtFromLinear(px) match {
          case Some(gt2) => assert(hasGT && gt == gt2)
          case None => assert(!hasGT)
        }
      } else
        assert(!hasGT)
    }

    var j = 0
    var k = 0
    if (hasGT) {
      val p = Genotype.gtPair(gt)
      j = p.j
      k = p.k
      if (hasAD) {
        var i = 0
        var simple = true
        while (i < ad.length && simple) {
          if (i != j && i != k && ad(i) != 0)
            simple = false
          i += 1
        }
        if (simple)
          flags = Genotype.flagSetSimpleAD(flags)
      }
    }

    var adsum = 0
    if (hasAD && hasDP) {
      adsum = ad.sum
      if (adsum == dp)
        flags = Genotype.flagSetSimpleDP(flags)
    }

    if (hasPX && hasGQ) {
      val gqFromPL = Genotype.gqFromPL(px)
      if (gq == gqFromPL)
        flags = Genotype.flagSetSimpleGQ(flags)
    }

    /*
    println("flags:")
    if (Genotype.flagHasGT(isBiallelic, flags))
      println(s"  gt = $gt")
    if (Genotype.flagHasDP(flags))
      println(s"  dp = $dp")
    */

    b.writeULEB128(flags)

    if (hasGT && !Genotype.flagStoresGT(isBiallelic, flags))
      b.writeULEB128(gt)

    if (hasAD) {
      if (Genotype.flagSimpleAD(flags)) {
        b.writeULEB128(ad(j))
        if (k != j)
          b.writeULEB128(ad(k))
      } else
        ad.foreach(b.writeULEB128)
    }

    if (hasDP) {
      if (hasAD) {
        if (!Genotype.flagSimpleDP(flags))
          b.writeULEB128(dp - adsum)
      } else
        b.writeULEB128(dp)
    }

    if (hasPX) {
      if (hasGT) {
        var i = 0
        while (i < gt) {
          b.writeULEB128(px(i))
          i += 1
        }
        i += 1
        while (i < px.length) {
          b.writeULEB128(px(i))
          i += 1
        }
      } else
        px.foreach(b.writeULEB128)
    }

    if (hasGQ) {
      if (!Genotype.flagSimpleGQ(flags))
        b.writeULEB128(gq)
    }
  }
}
