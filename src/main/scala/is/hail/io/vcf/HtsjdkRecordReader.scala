package is.hail.io.vcf

import htsjdk.variant.variantcontext.VariantContext
import is.hail.annotations._
import is.hail.expr._
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.Accumulable

import scala.collection.JavaConverters._
import scala.collection.mutable

class BufferedLineIterator(bit: BufferedIterator[String]) extends htsjdk.tribble.readers.LineIterator {
  override def peek(): String = bit.head

  override def hasNext: Boolean = bit.hasNext

  override def next(): String = bit.next()

  override def remove() {
    throw new UnsupportedOperationException
  }
}

abstract class HtsjdkRecordReader[T] extends Serializable {

  import HtsjdkRecordReader._

  def readVariantInfo(vc: VariantContext, infoSignature: Option[TStruct]): (Variant, Annotation) = {
    val pass = vc.filtersWereApplied() && vc.getFilters.isEmpty
    val filters: Set[String] = {
      if (vc.filtersWereApplied && vc.isNotFiltered)
        Set("PASS")
      else
        vc.getFilters.asScala.toSet
    }
    val rsid = vc.getID

    val ref = vc.getReference.getBaseString
    val v = Variant(vc.getContig,
      vc.getStart,
      ref,
      vc.getAlternateAlleles.iterator.asScala.map(a => {
        val base = if (a.getBaseString.isEmpty) "." else a.getBaseString // TODO: handle structural variants
        AltAllele(ref, base)
      }).toArray)
    val nAlleles = v.nAlleles
    val nGeno = v.nGenotypes

    val info = infoSignature.map { sig =>
      val a = Annotation(
        sig.fields.map { f =>
          val a = vc.getAttribute(f.name)
          try {
            cast(a, f.typ)
          } catch {
            case e: Exception =>
              fatal(
                s"""variant $v: INFO field ${ f.name }:
                   |  unable to convert $a (of class ${ a.getClass.getCanonicalName }) to ${ f.typ }:
                   |  caught $e""".stripMargin)
          }
        }: _*)
      assert(sig.typeCheck(a))
      a
    }

    val va = info match {
      case Some(infoAnnotation) => Annotation(rsid, vc.getPhredScaledQual, filters, pass, infoAnnotation)
      case None => Annotation(rsid, vc.getPhredScaledQual, filters, pass)
    }

    (v, va)
  }

  def readRecord(codec: htsjdk.variant.vcf.VCFCodec,
    reportAcc: Accumulable[mutable.Map[Int, Int], Int],
    vc: VariantContext,
    infoSignature: Option[TStruct],
    genotypeSignature: Type,
    vcfSettings: VCFSettings): (Variant, (Annotation, Iterable[T]))
}

class GenotypeRecordReader extends HtsjdkRecordReader[Genotype] {
  def readRecord(codec: htsjdk.variant.vcf.VCFCodec,
    reportAcc: Accumulable[mutable.Map[Int, Int], Int],
    vc: VariantContext,
    infoSignature: Option[TStruct],
    genotypeSignature: Type,
    vcfSettings: VCFSettings): (Variant, (Annotation, Iterable[Genotype])) = {

    val (v, va) = readVariantInfo(vc, infoSignature)

    val nAlleles = v.nAlleles

    if (vcfSettings.skipGenotypes)
      return (v, (va, Iterable.empty))

    val gb = new GenotypeBuilder(v.nAlleles, false) // FIXME: make dependent on fields in genotypes; for now, assumes PLs

    val noCall = Genotype()
    val gsb = new GenotypeStreamBuilder(v.nAlleles, isDosage = false)

    vc.getGenotypes.iterator.asScala.foreach { g =>

      val alleles = g.getAlleles.asScala
      assert(alleles.length == 1 || alleles.length == 2,
        s"expected 1 or 2 alleles in genotype, but found ${ alleles.length }")
      val a0 = alleles(0)
      val a1 = if (alleles.length == 2)
        alleles(1)
      else
        a0

      assert(a0.isCalled || a0.isNoCall)
      assert(a1.isCalled || a1.isNoCall)
      assert(a0.isCalled == a1.isCalled)

      var filter = false
      gb.clear()

      var pl = if (vcfSettings.ppAsPL) {
        val str = g.getAnyAttribute("PP")
        if (str != null)
          str.asInstanceOf[String].split(",").map(_.toInt)
        else null
      }
      else g.getPL

      // support haploid genotypes
      if (alleles.length == 1 && pl != null) {
        val expandedPL = Array.fill(v.nGenotypes)(HtsjdkRecordReader.haploidNonsensePL)
        var i = 0
        while (i < pl.length) {
          expandedPL(triangle(i + 1) - 1) = pl(i)
          i += 1
        }
        pl = expandedPL
      }

      if (g.hasPL) {
        val minPL = pl.min
        if (minPL != 0) {
          pl = pl.clone()
          var i = 0
          while (i < pl.length) {
            pl(i) -= minPL
            i += 1
          }
        }
      }

      var gt = -1 // notCalled

      if (a0.isCalled) {
        val i = vc.getAlleleIndex(a0)
        val j = vc.getAlleleIndex(a1)

        gt = if (i <= j)
          Genotype.gtIndex(i, j)
        else
          Genotype.gtIndex(j, i)

        if (g.hasPL && pl(gt) != 0) {
          reportAcc += VCFReport.GTPLMismatch
          filter = true
        }

        if (gt != -1)
          gb.setGT(gt)
      }

      val ad = g.getAD
      if (g.hasAD) {
        if (vcfSettings.skipBadAD && ad.length != nAlleles)
          reportAcc += VCFReport.ADInvalidNumber
        else
          gb.setAD(ad)
      }

      if (g.hasDP) {
        var dp = g.getDP
        if (g.hasAD) {
          val adsum = ad.sum
          if (!filter && dp < adsum) {
            reportAcc += VCFReport.ADDPMismatch
            filter = true
          }
        }

        gb.setDP(dp)
      }

      if (pl != null)
        gb.setPX(pl)

      if (g.hasGQ) {
        val gq = g.getGQ
        gb.setGQ(gq)

        if (!vcfSettings.storeGQ) {
          if (pl != null) {
            val gqFromPL = Genotype.gqFromPL(pl)

            if (!filter && gq != gqFromPL) {
              reportAcc += VCFReport.GQPLMismatch
              filter = true
            }
          } else if (!filter) {
            reportAcc += VCFReport.GQMissingPL
            filter = true
          }
        }
      }

      val odObj = g.getExtendedAttribute("OD")
      if (odObj != null) {
        val od = odObj.asInstanceOf[String].toInt

        if (g.hasAD) {
          val adsum = ad.sum
          if (!g.hasDP)
            gb.setDP(adsum + od)
          else if (!filter && adsum + od != g.getDP) {
            reportAcc += VCFReport.ADODDPPMismatch
            filter = true
          }
        } else if (!filter) {
          reportAcc += VCFReport.ODMissingAD
          filter = true
        }
      }

      if (filter)
        gsb += noCall
      else
        gsb.write(gb)
    }

    (v, (va, gsb.result()))
  }
}

class GenericRecordReader extends HtsjdkRecordReader[Annotation] {
  def readRecord(codec: htsjdk.variant.vcf.VCFCodec,
    reportAcc: Accumulable[mutable.Map[Int, Int], Int],
    vc: VariantContext,
    infoSignature: Option[TStruct],
    genotypeSignature: Type,
    vcfSettings: VCFSettings): (Variant, (Annotation, Iterable[Annotation])) = {

    val (v, va) = readVariantInfo(vc, infoSignature)

    val gs = vc.getGenotypes.iterator.asScala.map { g =>
      val a = Annotation(
        genotypeSignature.asInstanceOf[TStruct].fields.map { f =>

          val a = f.name match {
            case "GT" => g.getGenotypeString
            case _ => g.getAnyAttribute(f.name)
          }

          try {
            HtsjdkRecordReader.cast(a, f.typ)
          } catch {
            case e: Exception =>
              fatal(
                s"""variant $v: Genotype field ${ f.name }:
                 |  unable to convert $a (of class ${ a.getClass.getCanonicalName }) to ${ f.typ }:
                 |  caught $e""".stripMargin)
          }
        }: _*)
      assert(genotypeSignature.typeCheck(a))
      a
    }.toIterable

    (v, (va, gs))
  }
}

object HtsjdkRecordReader {

  val haploidNonsensePL = 1000

  def cast(value: Any, t: Type): Any = {
    ((value, t): @unchecked) match {
      case (null, _) => null
      case (s: String, TArray(TInt)) =>
        s.split(",").map(_.toInt): IndexedSeq[Int]
      case (s: String, TArray(TDouble)) =>
        s.split(",").map(_.toDouble): IndexedSeq[Double]
      case (s: String, TArray(TString)) =>
        s.split(","): IndexedSeq[String]
      case (s: String, TArray(TChar)) =>
        s.split(","): IndexedSeq[String]
      case (s: String, TBoolean) => s.toBoolean
      case (b: Boolean, TBoolean) => b
      case (s: String, TString) => s
      case (s: String, TChar) => s
      case (s: String, TInt) => s.toInt
      case (s: String, TDouble) => if (s == "nan" || s == "?" || s == "NA") Double.NaN else s.toDouble

      case (i: Int, TInt) => i
      case (d: Double, TInt) => d.toInt

      case (d: Double, TDouble) => d
      case (f: Float, TDouble) => f.toDouble

      case (f: Float, TFloat) => f
      case (d: Double, TFloat) => d.toFloat

      case (l: java.util.List[_], TArray(TInt)) =>
        l.asScala.iterator.map {
          case s: String => s.toInt
          case i: Int => i
        }.toArray: IndexedSeq[Int]
      case (l: java.util.List[_], TArray(TDouble)) =>
        l.asScala.iterator.map {
          case s: String => s.toDouble
          case i: Int => i.toDouble
          case d: Double => d
        }.toArray: IndexedSeq[Double]
      case (l: java.util.List[_], TArray(TString)) =>
        l.asScala.iterator.map {
          case s: String => s
          case i: Int => i.toString
          case d: Double => d.toString
        }.toArray[String]: IndexedSeq[String]
      case (l: java.util.List[_], TArray(TChar)) =>
        l.asScala.iterator.map(_.asInstanceOf[String]).toArray[String]: IndexedSeq[String]
    }
  }
}
