package is.hail.methods

import is.hail.SparkSuite
import is.hail.check._
import is.hail.expr._
import is.hail.utils._
import is.hail.variant.{GenericGenotype, Genotype, VSMSubgen, Variant, VariantDataset, VariantSampleMatrix}
import org.apache.commons.math3.distribution.{BinomialDistribution, NormalDistribution}
import org.apache.spark.sql.Row
import org.testng.annotations.Test

import scala.sys.process._

class DeNovoSuite extends SparkSuite {
  val gen: Gen[(VariantDataset, Pedigree)] = {
    for {
      vds <- VariantSampleMatrix.gen[Genotype](hc, VSMSubgen.plinkSafeBiallelic.copy(
        saSigGen = Gen.const(TStruct.empty),
        vaSigGen = Gen.const(TStruct.empty),
        globalSigGen = Gen.const(TStruct.empty),
        vGen = (for {
          pos <- Gen.choose(100000, 100000000)
          alt <- Gen.oneOf("T", "AA") // represent indels and snps
          contig <- Gen.oneOf("1", "X", "Y") // no MT (python caller doesn't support that)
        } yield Variant(contig, pos, "A", alt)).filter(v => !v.inYPar), tGen = (i: Int) => {
          require(i == 2)
          val alleleFrequency = Gen.choose(0.01, 0.30).sample()
          val readBias = Gen.choose(0.01, 0.10).sample()
          val gqMultiplier = Gen.choose(1d, 5d).sample()
          val avgDepth = Gen.choose(20d, 40d).sample()
          val depthSD = avgDepth / 2

          val depthGenerator = new NormalDistribution(avgDepth, depthSD)

          def sampleDepth(): Int = math.max(depthGenerator.sample(), 0).round.toInt

          def sampleAD(dp: Int, pAlt: Double): Array[Int] = {
            val altReads = new BinomialDistribution(dp, pAlt).sample()
            Array(dp - altReads, altReads)
          }


          def allele(d: Double): Int = if (d < alleleFrequency) 1 else 0

          for {
            isMissing <- Gen.choose(0d, 1d).map(_ < 0.1)
            a1 <- Gen.choose(0d, 1d).map(allele)
            a2 <- Gen.choose(0d, 1d).map(allele)
            gt <- Gen.const(a1 + a2)
            dp <- Gen.const(sampleDepth())
            ad <- Gen.const(if (gt == 0)
              sampleAD(dp, readBias)
            else if (gt == 1)
              sampleAD(dp, 0.5)
            else
              sampleAD(dp, 1 - readBias))
            gq <- Gen.const(if (gt == 0 || gt == 1) (gqMultiplier * dp).round.toInt
            else (dp * gqMultiplier * 3).round.toInt)
            pls <- Gen.const(if (gt == 0)
              Array(0, gq, gq * 3)
            else if (gt == 1)
              Array(gq, 0, gq)
            else
              Array(gq * 3, gq, 0))
          } yield if (isMissing) Genotype() else new GenericGenotype(gt, ad, dp, math.min(gq, 99), pls, false, false)
        },
        nSampleOverride = Some(100),
        nVariantOverride = Some(100)
      ))
      ped <- Pedigree.gen(vds.sampleIds, completeTrios = true)
    } yield (vds, ped)
  }

  @Test def test() {
    val vcfOut = tmpDir.createTempFile("out", "vcf")
    val famOut = tmpDir.createTempFile("out", "fam")
    val espOut = tmpDir.createTempFile("out", "esp")
    Prop.forAll(gen) { case (vds, ped) =>
      val vds2 = vds
        .annotateVariantsExpr("va.callStats = gs.callStats(g => v), va.filters = [\"PASS\"].toSet")
        .annotateVariantsExpr("va.esp = max(va.callStats.AF[1] + runif(-0.05, 0.05), 0), " +
          "va.info.AC = va.callStats.AC[1:], va.info.AN=va.callStats.AN, va.info.AF = va.callStats.AF[1:]")
        .annotateVariantsExpr("va.info.esp = va.esp")
        .cache()
      vds2.exportVariants(espOut,
        "Chromosome = v.contig, Position = v.start, Ref = v.ref, Alt = v.alt, 4 = 0, 5 = 0, " +
          "6 = 0, 7 = 0, 8 = 0, AC_EA = va.esp, AN_EA = 1, 11 = 0, AC_AA = 0, AN_AA = 0, 12 = 0")
      vds2.exportVCF(vcfOut)
      ped.write(famOut, hc.hadoopConf)

      val vcfURI = uriPath(vcfOut)
      val famURI = uriPath(famOut)
      val espURI = uriPath(espOut)
      val callerOutputString = s"python src/test/resources/de_novo_finder_3.py $vcfURI $famURI $espURI" !!

      val cm = callerOutputString.split("\n")
        .iterator
        .dropWhile(l => l.startsWith("#") || l.startsWith("Chr"))
        .map { l =>
          val line = l.split("\t")
          val chr = line(0)
          val pos = line(1).toInt
          val ref = line(3)
          val alt = line(4)
          val kid = line(5)
          val pdn = line(20).toDouble
          val anno = line(21)
          ((Variant(chr, pos, ref, alt), kid), (pdn, anno))
        }.toMap

      val kt = vds2.deNovo(famOut, "va.esp")

      val ktOut = kt.rdd
        .map(_.asInstanceOf[Row])
        .map { r =>
          val v = r.getAs[Variant](0)
          val kid = r.getAs[String](1)
          val pdn = r.getAs[Double](10)
          val isFemale = r.getAs[Boolean](4)
          val anno = r.getAs[String](6)
          ((v, kid), (pdn, anno, isFemale))
        }.collect()

      println(s"found ${ ktOut.length } de novo calls")
      val p1 = ktOut.forall { case ((v, kid), (pdn, anno, isf)) =>
        val variantRoot = if (v.altAllele.isSNP) "_SNV" else "_indel"
        val adjAnno = anno + variantRoot
        cm.get((v, kid)).exists { case (cPdn, cAnno) =>
          val p = D_==(cPdn, pdn, tolerance = 1e-4) && cAnno == adjAnno
          if (!p)
            println(s"MISMATCH: ${ v }, $kid, $pdn, $adjAnno, isFemale=$isf")
          p
        }
      }
      val p2 = cm.keySet == ktOut.map(_._1).toSet

      println(s"set diffs: ${ cm.keySet.diff(ktOut.map(_._1).toSet) }")
      p1 && p2
    }.check()

  }
}
