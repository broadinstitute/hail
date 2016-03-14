package org.broadinstitute.hail.methods

import htsjdk.tribble.TribbleException
import org.broadinstitute.hail.vcf.BufferedLineIterator
import scala.io.Source
import org.apache.spark.{Accumulable, SparkContext}
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.{PropagatedTribbleException, vcf}
import org.broadinstitute.hail.annotations._
import scala.collection.JavaConversions._
import scala.collection.mutable

object VCFReport {
  val GTPLMismatch = 1
  val ADDPMismatch = 2
  val ODMissingAD = 3
  val ADODDPPMismatch = 4
  val GQPLMismatch = 5
  val GQMissingPL = 6
  val RefNonACGTN = 7
  val SymbolicOrSV = 8

  var accumulators: List[(String, Accumulable[mutable.Map[Int, Int], Int])] = Nil

  def isVariant(id: Int): Boolean = id == RefNonACGTN || id == SymbolicOrSV

  def isGenotype(id: Int): Boolean = !isVariant(id)

  def warningMessage(id: Int, count: Int): String = {
    val desc = id match {
      case GTPLMismatch => "PL(GT) != 0"
      case ADDPMismatch => "sum(AD) > DP"
      case ODMissingAD => "OD present but AD missing"
      case ADODDPPMismatch => "DP != sum(AD) + OD"
      case GQPLMismatch => "GQ != difference of two smallest PL entries"
      case GQMissingPL => "GQ present but PL missing"
      case RefNonACGTN => "REF contains non-ACGT"
      case SymbolicOrSV => "Variant is symbolic or structural indel"
    }
    s"$count ${plural(count, "time")}: $desc"
  }

  def report() {
    val sb = new StringBuilder()
    for ((file, m) <- accumulators) {
      sb.clear()

      sb.append(s"while importing:\n    $file")

      val variantWarnings = m.value.filter { case (k, v) => isVariant(k) }
      val nVariantsFiltered = variantWarnings.values.sum
      if (nVariantsFiltered > 0) {
        sb.append(s"\n  filtered $nVariantsFiltered variants:")
        variantWarnings.foreach { case (id, n) =>
          if (n > 0) {
            sb.append("\n    ")
            sb.append(warningMessage(id, n))
          }
        }
        warn(sb.result())
      }

      val genotypeWarnings = m.value.filter { case (k, v) => isGenotype(k) }
      val nGenotypesFiltered = genotypeWarnings.values.sum
      if (nGenotypesFiltered > 0) {
        sb.append(s"\n  filtered $nGenotypesFiltered genotypes:")
        genotypeWarnings.foreach { case (id, n) =>
          if (n > 0) {
            sb.append("\n    ")
            sb.append(warningMessage(id, n))
          }
        }
      }

      if (nVariantsFiltered == 0 && nGenotypesFiltered == 0) {
        sb.append("  import clean")
        info(sb.result())
      } else
        warn(sb.result())
    }
  }
}

object LoadVCF {
  def lineRef(s: String): String = {
    var i = 0
    var t = 0
    while (t < 3
      && i < s.length) {
      if (s(i) == '\t')
        t += 1
      i += 1
    }
    val start = i

    while (i < s.length
      && s(i) != '\t')
      i += 1
    val end = i

    s.substring(start, end)
  }

  def apply(sc: SparkContext,
    file1: String,
    files: Array[String] = null, // FIXME hack
    storeGQ: Boolean = false,
    compress: Boolean = true,
    nPartitions: Option[Int] = None): VariantDataset = {

    val hConf = sc.hadoopConfiguration
    val headerLines = readFile(file1, hConf) { s =>
      Source.fromInputStream(s)
        .getLines()
        .takeWhile { line => line(0) == '#' }
        .toArray
    }

    val codec = new htsjdk.variant.vcf.VCFCodec()

    val header = codec.readHeader(new BufferedLineIterator(headerLines.iterator.buffered))
      .getHeaderValue
      .asInstanceOf[htsjdk.variant.vcf.VCFHeader]

    // FIXME apply descriptions when HTSJDK is fixed to expose filter descriptions
    val filters: IndexedSeq[(String, String)] = header
      .getFilterLines
      .toList
      .map(line => (line.getID, ""))
      .toArray[(String, String)]

    val infoSignatures = Annotations(header
      .getInfoHeaderLines
      .toList
      .map(line => (line.getID, VCFSignature.parse(line)))
      .toMap)

    val variantAnnotationSignatures: Annotations = Annotations(Map("info" -> infoSignatures,
      "filters" -> new SimpleSignature("Set[String]"),
      "pass" -> new SimpleSignature("Boolean"),
      "qual" -> new SimpleSignature("Double"),
      "multiallelic" -> new SimpleSignature("Boolean"),
      "rsid" -> new SimpleSignature("String")))

    val headerLine = headerLines.last
    assert(headerLine(0) == '#' && headerLine(1) != '#')

    val sampleIds = headerLine
      .split("\t")
      .drop(9)

    val sigMap = sc.broadcast(infoSignatures.attrs)

    val headerLinesBc = sc.broadcast(headerLines)

    val files2 = if (files == null)
      Array(file1)
    else
      files

    val genotypes = sc.union(files2.map { file =>
      val reportAcc = sc.accumulable[mutable.Map[Int, Int], Int](mutable.Map.empty[Int, Int])
      VCFReport.accumulators ::=(file, reportAcc)

      sc.textFile(file, nPartitions.getOrElse(sc.defaultMinPartitions))
        .mapPartitions { lines =>
          val codec = new htsjdk.variant.vcf.VCFCodec()
          val reader = vcf.HtsjdkRecordReader(headerLinesBc.value, codec)
          lines.flatMap { line =>
            try {
              if (line.isEmpty || line(0) == '#')
                None
              else if (!lineRef(line).forall(c => c == 'A' || c == 'C' || c == 'G' || c == 'T' || c == 'N')) {
                reportAcc += VCFReport.RefNonACGTN
                None
              }
              else {
                val vc = codec.decode(line)
                if (vc.isSymbolicOrSV) {
                  reportAcc += VCFReport.SymbolicOrSV
                  None
                }
                else
                  Some(reader.readRecord(reportAcc, vc, sigMap.value, storeGQ))
              }
            } catch {
              case e: TribbleException =>
                log.error(s"${e.getMessage}\n  line: $line", e)
                throw new PropagatedTribbleException(e.getMessage)
            }
          }
        }
    })

    VariantSampleMatrix(VariantMetadata(filters, sampleIds,
      Annotations.emptyIndexedSeq(sampleIds.length), Annotations.empty(),
      variantAnnotationSignatures), genotypes)
  }

}
