package org.broadinstitute.hail.io

import org.apache.spark.SparkContext
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.vcf

import scala.io.Source

object LoadVCF {
  // FIXME move to VariantDataset
  def apply(sc: SparkContext,
    file: String,
    readerBuilder: vcf.AbstractRecordReaderBuilder = vcf.HtsjdkRecordReaderBuilder,
    vsmtype: String = "sparky",
    compress: Boolean = true,
    nPartitions: Option[Int] = None): VariantDataset = {

    require(file.endsWith(".vcf")
      || file.endsWith(".vcf.bgz")
      || file.endsWith(".vcf.gz"))

    val hConf = sc.hadoopConfiguration
    val headerLines = readFile(file, hConf) { s =>
      Source.fromInputStream(s)
        .getLines()
        .takeWhile(line => line(0) == '#')
        .toArray
    }

    val headerLine = headerLines.last
    assert(headerLine(0) == '#' && headerLine(1) != '#')

    val sampleIds = headerLine
      .split("\t")
      .drop(9)

    val headerLinesBc = sc.broadcast(headerLines)
    val genotypes = sc.textFile(file, nPartitions.getOrElse(sc.defaultMinPartitions))
      .mapPartitions { lines =>
        val reader = readerBuilder.result(headerLinesBc.value)
        lines.filter(line => !line.isEmpty && line(0) != '#')
          .flatMap(reader.readRecord)
          .map { case (v, gs) =>
            val b = new GenotypeStreamBuilder(v, compress)
            for (g <- gs)
              b += g
            (v, b.result())
          }
      }

    // FIXME null should be contig lengths
    VariantSampleMatrix(vsmtype, VariantMetadata(null, sampleIds, headerLines), genotypes)
  }
}
