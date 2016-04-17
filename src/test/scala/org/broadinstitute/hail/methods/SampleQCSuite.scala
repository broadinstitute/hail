package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.driver.State
import org.testng.annotations.Test
import org.broadinstitute.hail.driver._

import scala.io.Source

class SampleQCSuite extends SparkSuite {
  @Test def testStoreAfterFilter() {
    var s = State(sc, sqlContext)

    s = ImportVCF.run(s, Array("src/test/resources/multipleChromosomes.vcf"))
    s = SplitMulti.run(s, Array.empty[String])
    s = FilterSamples.run(s, Array("--keep", "-c", """"HG" ~ s.id"""))
    s = SampleQC.run(s, Array("-o", "/tmp/sampleqc.tsv"))
    s = ExportSamples.run(s, Array("-o", "/tmp/exportsamples.tsv", "-c",
      """sampleID = s.id,
        |nNotCalled = sa.qc.nNotCalled,
        |nHomRef = sa.qc.nHomRef,
        |nHet = sa.qc.nHet,
        |nHomVar = sa.qc.nHomVar""".stripMargin))

    val sampleQCLines = Source.fromFile("/tmp/sampleqc.tsv")
      .getLines()
        .map { line =>
          val fields = line.split("\t")
          Array(fields(0), fields(3), fields(4), fields(5), fields(6)).mkString("\t")
        }
      .toSet
    val exportSamplesLines = Source.fromFile("/tmp/exportsamples.tsv")
      .getLines().toSet

    assert(exportSamplesLines == sampleQCLines)
  }
}
