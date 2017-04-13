package is.hail.vds

import is.hail.SparkSuite
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class AnnotateSamplesSuite extends SparkSuite {

  @Test def testVDS() {
    val vds = hc.importVCF("src/test/resources/sample2.vcf")

    val selfAnnotated = vds.annotateSamplesVDS(vds, root = Some("sa.other"))

    val (_, q) = selfAnnotated.querySA("sa.other")
    assert(vds.sampleIdsAndAnnotations == selfAnnotated.sampleIdsAndAnnotations.map { case (id, anno) =>
      (id, q(anno))
    })
  }

  @Test def testKeyTable() {
    val kt = hc.importKeyTable("src/test/resources/sampleAnnotations.tsv", impute = true).keyBy("Sample")

    val vds = hc.importVCF("src/test/resources/sample2.vcf")

    val sampleMap = vds.annotateSamplesTable(kt, expr = Some("sa.annot = table"))
      .querySamples("index(samples.map(s => {s: s, anno: sa.annot}).collect(), s).mapValues(x => x.anno)")._1
      .asInstanceOf[Map[_, _]]

    val ktMap = kt.keyedRDD()
      .map { case (k, v) => (k.getAs[String](0), v) }
      .collectAsMap()

    assert(sampleMap == ktMap)

    // should run without throwing an index error
    vds.annotateSamplesTable(kt.filter("false", keep = true), expr = Some("sa.annot = table"))

    val sampleMap2 = vds.annotateSamplesTable(kt, vdsKey = Some(List("s")), expr = Some("sa.annot = table"))
      .querySamples("index(samples.map(s => {s: s, anno: sa.annot}).collect(), s).mapValues(x => x.anno)")._1
      .asInstanceOf[Map[_, _]]

    assert(sampleMap2 == ktMap)
  }
}
