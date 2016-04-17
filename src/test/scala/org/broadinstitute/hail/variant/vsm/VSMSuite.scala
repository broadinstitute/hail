package org.broadinstitute.hail.variant.vsm

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.expr._
import scala.language.postfixOps
import org.broadinstitute.hail.methods.LoadVCF
import org.broadinstitute.hail.variant._
import org.testng.annotations.Test
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.Random
import org.broadinstitute.hail.check.Prop._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.driver._

class VSMSuite extends SparkSuite {

  @Test def testSame() {
    val vds1 = LoadVCF(sc, "src/test/resources/sample.vcf.gz")
    val vds2 = LoadVCF(sc, "src/test/resources/sample.vcf.gz")
    assert(vds1.same(vds2))

    val mdata1 = VariantMetadata(Array("S1", "S2", "S3"))
    val mdata2 = VariantMetadata(Array("S1", "S2"))
    val mdata3 = new VariantMetadata(
      Array("S1", "S2"),
      Annotation.emptyIndexedSeq(2),
      Annotation.empty,
      TStruct(
        "inner" -> TStruct(
          "thing1" -> TString),
        "thing2" -> TString),
      TEmpty,
      TEmpty)
    val mdata4 = new VariantMetadata(
      Array("S1", "S2"),
      Annotation.emptyIndexedSeq(2),
      Annotation.empty,
      TStruct(
        "inner" -> TStruct(
          "thing1" -> TString),
        "thing2" -> TString,
        "dummy" -> TString),
      TEmpty,
      TEmpty)

    assert(mdata1 != mdata2)
    assert(mdata1 != mdata3)
    assert(mdata2 != mdata3)
    assert(mdata1 != mdata4)
    assert(mdata2 != mdata4)
    assert(mdata3 != mdata4)

    val v1 = Variant("1", 1, "A", "T")
    val v2 = Variant("1", 2, "T", "G")
    val v3 = Variant("1", 2, "T", "A")

    val r1 = Annotation(Annotation("yes"), "yes")
    val r2 = Annotation(Annotation("yes"), "no")
    val r3 = Annotation(Annotation("no"), "yes")


    val va1 = r1
    val va2 = r2
    val va3 = r3

    val rdd1: RDD[(Variant, Annotation, Iterable[Genotype])] = sc.parallelize(Seq((v1, va1,
      Iterable(Genotype(),
        Genotype(0),
        Genotype(2))),
      (v2, va2,
        Iterable(Genotype(0),
          Genotype(0),
          Genotype(1)))))

    // differ in variant
    val rdd2: RDD[(Variant, Annotation, Iterable[Genotype])] = sc.parallelize(Seq((v1, va1,
      Iterable(Genotype(),
        Genotype(0),
        Genotype(2))),
      (v3, va2,
        Iterable(Genotype(0),
          Genotype(0),
          Genotype(1)))))

    // differ in genotype
    val rdd3: RDD[(Variant, Annotation, Iterable[Genotype])] = sc.parallelize(Seq((v1, va1,
      Iterable(Genotype(),
        Genotype(1),
        Genotype(2))),
      (v2, va2,
        Iterable(Genotype(0),
          Genotype(0),
          Genotype(1)))))

    // for mdata2
    val rdd4: RDD[(Variant, Annotation, Iterable[Genotype])] = sc.parallelize(Seq((v1, va1,
      Iterable(Genotype(),
        Genotype(0))),
      (v2, va2, Iterable(
        Genotype(0),
        Genotype(0)))))

    // differ in number of variants
    val rdd5: RDD[(Variant, Annotation, Iterable[Genotype])] = sc.parallelize(Seq((v1, va1,
      Iterable(Genotype(),
        Genotype(0)))))

    // differ in annotations
    val rdd6: RDD[(Variant, Annotation, Iterable[Genotype])] = sc.parallelize(Seq((v1, va1,
      Iterable(Genotype(),
        Genotype(0),
        Genotype(2))),
      (v2, va3,
        Iterable(Genotype(0),
          Genotype(0),
          Genotype(1)))))

    val vdss = Array(new VariantDataset(mdata1, rdd1),
      new VariantDataset(mdata1, rdd2),
      new VariantDataset(mdata1, rdd3),
      new VariantDataset(mdata2, rdd1),
      new VariantDataset(mdata2, rdd2),
      new VariantDataset(mdata2, rdd3),
      new VariantDataset(mdata2, rdd4),
      new VariantDataset(mdata2, rdd5),
      new VariantDataset(mdata3, rdd1),
      new VariantDataset(mdata3, rdd2),
      new VariantDataset(mdata4, rdd1),
      new VariantDataset(mdata4, rdd2),
      new VariantDataset(mdata1, rdd6))

    for (i <- vdss.indices;
      j <- vdss.indices) {
      if (i == j)
        assert(vdss(i) == vdss(j))
      else
        assert(vdss(i) != vdss(j))
    }
  }

  @Test def testReadWrite() {
    val p = forAll(VariantSampleMatrix.gen[Genotype](sc, Genotype.gen _)) { (vsm: VariantSampleMatrix[Genotype]) =>
      hadoopDelete("/tmp/foo.vds", sc.hadoopConfiguration, recursive = true)
      vsm.write(sqlContext, "/tmp/foo.vds")
      val vsm2 = VariantSampleMatrix.read(sqlContext, "/tmp/foo.vds")
      vsm2.same(vsm)
    }

    p.check
  }

  @Test def testFilterSamples() {
    val vds = LoadVCF(sc, "src/test/resources/sample.vcf.gz")
    val vdsAsMap = vds.mapWithKeys((v, s, g) => ((v, s), g)).collectAsMap()
    val nSamples = vds.nSamples

    // FIXME ScalaCheck

    val samples = vds.sampleIds
    for (n <- 0 until 20) {
      val keep = mutable.Set.empty[String]

      // n == 0: none
      if (n == 1) {
        for (i <- 0 until nSamples)
          keep += samples(i)
      } else if (n > 1) {
        for (i <- 0 until nSamples) {
          if (Random.nextFloat() < 0.5)
            keep += samples(i)
        }
      }

      val localKeep = keep
      val filtered = vds.filterSamples((s, sa) => localKeep(s))

      val filteredAsMap = filtered.mapWithKeys((v, s, g) => ((v, s), g)).collectAsMap()
      filteredAsMap.foreach { case (k, g) => assert(vdsAsMap(k) == g) }

      assert(filtered.nSamples == keep.size)
      assert(filtered.sampleIds.toSet == keep)

      val sampleKeys = filtered.mapWithKeys((v, s, g) => s).distinct.collect()
      assert(sampleKeys.toSet == keep)

      val filteredOut = "/tmp/test_filtered.vds"
      hadoopDelete(filteredOut, sc.hadoopConfiguration, recursive = true)
      filtered.write(sqlContext, filteredOut, compress = true)

      val filtered2 = VariantSampleMatrix.read(sqlContext, filteredOut)
      assert(filtered2.same(filtered))
    }
  }

  @Test def testSkipGenotypes() {
    var s = State(sc, sqlContext)

    s = ImportVCF.run(s, Array("src/test/resources/sample2.vcf"))
    s = Write.run(s, Array("-o", "/tmp/sample.vds"))

    s = Read.run(s, Array("--skip-genotypes", "-i", "/tmp/sample.vds"))
    s = FilterVariants.run(s, Array("--keep", "-c", "va.info.AF[0] < 0.01"))

    assert(s.vds.nVariants == 234)
  }

  @Test def testSkipDropSame() {
    var s = State(sc, sqlContext)

    s = ImportVCF.run(s, Array("src/test/resources/sample2.vcf"))
    s = Write.run(s, Array("-o", "/tmp/sample.vds"))

    s = Read.run(s, Array("--skip-genotypes", "-i", "/tmp/sample.vds"))

    var s2 = Read.run(s, Array("-i", "/tmp/sample.vds"))
    s2 = FilterSamples.run(s, Array("--remove", "--all"))

    assert(s.vds.same(s2.vds))
  }
}
