package org.broadinstitute.hail.driver

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.check.Prop._
import org.broadinstitute.hail.check.{Gen, Properties}
import org.broadinstitute.hail.expr.{TString, TStruct}
import org.broadinstitute.hail.variant.{Genotype, VSMSubgen, VariantSampleMatrix, _}
import org.testng.annotations.Test

class AggregateByKeySuite extends SparkSuite {
  @Test def replicateSampleAggregation() = {
    val inputVCF = "src/test/resources/sample.vcf"
    var s = State(sc, sqlContext)
    s = ImportVCF.run(s, Array("-i", inputVCF))
    s = AnnotateSamplesExpr.run(s, Array("-c", "sa.nHet = gs.filter(g => g.isHet).count()"))
    s = AggregateByKey.run(s, Array("-k", "Sample = s", "-a", "nHet = gs.filter(g => g.isHet).count()", "-n", "kt"))

    val kt = s.ktEnv("kt")
    val vds = s.vds
    println(kt.signature.schema)
    val (_, ktHetQuery) = kt.query("nHet")
    val (_, ktSampleQuery) = kt.query("Sample")
    val (_, saHetQuery) = vds.querySA("sa.nHet")

    val ktSampleResults = kt.rdd.map { a =>
      (ktSampleQuery(a).get.asInstanceOf[String], ktHetQuery(a))
    }.collectAsMap()
    ktSampleResults.head._2.foreach(x => println(x.getClass.getName))

    assert( vds.sampleIdsAndAnnotations.forall{ case (sid, sa) =>
      (saHetQuery(sa), ktSampleResults(sid)) match {
        case (None, None) => true
        case (Some(x), Some(y)) => x.asInstanceOf[Long] == y.asInstanceOf[String].toLong
        case _ => false
      }
    })
  }

  @Test def replicateVariantAggregation() = {
    val inputVCF = "src/test/resources/sample.vcf"
    var s = State(sc, sqlContext)
    s = ImportVCF.run(s, Array("-i", inputVCF))
    s = AnnotateVariantsExpr.run(s, Array("-c", "va.nHet = gs.filter(g => g.isHet).count()"))
    s = AggregateByKey.run(s, Array("-k", "Variant = v", "-a", "nHet = gs.filter(g => g.isHet).count()", "-n", "kt"))

    val kt = s.ktEnv("kt")
    val vds = s.vds

    val (_, ktHetQuery) = kt.query("nHet")
    val (_, ktVariantQuery) = kt.query("Variant")
    val (_, vaHetQuery) = vds.queryVA("va.nHet")

    val ktVariantResults = kt.rdd.map { a =>
      (ktVariantQuery(a).get.asInstanceOf[String], ktHetQuery(a))
    }.collectAsMap()

    assert( vds.variantsAndAnnotations.map{ case (v, va) =>
      (vaHetQuery(va), ktVariantResults(v.toString)) match {
        case (None, None) => true
        case (Some(x), Some(y)) => x.asInstanceOf[Long] == y.asInstanceOf[String].toLong
        case _ => false
      }
    }.fold(true)(_ && _))
  }

//  @Test def replicateGlobalAggregation() = {
//    val inputVCF = "src/test/resources/sample.vcf"
//    var s = State(sc, sqlContext)
//    s = ImportVCF.run(s, Array("-i", inputVCF))
//    s = AnnotateVariantsExpr.run(s, Array("-c", "va.nHet = gs.filter(g => g.isHet).count()"))
//    s = AnnotateGlobalExpr.run(s, Array("-c", "global.nHet = variants.map(v => va.nHet).sum()"))
//    s = AggregateByKey.run(s, Array("-k", " ", "-a", "nHet = gs.filter(g => g.isHet).count()", "-n", "kt"))
//
//    val kt = s.ktEnv("kt")
//    val vds = s.vds
//
//    val (_, ktHetQuery) = kt.query("nHet")
//    val (_, globalHet) = vds.queryGlobal("global.nHet")
//
//    val ktGlobalResults = kt.rdd.map{ a => ktHetQuery(a).asInstanceOf[String].toLong}.collect()
//    assert(ktGlobalResults.length == 1 && globalHet.get.asInstanceOf[Long] == ktGlobalResults(0))
//  }


//  def createKey(nItems: Int, nCategories: Int) =
//    Gen.buildableOfN[Array, Option[String]](nItems, Gen.option(Gen.oneOfSeq((0 until nCategories).map("group" + _)), 0.95))
//
//  def createKeys(nKeys: Int, nItems: Int) =
//    Gen.buildableOfN[Array, Array[Option[String]]](nKeys, createKey(nItems, Gen.choose(1, 10).sample()))
//
//  object Spec extends Properties("CreateKeyTable") {
//    val compGen = for (vds: VariantDataset <- VariantSampleMatrix.gen[Genotype](sc, VSMSubgen.random).filter(vds => vds.nVariants > 0 && vds.nSamples > 0);
//      nKeys <- Gen.choose(1, 5);
//      nSampleKeys <- Gen.choose(0, nKeys);
//      nVariantKeys <- Gen.const(nKeys - nSampleKeys);
//      sampleGroups <- createKeys(nSampleKeys, vds.nSamples);
//      variantGroups <- createKeys(nVariantKeys, vds.nVariants.toInt)
//    ) yield (vds, sampleGroups, variantGroups)
//
//    property("aggregate by sample and variants same") = forAll(compGen) { case (vds, sampleGroups, variantGroups) =>
//      val outputFile = tmpDir.createTempFile("keyTableTest", "tsv")
//
//      val nKeys = sampleGroups.length + variantGroups.length
//      val keyNames = (1 to nKeys).map("key" + _)
//      val sampleKeyNames = (1 to sampleGroups.length).map("key" + _)
//      val variantKeyNames = (sampleGroups.length + 1 to nKeys).map("key" + _)
//
//      var sampleSignature = TStruct()
//      sampleKeyNames.foreach(k => sampleSignature = sampleSignature.appendKey(k, TString))
//
//      var variantSignature = TStruct()
//      variantKeyNames.foreach(k => variantSignature = variantSignature.appendKey(k, TString))
//
//      val sampleMap = vds.sampleIds.zipWithIndex.map { case (sid, i) =>
//        (sid, Annotation(sampleGroups.map(_ (i).orNull).toSeq: _*))
//      }.toMap
//
//      val variantAnnotations = sc.parallelize(vds.variants.collect().zipWithIndex.map { case (v, i) =>
//        (v, Annotation(variantGroups.map(_ (i).orNull).toSeq: _*))
//      }).toOrderedRDD
//
//      var s = State(sc, sqlContext, vds.annotateSamples(sampleMap, sampleSignature, "sa.keys")
//        .annotateVariants(variantAnnotations, variantSignature, "va.keys"))
//
//      val (_, sampleKeyQuery) = s.vds.querySA("sa.keys.*")
//      val (_, variantKeyQuery) = s.vds.queryVA("va.keys.*")
//
//      val keyGenotypeRDD = s.vds.mapWithAll { case (v, va, sid, sa, g) =>
//        val key = sampleKeyQuery(sa).get.asInstanceOf[IndexedSeq[String]] ++ variantKeyQuery(va).get.asInstanceOf[IndexedSeq[String]]
//        (key, g)
//      }
//
//      val result = keyGenotypeRDD.aggregateByKey((0L, 0L, 0L))(
//        (comb, gt) => (comb._1 + gt.isHet.toInt.toInt, comb._2 + gt.isCalled.toInt.toInt, comb._3 + 1),
//        (comb1, comb2) => (comb1._1 + comb2._1, comb1._2 + comb2._2, comb1._3 + comb2._3)).collectAsMap()
//
//      s = AggregateByKey.run(s, Array("-k", (sampleKeyNames.map(k => k + " = " + "sa.keys." + k) ++ variantKeyNames.map(k => k + " = " + "va.keys." + k)).mkString(","),
//        "-a", "nHet = gs.filter(g => g.isHet).count(), nCalled = gs.filter(g => g.isCalled).count(), nTotal = gs.count()", "-n", "foo"))
//
//      s = ExportKeyTable.run(s, Array("-o", outputFile, "-n", "foo"))
//      println(outputFile)
//
//      val ktr = hadoopConf.readLines(outputFile)(_.map(_.map { line =>
//        line.trim.split("\\s+")
//      }.value).toIndexedSeq)
//
//      val header = ktr.take(1)
//
//      val keyTableResults = ktr.drop(1).map(r => (header(0), r).zipped.toMap)
//        .map { x => (keyNames.map { k => x(k) }, (x("nHet"), x("nCalled"), x("nTotal"))) }.toMap
//
//      result.forall { case (keys, (nHet, nCalled, nTotal)) =>
//        val (ktHet, ktCalled, ktTotal) = keyTableResults(keys.map(k => if (k != null) k else "NA").toIndexedSeq)
//        ktHet.toLong == nHet &&
//          ktCalled.toLong == nCalled &&
//          ktTotal.toLong == nTotal
//      }
//    }
//  }
//  //
//  //  @Test def testAddKeyTable() {
//  //    Spec.check()
//  //  }
//
//
//  @Test def test() {
//
//  }
}
