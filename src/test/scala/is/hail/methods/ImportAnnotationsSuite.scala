package is.hail.methods

import is.hail.SparkSuite
import is.hail.TestUtils._
import is.hail.annotations._
import is.hail.check.Prop
import is.hail.expr.{TDouble, TInt, TString, TStruct}
import is.hail.io.plink.{FamFileConfig, PlinkLoader}
import is.hail.utils._
import is.hail.variant._
import org.testng.annotations.Test

import scala.io.Source

class ImportAnnotationsSuite extends SparkSuite {

  @Test def testSampleTSVAnnotator() {
    val vds = hc.importVCF("src/test/resources/sample2.vcf")

    val path1 = "src/test/resources/sampleAnnotations.tsv"
    val fileMap = hadoopConf.readFile(path1) { reader =>
      Source.fromInputStream(reader)
        .getLines()
        .filter(line => !line.startsWith("Sample"))
        .map(line => {
          val split = line.split("\t")
          val f3 = split(2) match {
            case "NA" => None
            case x => Some(x.toInt)
          }
          (split(0), (Some(split(1)), f3))
        })
        .toMap
    }

    val anno1 = vds.annotateSamplesTable(
      hc.importKeyTable("src/test/resources/sampleAnnotations.tsv", types = Map("qPhen" -> TInt)).keyBy("Sample"),
      root = "sa.`my phenotype`")

    val q1 = anno1.querySA("sa.`my phenotype`.Status")._2
    val q2 = anno1.querySA("sa.`my phenotype`.qPhen")._2

    assert(anno1.sampleIdsAndAnnotations
      .forall {
        case (id, sa) =>
          fileMap.get(id).forall { case (status, qphen) =>
            status.liftedZip(Option(q1(sa))).exists { case (v1, v2) => v1 == v2 } &&
              (qphen.isEmpty && Option(q2(sa)).isEmpty || qphen.liftedZip(Option(q2(sa))).exists { case (v1, v2) => v1 == v2 })
          }
      })
  }

  @Test def testSampleFamAnnotator() {
    def assertNumeric(s: String) = assert(PlinkLoader.numericRegex.findFirstIn(s).isDefined)

    def assertNonNumeric(s: String) = assert(PlinkLoader.numericRegex.findFirstIn(s).isEmpty)

    List("0", "0.0", ".0", "-01", "1e5", "1e10", "1.1e10", ".1E-10").foreach(assertNumeric)
    List("", "a", "1.", ".1.", "1e", "e", "E0", "1e1.", "1e.1", "1e1.1").foreach(assertNonNumeric)

    def qMap(query: String, vds: VariantDataset): Map[String, Option[Any]] = {
      val q = vds.querySA(query)._2
      vds.sampleIds
        .zip(vds.sampleAnnotations)
        .map { case (id, sa) => (id, Option(q(sa))) }
        .toMap
    }

    var vds = hc.importVCF("src/test/resources/importFam.vcf")

    vds = vds.annotateSamplesFam("src/test/resources/importFamCaseControl.fam")
    val m = qMap("sa.fam", vds)

    assert(m("A").contains(Annotation("Newton", "C", "D", false, false)))
    assert(m("B").contains(Annotation("Turing", "C", "D", true, true)))
    assert(m("C").contains(Annotation(null, null, null, null, null)))
    assert(m("D").contains(Annotation(null, null, null, null, null)))
    assert(m("E").contains(Annotation(null, null, null, null, null)))
    assert(m("F").isEmpty)

    interceptFatal("non-numeric") {
      vds.annotateSamplesFam("src/test/resources/importFamCaseControlNumericException.fam")
    }

    vds = vds.annotateSamplesFam("src/test/resources/importFamQPheno.fam", config = FamFileConfig(isQuantitative = true))
    val m1 = qMap("sa.fam", vds)

    assert(m1("A").contains(Annotation("Newton", "C", "D", false, 1.0)))
    assert(m1("B").contains(Annotation("Turing", "C", "D", true, 2.0)))
    assert(m1("C").contains(Annotation(null, null, null, null, 0.0)))
    assert(m1("D").contains(Annotation(null, null, null, null, -9.0)))
    assert(m1("E").contains(Annotation(null, null, null, null, null)))
    assert(m1("F").isEmpty)


    vds = vds.annotateSamplesFam("src/test/resources/importFamQPheno.space.m9.fam", "sa.ped",
      config = FamFileConfig(isQuantitative = true, delimiter = "\\\\s+", missingValue = "-9"))

    val m2 = qMap("sa.ped", vds)

    assert(m2("A").contains(Annotation("Newton", "C", "D", false, 1.0)))
    assert(m2("B").contains(Annotation("Turing", "C", "D", true, 2.0)))
    assert(m2("C").contains(Annotation(null, null, null, null, 0.0)))
    assert(m2("D").contains(Annotation(null, null, null, null, null)))
    assert(m2("E").contains(Annotation(null, null, null, null, 3.0)))
    assert(m2("F").isEmpty)

  }

  @Test def testSampleListAnnotator() {
    var vds = hc.importVCF("src/test/resources/sample.vcf")

    val sampleList1 = Array("foo1", "foo2", "foo3", "foo4")
    val sampleList2 = Array("C1046::HG02024", "C1046::HG02025", "C1046::HG02026",
      "C1047::HG00731", "C1047::HG00732", "C1047::HG00733", "C1048::HG02024")
    val sampleList3 = vds.sampleIds.toArray

    val fileRoot = tmpDir.createTempFile(prefix = "sampleListAnnotator")
    hadoopConf.writeTable(fileRoot + "file1.txt", sampleList1)
    hadoopConf.writeTable(fileRoot + "file2.txt", sampleList2)
    hadoopConf.writeTable(fileRoot + "file3.txt", sampleList3)

    vds = vds.annotateSamplesList(fileRoot + "file1.txt", root = "sa.test1")
    vds = vds.annotateSamplesList(fileRoot + "file2.txt", root = "sa.test2")
    vds = vds.annotateSamplesList(fileRoot + "file3.txt", root = "sa.test3")

    val (_, querier1) = vds.querySA("sa.test1")
    val (_, querier2) = vds.querySA("sa.test2")
    val (_, querier3) = vds.querySA("sa.test3")

    assert(vds.sampleIdsAndAnnotations.forall { case (sample, sa) => querier1(sa) == false })
    assert(vds.sampleIdsAndAnnotations.forall { case (sample, sa) => querier3(sa) == true })
    assert(vds.sampleIdsAndAnnotations.forall { case (sample, sa) => querier2(sa) == sampleList2.contains(sample) })
  }

  @Test def testVariantTSVAnnotator() {
    val vds = hc.importVCF("src/test/resources/sample.vcf")
      .splitMulti()

    val fileMap = hadoopConf.readFile("src/test/resources/variantAnnotations.tsv") { reader =>
      Source.fromInputStream(reader)
        .getLines()
        .filter(line => !line.startsWith("Chromosome"))
        .map(line => {
          val split = line.split("\t")
          val v = Variant(split(0), split(1).toInt, split(2), split(3))
          val rand1 = if (split(4) == "NA") None else Some(split(4).toDouble)
          val rand2 = if (split(5) == "NA") None else Some(split(5).toDouble)
          val gene = if (split(6) == "NA") None else Some(split(6))
          (v, (rand1, rand2, gene))
        })
        .toMap
    }
    val kt1 = hc.importKeyTable("src/test/resources/variantAnnotations.tsv",
      types = Map("Rand1" -> TDouble, "Rand2" -> TDouble))
      .annotate("Variant = Variant(Chromosome, Position.toInt(), Ref, Alt)")
      .keyBy("Variant")

    val anno1 = vds.annotateVariantsTable(kt1,
      expr = "va.stuff = select(table, Rand1, Rand2, Gene)")

    val q1 = anno1.queryVA("va.stuff")._2
    anno1.rdd
      .collect()
      .foreach {
        case (v, (va, gs)) =>
          val (rand1, rand2, gene) = fileMap(v)
          assert(q1(va) == Annotation(rand1.getOrElse(null), rand2.getOrElse(null), gene.getOrElse(null)))
      }

    val kt2 = hc.importKeyTable("src/test/resources/variantAnnotations.alternateformat.tsv",
      types = Map("Rand1" -> TDouble, "Rand2" -> TDouble))
      .annotate("v = Variant(`Chromosome:Position:Ref:Alt`)")
      .keyBy("v")
    val anno1alternate = vds.annotateVariantsTable(kt2,
      expr = "va.stuff = select(table, Rand1, Rand2, Gene)")

    val kt3 = hc.importKeyTable("src/test/resources/variantAnnotations.split.*.tsv",
      types = Map("Rand1" -> TDouble, "Rand2" -> TDouble))
      .annotate("v = Variant(Chromosome, Position.toInt(), Ref, Alt)")
      .keyBy("v")
    val anno1glob = vds.annotateVariantsTable(kt3,
      expr = "va.stuff = select(table, Rand1, Rand2, Gene)")

    assert(anno1alternate.same(anno1))
    assert(anno1glob.same(anno1))
  }

  @Test def testVCFAnnotator() {
    val vds = hc.importVCF("src/test/resources/sample.vcf")
      .splitMulti()

    val anno1 = vds.annotateVariantsVDS(hc.importVCF("src/test/resources/sampleInfoOnly.vcf").splitMulti(),
      root = Some("va.other"))

    val otherMap = hc.importVCF("src/test/resources/sampleInfoOnly.vcf")
      .splitMulti()
      .variantsAndAnnotations
      .collect()
      .toMap

    val initialMap = vds.variantsAndAnnotations
      .collect()
      .toMap

    val (_, q) = anno1.queryVA("va.other")

    anno1.rdd.collect()
      .foreach {
        case (v, (va, gs)) =>
          assert(q(va) == otherMap.getOrElse(v, null))
      }
  }

  @Test def testBedIntervalAnnotator() {
    val vds = hc.importVCF("src/test/resources/sample.vcf")
      .cache()
      .splitMulti()

    val bed1r = vds.annotateVariantsBED("src/test/resources/example1.bed", "va.test")
    val bed2r = vds.annotateVariantsBED("src/test/resources/example2.bed", "va.test")
    val bed3r = vds.annotateVariantsBED("src/test/resources/example3.bed", "va.test")

    val q1 = bed1r.queryVA("va.test")._2
    val q2 = bed2r.queryVA("va.test")._2
    val q3 = bed3r.queryVA("va.test")._2

    bed1r.variantsAndAnnotations
      .collect()
      .foreach {
        case (v, va) =>
          assert(v.start <= 14000000 ||
            v.start >= 17000000 ||
            q1(va) == false)
      }

    bed2r.variantsAndAnnotations
      .collect()
      .foreach {
        case (v, va) =>
          (v.start <= 14000000 && q2(va) == "gene1") ||
            (v.start >= 17000000 && q2(va) == "gene2") ||
            q2(va) == null
      }

    assert(bed3r.same(bed2r))

    val int1r = vds.annotateVariantsIntervals("src/test/resources/exampleAnnotation1.interval_list", "va.test")
    val int2r = vds.annotateVariantsIntervals("src/test/resources/exampleAnnotation2.interval_list", "va.test")

    assert(int1r.same(bed1r))
    assert(int2r.same(bed2r))
  }

  //FIXME this test is impossible to follow
  @Test def testImportAnnotations() {
    var vds = hc.importVCF("src/test/resources/sample.vcf")

    val sSample = vds.splitMulti()

    // tsv
    val importTSVFile = tmpDir.createTempFile("variantAnnotationsTSV", ".vds")
    vds = VariantDataset.fromKeyTable(hc.importKeyTable("src/test/resources/variantAnnotations.tsv",
      impute = true, types = Map("Chromosome" -> TString))
      .annotate("v = Variant(Chromosome, Position, Ref, Alt)")
      .keyBy("v"))
      .splitMulti()
      .annotateVariantsExpr(
        """va = {Chromosome: va.Chromosome,
          |Position: va.Position,
          |Ref: va.Ref,
          |Alt: va.Alt,
          |Rand1: va.Rand1,
          |Rand2: va.Rand2,
          |Gene: va.Gene}""".stripMargin)
    vds.write(importTSVFile)

    val kt = hc.importKeyTable("src/test/resources/variantAnnotations.tsv",
      impute = true, types = Map("Chromosome" -> TString))
      .annotate("v = Variant(Chromosome, Position, Ref, Alt)")
      .keyBy("v")

    vds = sSample
      .annotateVariantsTable(kt, expr = "va.stuff = table")

    var t = sSample.annotateVariantsVDS(hc.read(importTSVFile), root = Some("va.stuff"))

    assert(vds.same(t))

    // json
    val importJSONFile = tmpDir.createTempFile("variantAnnotationsJSON", ".vds")
    // FIXME better way to array-ify

    val kt2 = hc.importKeyTable("src/test/resources/importAnnot.json",
      types = Map("_0" -> TStruct("Rand1" -> TDouble, "Rand2" -> TDouble,
        "Gene" -> TString, "contig" -> TString, "start" -> TInt, "ref" -> TString, "alt" -> TString)),
      noHeader = true)
      .annotate("""v = Variant(_0.contig, _0.start, _0.ref, _0.alt.split("/"))""")
      .keyBy("v")

    vds = VariantDataset.fromKeyTable(kt2)
      .annotateVariantsExpr("va = va._0")
      .filterMulti()
    vds.write(importJSONFile)

    vds = sSample.annotateVariantsTable(kt2, root = "va.third")
      .annotateVariantsExpr("va.third = va.third._0")

    t = sSample.annotateVariantsVDS(hc.read(importJSONFile), root = Some("va.third"))

    assert(vds.same(t))
  }

  @Test def testAnnotateSamples() {
    val vds = hc.importVCF("src/test/resources/sample.vcf")
      .splitMulti()

    val annoMap = vds.sampleIds.map(id => (id, 5))
      .toMap
    val vds2 = vds.filterSamples({ case (s, sa) => scala.util.Random.nextFloat > 0.5 })
      .annotateSamples(annoMap, TInt, "sa.test")

    val q = vds2.querySA("sa.test")._2

    vds2.sampleIds
      .zipWithIndex
      .foreach {
        case (s, i) =>
          assert(q(vds2.sampleAnnotations(i)) == 5)
      }

  }

  @Test def testTables() {

    val format1 =
      """Chr Pos        Ref   Alt   Anno1   Anno2
        |22  16050036   A     C     0.123   var1
        |22  16050115   G     A     0.234   var2
        |22  16050159   C     T     0.345   var3
        |22  16050213   C     T     0.456   var4""".stripMargin

    val format2 =
      """# This line has some garbage
        |### this line too
        |## and this one
        |Chr Pos        Ref   Alt   Anno1   Anno2
        |22  16050036   A     C     0.123   var1
        |22  16050115   G     A     0.234   var2
        |22  16050159   C     T     0.345   var3
        |22  16050213   C     T     0.456   var4""".stripMargin

    val format3 =
      """# This line has some garbage
        |### this line too
        |## and this one
        |22  16050036   A     C     0.123   var1
        |22  16050115   G     A     0.234   var2
        |22  16050159   C     T     0.345   var3
        |22  16050213   C     T     0.456   var4""".stripMargin

    val format4 =
      """22,16050036,A,C,0.123,var1
        |22,16050115,G,A,0.234,var2
        |22,16050159,C,T,0.345,var3
        |22,16050213,C,T,0.456,var4""".stripMargin

    val format5 =
      """Chr Pos        Ref   Alt   OtherCol1  Anno1   OtherCol2  Anno2   OtherCol3
        |22  16050036   A     C     .          0.123   .          var1    .
        |22  16050115   G     A     .          0.234   .          var2    .
        |22  16050159   C     T     .          0.345   .          var3    .
        |22  16050213   C     T     .          0.456   .          var4    .""".stripMargin

    val format6 =
      """!asdasd
        |!!astasd
        |!!!asdagaadsa
        |22,16050036,A,C,...,0.123,...,var1,...
        |22,16050115,G,A,...,0.234,...,var2,...
        |22,16050159,C,T,...,0.345,...,var3,...
        |22,16050213,C,T,...,0.456,...,var4,...""".stripMargin

    val tmpf1 = tmpDir.createTempFile("f1", ".txt")
    val tmpf2 = tmpDir.createTempFile("f2", ".txt")
    val tmpf3 = tmpDir.createTempFile("f3", ".txt")
    val tmpf4 = tmpDir.createTempFile("f4", ".txt")
    val tmpf5 = tmpDir.createTempFile("f5", ".txt")
    val tmpf6 = tmpDir.createTempFile("f6", ".txt")

    hadoopConf.writeTextFile(tmpf1) { out => out.write(format1) }
    hadoopConf.writeTextFile(tmpf2) { out => out.write(format2) }
    hadoopConf.writeTextFile(tmpf3) { out => out.write(format3) }
    hadoopConf.writeTextFile(tmpf4) { out => out.write(format4) }
    hadoopConf.writeTextFile(tmpf5) { out => out.write(format5) }
    hadoopConf.writeTextFile(tmpf6) { out => out.write(format6) }

    val vds = hc.importVCF("src/test/resources/sample.vcf")
    val kt1 = hc.importKeyTable(tmpf1, separator = "\\s+", impute = true)
      .annotate("v = Variant(str(Chr), Pos, Ref, Alt)")
      .keyBy("v")
    val fmt1 = vds.annotateVariantsTable(kt1, expr = "va = merge(va, select(table, Anno1, Anno2))")

    val fmt2 = vds.annotateVariantsTable(hc.importKeyTable(tmpf2, separator = "\\s+", impute = true,
      commentChar = Some("#"))
      .annotate("v = Variant(str(Chr), Pos, Ref, Alt)")
      .keyBy("v"),
      expr = "va = merge(va, select(table, Anno1, Anno2))")

    val fmt3 = vds.annotateVariantsTable(hc.importKeyTable(tmpf3,
      commentChar = Some("#"), separator = "\\s+", noHeader = true, impute = true)
      .annotate("v = Variant(str(_0), _1, _2, _3)")
      .keyBy("v"),
      expr = "va.Anno1 = table._4, va.Anno2 = table._5")

    val fmt4 = vds.annotateVariantsTable(hc.importKeyTable(tmpf4, separator = ",", noHeader = true, impute = true)
      .annotate("v = Variant(str(_0), _1, _2, _3)")
      .keyBy("v"),
      expr = "va.Anno1 = table._4, va.Anno2 = table._5")

    val fmt5 = vds.annotateVariantsTable(hc.importKeyTable(tmpf5, separator = "\\s+", impute = true, missing = ".")
      .annotate("v = Variant(str(Chr), Pos, Ref, Alt)")
      .keyBy("v"),
      expr = "va.Anno1 = table.Anno1, va.Anno2 = table.Anno2")

    val fmt6 = vds.annotateVariantsTable(hc.importKeyTable(tmpf6,
      noHeader = true, impute = true, separator = ",", commentChar = Some("!"))
      .annotate("v = Variant(str(_0), _1, _2, _3)")
      .keyBy("v"),
      expr = "va.Anno1 = table._5, va.Anno2 = table._7")

    val vds1 = fmt1.cache()

    assert(vds1.same(fmt2))
    assert(vds1.same(fmt3))
    assert(vds1.same(fmt4))
    assert(vds1.same(fmt5))
    assert(vds1.same(fmt6))
  }

  @Test def testAnnotationsVDSReadWrite() {
    val outPath = tmpDir.createTempFile("annotationOut", ".vds")
    val p = Prop.forAll(VariantSampleMatrix.gen(hc, VSMSubgen.realistic)
      .filter(vds => vds.countVariants > 0)) { vds: VariantDataset =>

      vds.annotateVariantsVDS(vds, code = Some("va = vds")).same(vds)
    }

    p.check()
  }

  @Test def testPositions() {
    val vds = hc.importVCF("src/test/resources/sample2.vcf")
      .splitMulti()

    val kt = hc.importKeyTable("src/test/resources/sample2_va_positions.tsv",
      types = Map("Rand1" -> TDouble, "Rand2" -> TDouble))
      .annotate("loc = Locus(Chromosome, Position.toInt())")
      .keyBy("loc")

    val byPosition = vds.annotateLociTable(kt,
      expr = Some("va.stuff = select(table, Rand1, Rand2)"))

    val kt2 = hc.importKeyTable("src/test/resources/sample2_va_nomulti.tsv",
      types = Map("Rand1" -> TDouble, "Rand2" -> TDouble))
      .annotate("v = Variant(Chromosome, Position.toInt(), Ref, Alt)")
      .keyBy("v")
    val byVariant = vds.annotateVariantsTable(kt2,
      expr = "va.stuff = select(table, Rand1, Rand2)")

    assert(byPosition.same(byVariant))
  }
}

