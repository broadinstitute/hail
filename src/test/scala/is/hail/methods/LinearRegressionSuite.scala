package is.hail.methods

import is.hail.SparkSuite
import is.hail.TestUtils._
import is.hail.annotations.Querier
import is.hail.expr.TDouble
import is.hail.io.plink.FamFileConfig
import is.hail.utils._
import is.hail.variant.{Genotype, Variant}
import org.testng.annotations.Test

class LinearRegressionSuite extends SparkSuite {

  @Test def testWithTwoCov() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.cov",
        "Sample",
        root = Some("sa.cov"),
        config = TextTableConfiguration(types = Map("Cov1" -> TDouble, "Cov2" -> TDouble)))
      .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
        "Sample",
        root = Some("sa.pheno"),
        config = TextTableConfiguration(types = Map("Pheno" -> TDouble), missing = "0"))
      .linreg("sa.pheno.Pheno", Array("sa.cov.Cov1", "sa.cov.Cov2 + 1 - 1"), "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 0.0)

    val v1 = Variant("1", 1, "C", "T")   // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T")   // x = (., 2, ., 2, 0, 0)
    val v3 = Variant("1", 3, "C", "T")   // x = (0, ., 1, 1, 1, .)
    val v6 = Variant("1", 6, "C", "T")   // x = (0, 0, 0, 0, 0, 0)
    val v7 = Variant("1", 7, "C", "T")   // x = (1, 1, 1, 1, 1, 1)
    val v8 = Variant("1", 8, "C", "T")   // x = (2, 2, 2, 2, 2, 2)
    val v9 = Variant("1", 9, "C", "T")   // x = (., 1, 1, 1, 1, 1)
    val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

    val qBeta = vds.queryVA("va.linreg.beta")._2
    val qSe = vds.queryVA("va.linreg.se")._2
    val qTstat = vds.queryVA("va.linreg.tstat")._2
    val qPval = vds.queryVA("va.linreg.pval")._2

    val annotationMap = vds.variantsAndAnnotations
      .collect()
      .toMap

    def assertInt(q: Querier, v: Variant, value: Int) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Int], value))

    def assertDouble(q: Querier, v: Variant, value: Double) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Double], value))

    def assertEmpty(q: Querier, v: Variant) =
      assert(q(annotationMap(v)) == null)

    /*
    comparing to output of R code:
    y = c(1, 1, 2, 2, 2, 2)
    x = c(0, 1, 0, 0, 0, 1)
    c1 = c(0, 2, 1, -2, -2, 4)
    c2 = c(-1, 3, 5, 0, -4, 3)
    df = data.frame(y, x, c1, c2)
    fit <- lm(y ~ x + c1 + c2, data=df)
    summary(fit)["coefficients"]

    */

    assertDouble(qBeta, v1, -0.28589421)
    assertDouble(qSe, v1, 1.2739153)
    assertDouble(qTstat, v1, -0.22442167)
    assertDouble(qPval, v1, 0.84327106)

    /*
    v2 has two missing genotypes, comparing to output of R code as above with imputed genotypes:
    x = c(1, 2, 1, 2, 0, 0)
    */

    assertDouble(qBeta, v2, -0.5417647)
    assertDouble(qSe, v2, 0.3350599)
    assertDouble(qTstat, v2, -1.616919)
    assertDouble(qPval, v2, 0.24728705)

    /*
    v3 has two missing genotypes, comparing to output of R code as above with imputed genotypes:
    x = c(0, 0.75, 1, 1, 1, 0.75)
    */

    assertDouble(qBeta, v3, 1.07367185)
    assertDouble(qSe, v3, 0.6764348)
    assertDouble(qTstat, v3, 1.5872510)
    assertDouble(qPval, v3, 0.2533675)

    assertEmpty(qBeta, v6)
    assertEmpty(qBeta, v7)
    assertEmpty(qBeta, v8)
    assertEmpty(qBeta, v9)
    assertEmpty(qBeta, v10)
  }

  @Test def testWithTwoCovPhred() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.cov",
        "Sample",
        root = Some("sa.cov"),
        config = TextTableConfiguration(types = Map("Cov1" -> TDouble, "Cov2" -> TDouble)))
      .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
        "Sample",
        root = Some("sa.pheno"),
        config = TextTableConfiguration(types = Map("Pheno" -> TDouble), missing = "0"))
      .linreg("sa.pheno.Pheno", Array("sa.cov.Cov1", "sa.cov.Cov2 + 1 - 1"), "va.linreg", useDosages = true, useDominance = false, useNormalise = false, 1, 0.0)

    val v1 = Variant("1", 1, "C", "T")   // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T")   // x = (., 2, ., 2, 0, 0)
    val v3 = Variant("1", 3, "C", "T")   // x = (0, ., 1, 1, 1, .)
    val v6 = Variant("1", 6, "C", "T")   // x = (0, 0, 0, 0, 0, 0)
    val v7 = Variant("1", 7, "C", "T")   // x = (1, 1, 1, 1, 1, 1)
    val v8 = Variant("1", 8, "C", "T")   // x = (2, 2, 2, 2, 2, 2)
    val v9 = Variant("1", 9, "C", "T")   // x = (., 1, 1, 1, 1, 1)
    val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

    val qBeta = vds.queryVA("va.linreg.beta")._2
    val qSe = vds.queryVA("va.linreg.se")._2
    val qTstat = vds.queryVA("va.linreg.tstat")._2
    val qPval = vds.queryVA("va.linreg.pval")._2

    val annotationMap = vds.variantsAndAnnotations
      .collect()
      .toMap

    def assertInt(q: Querier, v: Variant, value: Int) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Int], value))

    def assertDouble(q: Querier, v: Variant, value: Double) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Double], value))

    def assertEmpty(q: Querier, v: Variant) =
      assert(q(annotationMap(v)) == null)

    val gt0 = Genotype.phredToBiallelicDosageGT(Array(0, 20, 100))
    val gt1 = Genotype.phredToBiallelicDosageGT(Array(20, 0, 100))
    val gt2 = Genotype.phredToBiallelicDosageGT(Array(20, 100, 0))

    assert(D_==(gt0, 0.009900990296049406))
    assert(D_==(gt1, 0.9900990100009803))
    assert(D_==(gt2, 1.980198019704931))

    /*
    comparing to output of R code:
    y = c(1, 1, 2, 2, 2, 2)
    x = c(0.009900990296049406, 0.9900990100009803, 0.009900990296049406, 0.009900990296049406, 0.009900990296049406, 0.9900990100009803)
    c1 = c(0, 2, 1, -2, -2, 4)
    c2 = c(-1, 3, 5, 0, -4, 3)
    df = data.frame(y, x, c1, c2)
    fit <- lm(y ~ x + c1 + c2, data=df)
    summary(fit)["coefficients"]

    */

    assertDouble(qBeta, v1, -0.29166985)
    assertDouble(qSe, v1, 1.2996510)
    assertDouble(qTstat, v1, -0.22442167)
    assertDouble(qPval, v1, 0.84327106)

    /*
    v2 has two missing genotypes, comparing to output of R code as above with imputed genotypes:
    x = c(0.9950495050004902, 1.980198019704931, 0.9950495050004902, 1.980198019704931, 0.009900990296049406, 0.009900990296049406)
    */

    assertDouble(qBeta, v2, -0.5499320)
    assertDouble(qSe, v2, 0.3401110)
    assertDouble(qTstat, v2, -1.616919)
    assertDouble(qPval, v2, 0.24728705)

    /*
    v3 has two missing genotypes, comparing to output of R code as above with imputed genotypes:
    x = c(0.009900990296049406, 0.7450495050747477, 0.9900990100009803, 0.9900990100009803, 0.9900990100009803, 0.7450495050747477)
    */

    assertDouble(qBeta, v3, 1.09536219)
    assertDouble(qSe, v3, 0.6901002)
    assertDouble(qTstat, v3, 1.5872510)
    assertDouble(qPval, v3, 0.2533675)

    assertEmpty(qBeta, v6)
  }

  @Test def testWithNoCov() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
        "Sample",
        root = Some("sa.pheno"),
        config = TextTableConfiguration(types = Map("Pheno" -> TDouble), missing = "0"))
      .linreg("sa.pheno.Pheno", Array.empty[String], "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 0.0)

    val v1 = Variant("1", 1, "C", "T")   // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T")   // x = (., 2, ., 2, 0, 0)
    val v6 = Variant("1", 6, "C", "T")   // x = (0, 0, 0, 0, 0, 0)
    val v7 = Variant("1", 7, "C", "T")   // x = (1, 1, 1, 1, 1, 1)
    val v8 = Variant("1", 8, "C", "T")   // x = (2, 2, 2, 2, 2, 2)
    val v9 = Variant("1", 9, "C", "T")   // x = (., 1, 1, 1, 1, 1)
    val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

    val qBeta = vds.queryVA("va.linreg.beta")._2
    val qSe = vds.queryVA("va.linreg.se")._2
    val qTstat = vds.queryVA("va.linreg.tstat")._2
    val qPval = vds.queryVA("va.linreg.pval")._2

    val annotationMap = vds.variantsAndAnnotations
      .collect()
      .toMap

    def assertDouble(q: Querier, v: Variant, value: Double) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Double], value))

    def assertEmpty(q: Querier, v: Variant) =
      assert(q(annotationMap(v)) == null)

    /*
    comparing to output of R code:
    y = c(1, 1, 2, 2, 2, 2)
    x = c(0, 1, 0, 0, 0, 1)
    df = data.frame(y, x)
    fit <- lm(y ~ x, data=df)
    summary(fit)
    */

    assertDouble(qBeta, v1, -0.25)
    assertDouble(qSe, v1, 0.4841229)
    assertDouble(qTstat, v1, -0.5163978)
    assertDouble(qPval, v1, 0.63281250)

    /*
    v2 has two missing genotypes, comparing to output of R code as above with imputed genotypes:
    x = c(1, 2, 1, 2, 0, 0)
    */

    assertDouble(qBeta, v2, -0.250000)
    assertDouble(qSe, v2, 0.2602082)
    assertDouble(qTstat, v2, -0.9607689)
    assertDouble(qPval, v2, 0.391075888)

    assertEmpty(qBeta, v6)
    assertEmpty(qBeta, v7)
    assertEmpty(qBeta, v8)
    assertEmpty(qBeta, v9)
    assertEmpty(qBeta, v10)
  }

  @Test def testWithImportFamBoolean() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.cov",
        "Sample",
        root = Some("sa.cov"),
        config = TextTableConfiguration(types = Map("Cov1" -> TDouble, "Cov2" -> TDouble)))
      .annotateSamplesFam("src/test/resources/regressionLinear.fam")
      .linreg("sa.fam.isCase", Array("sa.cov.Cov1", "sa.cov.Cov2"), "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 0.0)

    val v1 = Variant("1", 1, "C", "T")   // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T")   // x = (., 2, ., 2, 0, 0)
    val v6 = Variant("1", 6, "C", "T")   // x = (0, 0, 0, 0, 0, 0)
    val v7 = Variant("1", 7, "C", "T")   // x = (1, 1, 1, 1, 1, 1)
    val v8 = Variant("1", 8, "C", "T")   // x = (2, 2, 2, 2, 2, 2)
    val v9 = Variant("1", 9, "C", "T")   // x = (., 1, 1, 1, 1, 1)
    val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

    val (_, qBeta) = vds.queryVA("va.linreg.beta")
    val (_, qSe) = vds.queryVA("va.linreg.se")
    val (_, qTstat) = vds.queryVA("va.linreg.tstat")
    val (_, qPval) = vds.queryVA("va.linreg.pval")

    val annotationMap = vds.variantsAndAnnotations
      .collect()
      .toMap

    def assertInt(q: Querier, v: Variant, value: Int) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Int], value))

    def assertDouble(q: Querier, v: Variant, value: Double) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Double], value))

    def assertEmpty(q: Querier, v: Variant) =
      assert(q(annotationMap(v)) == null)

    /*
    comparing to output of R code:
    y = c(1, 1, 2, 2, 2, 2)
    x = c(0, 1, 0, 0, 0, 1)
    c1 = c(0, 2, 1, -2, -2, 4)
    c2 = c(-1, 3, 5, 0, -4, 3)
    df = data.frame(y, x, c1, c2)
    fit <- lm(y ~ x + c1 + c2, data=df)
    summary(fit)["coefficients"]

    */

    assertDouble(qBeta, v1, -0.28589421)
    assertDouble(qSe, v1, 1.2739153)
    assertDouble(qTstat, v1, -0.22442167)
    assertDouble(qPval, v1, 0.84327106)

    /*
    v2 has two missing genotypes, comparing to output of R code as above with imputed genotypes:
    x = c(1, 2, 1, 2, 0, 0)
    */

    assertDouble(qBeta, v2, -0.5417647)
    assertDouble(qSe, v2, 0.3350599)
    assertDouble(qTstat, v2, -1.616919)
    assertDouble(qPval, v2, 0.24728705)

    assertEmpty(qBeta, v6)
    assertEmpty(qBeta, v7)
    assertEmpty(qBeta, v8)
    assertEmpty(qBeta, v9)
    assertEmpty(qBeta, v10)
  }

  @Test def testWithImportFam() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.cov",
        "Sample",
        root = Some("sa.cov"),
        config = TextTableConfiguration(types = Map("Cov1" -> TDouble, "Cov2" -> TDouble)))
      .annotateSamplesFam("src/test/resources/regressionLinear.fam",
        config = FamFileConfig(isQuantitative = true, missingValue = "0"))
      .linreg("sa.fam.qPheno", Array("sa.cov.Cov1", "sa.cov.Cov2"), "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 0.0)

    val v1 = Variant("1", 1, "C", "T")   // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T")   // x = (., 2, ., 2, 0, 0)
    val v6 = Variant("1", 6, "C", "T")   // x = (0, 0, 0, 0, 0, 0)
    val v7 = Variant("1", 7, "C", "T")   // x = (1, 1, 1, 1, 1, 1)
    val v8 = Variant("1", 8, "C", "T")   // x = (2, 2, 2, 2, 2, 2)
    val v9 = Variant("1", 9, "C", "T")   // x = (., 1, 1, 1, 1, 1)
    val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

    val qBeta = vds.queryVA("va.linreg.beta")._2
    val qSe = vds.queryVA("va.linreg.se")._2
    val qTstat = vds.queryVA("va.linreg.tstat")._2
    val qPval = vds.queryVA("va.linreg.pval")._2

    val annotationMap = vds.variantsAndAnnotations
      .collect()
      .toMap

    def assertInt(q: Querier, v: Variant, value: Int) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Int], value))

    def assertDouble(q: Querier, v: Variant, value: Double) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Double], value))

    def assertEmpty(q: Querier, v: Variant) =
      assert(q(annotationMap(v)) == null)

    /*
    comparing to output of R code:
    y = c(1, 1, 2, 2, 2, 2)
    x = c(0, 1, 0, 0, 0, 1)
    c1 = c(0, 2, 1, -2, -2, 4)
    c2 = c(-1, 3, 5, 0, -4, 3)
    df = data.frame(y, x, c1, c2)
    fit <- lm(y ~ x + c1 + c2, data=df)
    summary(fit)["coefficients"]

    */

    assertDouble(qBeta, v1, -0.28589421)
    assertDouble(qSe, v1, 1.2739153)
    assertDouble(qTstat, v1, -0.22442167)
    assertDouble(qPval, v1, 0.84327106)

    /*
    v2 has two missing genotypes, comparing to output of R code as above with imputed genotypes:
    x = c(1, 2, 1, 2, 0, 0)
    */

    assertDouble(qBeta, v2, -0.5417647)
    assertDouble(qSe, v2, 0.3350599)
    assertDouble(qTstat, v2, -1.616919)
    assertDouble(qPval, v2, 0.24728705)

    assertEmpty(qBeta, v6)
    assertEmpty(qBeta, v7)
    assertEmpty(qBeta, v8)
    assertEmpty(qBeta, v9)
    assertEmpty(qBeta, v10)
  }

  @Test def testNonNumericPheno() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.cov",
        "Sample",
        root = Some("sa.cov"),
        config = TextTableConfiguration(types = Map("Cov1" -> TDouble, "Cov2" -> TDouble)))
      .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
        "Sample",
        root = Some("sa.pheno"),
        config = TextTableConfiguration(missing = "0"))

    interceptFatal("Sample annotation `sa.pheno.Pheno' must be numeric or Boolean, got String") {
      vds.linreg("sa.pheno.Pheno", Array("sa.cov.Cov1", "sa.cov.Cov2"), "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 0.0)
    }
  }

  @Test def testNonNumericCov() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.cov",
        "Sample",
        root = Some("sa.cov"),
        config = TextTableConfiguration(types = Map("Cov1" -> TDouble)))
      .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
        "Sample",
        root = Some("sa.pheno"),
        config = TextTableConfiguration(types = Map("Pheno" -> TDouble), missing = "0"))

    interceptFatal("Sample annotation `sa.cov.Cov2' must be numeric or Boolean, got String") {
      vds.linreg("sa.pheno.Pheno", Array("sa.cov.Cov1", "sa.cov.Cov2"), "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 0.0)
    }
  }

  @Test def testFilters() {
    var vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
        "Sample",
        root = Some("sa.pheno"),
        config = TextTableConfiguration(types = Map("Pheno" -> TDouble), missing = "0"))


    val v1 = Variant("1", 1, "C", "T")
    // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T") // x = (., 2, ., 2, 0, 0)

    def annotationMap = vds.variantsAndAnnotations
      .collect()
      .toMap

    vds = vds.linreg("sa.pheno.Pheno", Array.empty[String], "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 4, 0.0)

    def qBeta = vds.queryVA("va.linreg.beta")._2

    assert(qBeta(annotationMap(v1)) == null)
    assert(qBeta(annotationMap(v2)) != null)

    // only 6 samples are included, so 12 alleles total
    vds = vds.linreg("sa.pheno.Pheno", Array.empty[String], "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 0.3)

    assert(qBeta(annotationMap(v1)) == null)
    assert(qBeta(annotationMap(v2)) != null)

    vds = vds.linreg("sa.pheno.Pheno", Array.empty[String], "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 0.4)

    assert(qBeta(annotationMap(v1)) == null)
    assert(qBeta(annotationMap(v2)) == null)

    vds = vds.linreg("sa.pheno.Pheno", Array.empty[String], "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 0.3)

    assert(qBeta(annotationMap(v1)) == null)
    assert(qBeta(annotationMap(v2)) != null)

    vds = vds.linreg("sa.pheno.Pheno", Array.empty[String], "va.linreg", useDosages = false,  useDominance = false, useNormalise = false, 5, 0.1)

    assert(qBeta(annotationMap(v1)) == null)
    assert(qBeta(annotationMap(v2)) == null)
  }

  @Test def testFiltersFatals() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
        "Sample",
        root = Some("sa.pheno"),
        config = TextTableConfiguration(types = Map("Pheno" -> TDouble), missing = "0"))

    interceptFatal("Minumum alternate allele count must be a positive integer, got 0") {
      vds.linreg("sa.pheno.Pheno", Array.empty[String], "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 0, 0.0)
    }

    interceptFatal("Minumum alternate allele frequency must lie in") {
      vds.linreg("sa.pheno.Pheno", Array.empty[String], "va.linreg", useDosages = false, useDominance = false, useNormalise = false, 1, 2.0)
    }
  }

  @Test def testWithTwoCovDom() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.cov",
        "Sample",
        root = Some("sa.cov"),
        config = TextTableConfiguration(types = Map("Cov1" -> TDouble, "Cov2" -> TDouble)))
      .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
        "Sample",
        root = Some("sa.pheno"),
        config = TextTableConfiguration(types = Map("Pheno" -> TDouble), missing = "0"))
      .linreg("sa.pheno.Pheno", Array("sa.cov.Cov1", "sa.cov.Cov2 + 1 - 1"), "va.linreg", useDosages = false, useDominance = true, useNormalise = false, 1, 0.0)

    val v1 = Variant("1", 1, "C", "T")   // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T")   // x = (., 2, ., 2, 0, 0)
    val v3 = Variant("1", 3, "C", "T")   // x = (0, ., 1, 1, 1, .)
    val v6 = Variant("1", 6, "C", "T")   // x = (0, 0, 0, 0, 0, 0)
    val v7 = Variant("1", 7, "C", "T")   // x = (1, 1, 1, 1, 1, 1)
    val v8 = Variant("1", 8, "C", "T")   // x = (2, 2, 2, 2, 2, 2)
    val v9 = Variant("1", 9, "C", "T")   // x = (., 1, 1, 1, 1, 1)
    val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

    val qBeta = vds.queryVA("va.linreg.beta")._2
    val qSe = vds.queryVA("va.linreg.se")._2
    val qTstat = vds.queryVA("va.linreg.tstat")._2
    val qPval = vds.queryVA("va.linreg.pval")._2

    val annotationMap = vds.variantsAndAnnotations
      .collect()
      .toMap

    def assertInt(q: Querier, v: Variant, value: Int) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Int], value))

    def assertDouble(q: Querier, v: Variant, value: Double) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Double], value))

    def assertEmpty(q: Querier, v: Variant) =
      assert(q(annotationMap(v)) == null)

    /*
    comparing to output of R code:
    y = c(1, 1, 2, 2, 2, 2)
    x = c(0, 1, 0, 0, 0, 1)
    c1 = c(0, 2, 1, -2, -2, 4)
    c2 = c(-1, 3, 5, 0, -4, 3)
    df = data.frame(y, x, c1, c2)
    fit <- lm(y ~ x + c1 + c2, data=df)
    summary(fit)["coefficients"]
    */

    assertDouble(qBeta, v1, -0.23824517)
    assertDouble(qSe, v1, 1.0615961)
    assertDouble(qTstat, v1, -0.22442167)
    assertDouble(qPval, v1, 0.84327106)

    assertEmpty(qBeta, v2)

    assertDouble(qBeta, v3, 0.67104491)
    assertDouble(qSe, v3, 0.4227718)
    assertDouble(qTstat, v3, 1.5872510)
    assertDouble(qPval, v3, 0.2533675)

    assertEmpty(qBeta, v6)
    assertEmpty(qBeta, v7)
    assertEmpty(qBeta, v8)
    assertEmpty(qBeta, v9)
    assertEmpty(qBeta, v10)
  }

  @Test def testWithTwoCovDomNorm() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
    .splitMulti()
    .annotateSamplesTable("src/test/resources/regressionLinear.cov",
    "Sample",
    root = Some("sa.cov"),
    config = TextTableConfiguration(types = Map("Cov1" -> TDouble, "Cov2" -> TDouble)))
    .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
    "Sample",
    root = Some("sa.pheno"),
    config = TextTableConfiguration(types = Map("Pheno" -> TDouble), missing = "0"))
    .linreg("sa.pheno.Pheno", Array("sa.cov.Cov1", "sa.cov.Cov2 + 1 - 1"), "va.linreg", useDosages = false, useDominance = true, useNormalise = true, 1, 0.0)

    val v1 = Variant("1", 1, "C", "T")   // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T")   // x = (., 2, ., 2, 0, 0)
    val v3 = Variant("1", 3, "C", "T")   // x = (0, ., 1, 1, 1, .)
    val v6 = Variant("1", 6, "C", "T")   // x = (0, 0, 0, 0, 0, 0)
    val v7 = Variant("1", 7, "C", "T")   // x = (1, 1, 1, 1, 1, 1)
    val v8 = Variant("1", 8, "C", "T")   // x = (2, 2, 2, 2, 2, 2)
    val v9 = Variant("1", 9, "C", "T")   // x = (., 1, 1, 1, 1, 1)
    val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

    val qBeta = vds.queryVA("va.linreg.beta")._2
    val qSe = vds.queryVA("va.linreg.se")._2
    val qTstat = vds.queryVA("va.linreg.tstat")._2
    val qPval = vds.queryVA("va.linreg.pval")._2

    val annotationMap = vds.variantsAndAnnotations
    .collect()
    .toMap

    def assertInt(q: Querier, v: Variant, value: Int) =
    assert(D_==(q(annotationMap(v)).asInstanceOf[Int], value))

    def assertDouble(q: Querier, v: Variant, value: Double) =
    assert(D_==(q(annotationMap(v)).asInstanceOf[Double], value))

    def assertEmpty(q: Querier, v: Variant) =
    assert(q(annotationMap(v)) == null)

    assertDouble(qBeta, v1, -0.28589421)
    assertDouble(qSe, v1, 1.2739153)
    assertDouble(qTstat, v1, -0.22442167)
    assertDouble(qPval, v1, 0.84327106)

    assertEmpty(qBeta, v2)

    assertDouble(qBeta, v3, 0.80525389)
    assertDouble(qSe, v3, 0.5073261)
    assertDouble(qTstat, v3, 1.5872510)
    assertDouble(qPval, v3, 0.2533675)

    assertEmpty(qBeta, v6)
    assertEmpty(qBeta, v7)
    assertEmpty(qBeta, v8)
    assertEmpty(qBeta, v9)
    assertEmpty(qBeta, v10)
  }

  @Test def testWithTwoCovNorm() {
    val vds = hc.importVCF("src/test/resources/regressionLinear.vcf")
      .splitMulti()
      .annotateSamplesTable("src/test/resources/regressionLinear.cov",
        "Sample",
        root = Some("sa.cov"),
        config = TextTableConfiguration(types = Map("Cov1" -> TDouble, "Cov2" -> TDouble)))
      .annotateSamplesTable("src/test/resources/regressionLinear.pheno",
        "Sample",
        root = Some("sa.pheno"),
        config = TextTableConfiguration(types = Map("Pheno" -> TDouble), missing = "0"))
      .linreg("sa.pheno.Pheno", Array("sa.cov.Cov1", "sa.cov.Cov2 + 1 - 1"), "va.linreg", useDosages = false, useDominance = false, useNormalise = true, 1, 0.0)

    val v1 = Variant("1", 1, "C", "T")   // x = (0, 1, 0, 0, 0, 1)
    val v2 = Variant("1", 2, "C", "T")   // x = (., 2, ., 2, 0, 0)
    val v3 = Variant("1", 3, "C", "T")   // x = (0, ., 1, 1, 1, .)
    val v6 = Variant("1", 6, "C", "T")   // x = (0, 0, 0, 0, 0, 0)
    val v7 = Variant("1", 7, "C", "T")   // x = (1, 1, 1, 1, 1, 1)
    val v8 = Variant("1", 8, "C", "T")   // x = (2, 2, 2, 2, 2, 2)
    val v9 = Variant("1", 9, "C", "T")   // x = (., 1, 1, 1, 1, 1)
    val v10 = Variant("1", 10, "C", "T") // x = (., 2, 2, 2, 2, 2)

    val qBeta = vds.queryVA("va.linreg.beta")._2
    val qSe = vds.queryVA("va.linreg.se")._2
    val qTstat = vds.queryVA("va.linreg.tstat")._2
    val qPval = vds.queryVA("va.linreg.pval")._2

    val annotationMap = vds.variantsAndAnnotations
      .collect()
      .toMap

    def assertInt(q: Querier, v: Variant, value: Int) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Int], value))

    def assertDouble(q: Querier, v: Variant, value: Double) =
      assert(D_==(q(annotationMap(v)).asInstanceOf[Double], value))

    def assertEmpty(q: Querier, v: Variant) =
      assert(q(annotationMap(v)) == null)

    assertDouble(qBeta, v1, -0.28589421)
    assertDouble(qSe, v1, 1.2739153)
    assertDouble(qTstat, v1, -0.22442167)
    assertDouble(qPval, v1, 0.84327106)

    assertDouble(qBeta, v2, -0.9383640)
    assertDouble(qSe, v2, 0.5803407)
    assertDouble(qTstat, v2, -1.616919)
    assertDouble(qPval, v2, 0.24728705)

    assertDouble(qBeta, v3, 0.80525389)
    assertDouble(qSe, v3, 0.5073261)
    assertDouble(qTstat, v3, 1.5872510)
    assertDouble(qPval, v3, 0.2533675)

    assertEmpty(qBeta, v6)
    assertEmpty(qBeta, v7)
    assertEmpty(qBeta, v8)
    assertEmpty(qBeta, v9)
    assertEmpty(qBeta, v10)
  }
}