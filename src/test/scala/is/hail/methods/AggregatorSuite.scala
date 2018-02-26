package is.hail.methods

import is.hail.check.{Gen, Prop}
import is.hail.expr._
import is.hail.expr.types._
import is.hail.table.Table
import is.hail.utils._
import is.hail.testUtils._
import is.hail.variant.{MatrixTable, VSMSubgen, Variant}
import is.hail.{SparkSuite, TestUtils}
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class AggregatorSuite extends SparkSuite {

  @Test def testRows() {
    var vds = hc.importVCF("src/test/resources/sample2.vcf")
    vds = SplitMulti(vds)
    vds = VariantQC(vds, "qc")
    vds = vds
      .annotateVariantsExpr(
        """test.callrate = AGG.fraction(g => isDefined(g.GT)),
          |test.AC = AGG.map(g => g.GT.nNonRefAlleles()).sum(),
          |test.AF = AGG.map(g => g.GT.nNonRefAlleles()).stats().sum.toFloat64() / AGG.filter(g => isDefined(g.GT)).count() / 2.0,
          |test.gqstats = AGG.map(g => g.GQ).stats(), test.gqhetstats = AGG.filter(g => g.GT.isHet()).map(g => g.GQ).stats(),
          |lowGqGts = AGG.filter(g => g.GQ < 60).collect()""".stripMargin)

    val qCallRate = vds.queryVA("va.test.callrate")._2
    val qCallRateQC = vds.queryVA("va.qc.callRate")._2
    val qAC = vds.queryVA("va.test.AC")._2
    val qACQC = vds.queryVA("va.qc.AC")._2
    val qAF = vds.queryVA("va.test.AF")._2
    val qAFQC = vds.queryVA("va.qc.AF")._2
    val gqStatsMean = vds.queryVA("va.test.gqstats.mean")._2
    val gqStatsMeanQC = vds.queryVA("va.qc.gqMean")._2
    val gqStatsStDev = vds.queryVA("va.test.gqstats.stdev")._2
    val gqStatsStDevQC = vds.queryVA("va.qc.gqStDev")._2
    val gqStatsHetMean = vds.queryVA("va.test.gqhetstats.mean")._2
    val gqStatsHetStDev = vds.queryVA("va.test.gqhetstats.stdev")._2
    val lowGqGts = vds.queryVA("va.lowGqGts")._2

    vds.rdd.collect()
      .foreach { case (v, (va, gs)) =>
        assert(qCallRate(va) == qCallRateQC(va))
        assert(qAC(va) == qACQC(va))
        assert(D_==(qAF(va).asInstanceOf[Double], qAFQC(va).asInstanceOf[Double]))
        assert(Option(gqStatsMean(va)).zip(Option(gqStatsMeanQC(va))).forall {
          case (a, b) => D_==(a.asInstanceOf[Double], b.asInstanceOf[Double])
        })
        assert(Option(gqStatsStDev(va)).zip(Option(gqStatsStDevQC(va))).forall {
          case (a, b) => D_==(a.asInstanceOf[Double], b.asInstanceOf[Double])
        })
      }
  }

  @Test def testColumns() {
    val vds = SampleQC(hc.importVCF("src/test/resources/sample2.vcf"))
      .annotateSamplesExpr(
        """test.callrate = AGG.fraction(g => isDefined(g.GT)),
          |test.nCalled = AGG.filter(g => isDefined(g.GT)).count(),
          |test.nNotCalled = AGG.filter(g => !isDefined(g.GT)).count(),
          |test.gqstats = AGG.map(g => g.GQ).stats(),
          |test.gqhetstats = AGG.filter(g => g.GT.isHet()).map(g => g.GQ).stats(),
          |test.nHet = AGG.filter(g => g.GT.isHet()).count(),
          |test.nHomRef = AGG.filter(g => g.GT.isHomRef()).count(),
          |test.nHomVar = AGG.filter(g => g.GT.isHomVar()).count(),
          |test.nSNP = AGG.map(g => [g.GT[0], g.GT[1]].map(x => if (x > 0 && is_snp(va.alleles[0], va.alleles[x])) 1 else 0).sum()).sum(),
          |test.nInsertion = AGG.map(g => [g.GT[0], g.GT[1]].map(x => if (x > 0 && is_insertion(va.alleles[0], va.alleles[x])) 1 else 0).sum()).sum(),
          |test.nDeletion = AGG.map(g => [g.GT[0], g.GT[1]].map(x => if (x > 0 && is_deletion(va.alleles[0], va.alleles[x])) 1 else 0).sum()).sum(),
          |test.nTi = AGG.map(g => [g.GT[0], g.GT[1]].map(x => if (x > 0 && is_transition(va.alleles[0], va.alleles[x])) 1 else 0).sum()).sum(),
          |test.nTv = AGG.map(g => [g.GT[0], g.GT[1]].map(x => if (x > 0 && is_transversion(va.alleles[0], va.alleles[x])) 1 else 0).sum()).sum(),
          |test.nStar = AGG.map(g => [g.GT[0], g.GT[1]].map(x => if (x > 0 && is_star(va.alleles[0], va.alleles[x])) 1 else 0).sum()).sum()""".stripMargin)

    val qCallRate = vds.querySA("sa.test.callrate")._2
    val qCallRateQC = vds.querySA("sa.qc.callRate")._2
    val gqStatsMean = vds.querySA("sa.test.gqstats.mean")._2
    val gqStatsMeanQC = vds.querySA("sa.qc.gqMean")._2
    val gqStatsStDev = vds.querySA("sa.test.gqstats.stdev")._2
    val gqStatsStDevQC = vds.querySA("sa.qc.gqStDev")._2
    val gqStatsHetMean = vds.querySA("sa.test.gqhetstats.mean")._2
    val gqStatsHetStDev = vds.querySA("sa.test.gqhetstats.stdev")._2
    val nHomRef = vds.querySA("sa.test.nHomRef")._2
    val nHet = vds.querySA("sa.test.nHet")._2
    val nHomVar = vds.querySA("sa.test.nHomVar")._2

    vds.stringSampleIds.zip(vds.colValues)
      .foreach {
        case (s, sa) =>
          assert(qCallRate(sa) == qCallRateQC(sa))
          assert(Option(gqStatsMean(sa)).zip(Option(gqStatsMeanQC(sa))).forall {
            case (a, b) => D_==(a.asInstanceOf[Double], b.asInstanceOf[Double])
          })
          assert(Option(gqStatsStDev(sa)).zip(Option(gqStatsStDevQC(sa))).forall {
            case (a, b) => D_==(a.asInstanceOf[Double], b.asInstanceOf[Double])
          })
      }

    assert(vds.colsTable().forall("row.qc.nCalled == row.test.nCalled"))
    assert(vds.colsTable().forall("row.qc.nNotCalled == row.test.nNotCalled"))
    assert(vds.colsTable().forall("row.qc.callRate == row.test.callrate"))
    assert(vds.colsTable().forall("row.qc.nHet == row.test.nHet"))
    assert(vds.colsTable().forall("row.qc.nHomVar == row.test.nHomVar"))
    assert(vds.colsTable().forall("row.qc.nHomRef == row.test.nHomRef"))
    assert(vds.colsTable().forall("row.qc.nSNP == row.test.nSNP"))
    assert(vds.colsTable().forall("row.qc.nInsertion == row.test.nInsertion"))
    assert(vds.colsTable().forall("row.qc.nDeletion == row.test.nDeletion"))
    assert(vds.colsTable().forall("row.qc.nTransition == row.test.nTi"))
    assert(vds.colsTable().forall("row.qc.nTransversion == row.test.nTv"))
    assert(vds.colsTable().forall("row.qc.nStar == row.test.nStar"))
  }

  @Test def testSum() {
    val p = Prop.forAll(MatrixTable.gen(hc, VSMSubgen.random)) { vds =>
      var vds2 = SplitMulti(vds)
      vds2 = VariantQC(vds2, "qc")
      vds2 = vds2
        .annotateVariantsExpr("oneHotAC = AGG.map(g => g.GT.oneHotAlleles(va.alleles)).sum()")
        .annotateVariantsExpr("same = (AGG.filter(g => isDefined(g.GT)).count() == 0) || " +
          "(va.oneHotAC[0] == va.qc.nCalled * 2  - va.qc.AC) && (va.oneHotAC[1] == va.qc.nHet + 2 * va.qc.nHomVar)")
      val (_, querier) = vds2.queryVA("va.same")
      vds2.variantsAndAnnotations
        .forall { case (v, va) =>
          Option(querier(va)).exists(_.asInstanceOf[Boolean])
        }
    }
    p.check()
  }

  @Test def testMaxMin() {
    val rdd = sc.parallelize(Seq(
      Row("a", 0, null, 1, -1, null, null, 1, null, 0l, 0f, 0d),
      Row("a", -1, -1, null, -2, null, 1, null, null, -1l, -1f, -1d),
      Row("a", 1, -2, 2, null, -1, null, null, null, 1l, 1f, 1d)), numSlices = 2)

    val signature = TStruct((("group" -> TString()) +: (0 until 8).map(i => s"s$i" -> TInt32()))
      ++ IndexedSeq("s8" -> TInt64(), "s9" -> TFloat32(), "s10" -> TFloat64()): _*)

    val ktMax = Table(hc, rdd, signature)
      .aggregate("group = row.group", (0 until 11).map(i => s"s$i = AGG.map(r => r.s$i).max()").mkString(","))

    assert(ktMax.collect() sameElements Array(Row("a", 1, -1, 2, -1, -1, 1, 1, null, 1l, 1f, 1d)))

    val ktMin = Table(hc, rdd, signature)
      .aggregate("group = row.group", (0 until 11).map(i => s"s$i = AGG.map(r => r.s$i).min()").mkString(","))

    assert(ktMin.collect() sameElements Array(Row("a", -1, -2, 1, -2, -1, 1, 1, null, -1l, -1f, -1d)))
  }

  @Test def testProduct() {
    val rdd = sc.parallelize(Seq(
      Row("a", 0, null, 1, 1, null, null, 10, null, 0l, 2f, 0d),
      Row("a", -1, -1, null, 2, null, 1, 4, null, -1l, -1f, -1d),
      Row("a", 1, -2, 2, 3, -1, -3, 2, null, 1l, 2f, 1d)), numSlices = 2)

    val signature = TStruct((("group" -> TString()) +: (0 until 8).map(i => s"s$i" -> TInt32()))
      ++ IndexedSeq("s8" -> TInt64(), "s9" -> TFloat32(), "s10" -> TFloat64()): _*)

    val ktProduct = Table(hc, rdd, signature)
      .aggregate("group = row.group", ((0 until 11).map(i => s"s$i = AGG.map(r => r.s$i).product()") :+ ("empty = AGG.map(r => r.s10).filter(x => false).product()")).mkString(","))

    assert(ktProduct.collect() sameElements Array(Row("a", 0l, 2l, 2l, 6l, -1l, -3l, 80l, 1l, 0l, -4d, 0d, 1d)))

  }

  @Test def testHist() {
    val vds = hc.importVCF("src/test/resources/sample2.vcf").cache()

    assert(vds.annotateVariantsExpr(
      """
        hist = AGG.map(g => g.GQ).hist(0, 100, 20),
        bin0 = AGG.filter(g => g.GQ < 5).count(),
        bin1 = AGG.filter(g => g.GQ >= 5 && g.GQ < 10).count(),
        last = AGG.filter(g => g.GQ >= 95).count()""")
      .rowsTable()
      .forall(
        """
        row.hist.binFrequencies[0] == row.bin0 &&
        row.hist.binFrequencies[1] == row.bin1 &&
        row.hist.binFrequencies[-1] == row.last"""))

    assert(vds
      .annotateVariantsExpr(
        """
        hist = AGG.map(g => g.GQ).hist(22, 80, 5),
        nLess = AGG.filter(g => g.GQ < 22).count(),
        nGreater = AGG.filter(g => g.GQ > 80).count()""")
      .rowsTable()
      .forall(
        """
        row.hist.nLess == row.nLess &&
        row.hist.nGreater == row.nGreater"""))
  }

  @Test def testCallStats() {
    val vds = hc.importVCF("src/test/resources/sample2.vcf").cache()
      .annotateVariantsExpr(
        """callStats = AGG.map(g => g.GT).callStats(g => va.alleles),
          |AC = AGG.map(g => g.GT.oneHotAlleles(va.alleles)).sum(),
          |AN = AGG.filter(g => isDefined(g.GT)).count() * 2""".stripMargin)
      .annotateVariantsExpr("AF = va.AC / va.AN.toFloat64()")
    val (_, csAC) = vds.queryVA("va.callStats.AC")
    val (_, csAF) = vds.queryVA("va.callStats.AF")
    val (_, csAN) = vds.queryVA("va.callStats.AN")
    val (_, ac) = vds.queryVA("va.AC")
    val (_, af) = vds.queryVA("va.AF")
    val (_, an) = vds.queryVA("va.AN")

    vds.variantsAndAnnotations
      .collect()
      .foreach { case (_, va) =>
        assert(csAC(va) == ac(va), s"AC was different")
        assert(csAN(va) == an(va), s"AN was different")
        assert(csAF(va) == af(va), s"AF was different")
      }
  }

  @Test def testCounter() {
    Prop.forAll(MatrixTable.gen(hc, VSMSubgen.plinkSafeBiallelic)) { vds =>
      val (r, t) = vds.queryVariants("AGG.map(_ => va.locus.contig).counter()")
      val counterMap = r.asInstanceOf[Map[String, Long]]
      val aggMap = vds.variants.map(_.asInstanceOf[Variant].contig).countByValue()
      aggMap == counterMap
    }.check()
  }

  @Test def testTake() {
    val vds = hc.importVCF("src/test/resources/aggTake.vcf")
      .annotateVariantsExpr("take = AGG.map(g => g.DP).take(3)")
      .annotateVariantsExpr("takeBy = AGG.map(g => g.DP).takeBy(dp => g.GQ, 3)")

    val (_, qTake) = vds.queryVA("va.take")
    val (_, qTakeBy) = vds.queryVA("va.takeBy")

    val va = vds.variantsAndAnnotations.map(_._2).collect().head
    assert(qTake(va) == IndexedSeq[Any](11, null, 20))
    assert(qTakeBy(va) == IndexedSeq[Any](5, 20, 1))
  }

  @Test def testTransformations() {
    val p = Prop.forAll(
      for {
        vds <- MatrixTable.gen(hc, VSMSubgen.random
          .copy(sGen = _ => Gen.oneOf("a", "b")))
          .filter(vds => vds.numCols > 0);
        s <- Gen.choose(0, vds.numCols - 1)
      } yield {
        val s1 = vds.stringSampleIds(0)
        assert(vds.querySamples(s"""AGG.map(r => if (r.s == "$s1") (NA : String) else r.s).map(x => 1).sum()""")._1 == vds.numCols)
        assert(vds.querySamples("AGG.filter(r => true).map(id => 1).sum()")._1 == vds.numCols)
        assert(vds.querySamples("AGG.filter(r => false).map(id => 1).sum()")._1 == 0)
        assert(vds.querySamples("AGG.flatMap(g => [1]).sum()")._1 == vds.numCols)
        assert(vds.querySamples("AGG.flatMap(g => [0][:0]).sum()")._1 == 0)
        assert(vds.querySamples("AGG.flatMap(g => [1,2]).sum()")._1 == 3 * vds.numCols)
        assert(vds.querySamples("AGG.flatMap(g => [1,2]).filter(x => x % 2 == 0).sum()")._1 == 2 * vds.numCols)
        assert(vds.querySamples("AGG.flatMap(g => [1,2,2].toSet()).filter(x => x % 2 == 0).sum()")._1 == 2 * vds.numCols)

        vds.annotateVariantsExpr(s"""foo = AGG.filter(g => sa.s == "$s1").map(g => 1).sum()""")
          .rowsTable()
          .forall("row.foo == 1")
      })

    p.check()
  }

  @Test def testQueryGenotypes() {
    Prop.forAll(MatrixTable.gen(hc, VSMSubgen.random)) { vds =>
      val countResult = vds.summarize().callRate.getOrElse(null)
      val (queryResult, t) = vds.queryGenotypes("AGG.fraction(g => isDefined(g.GT))")
      assert(t.valuesSimilar(countResult, queryResult))

      val filterCountResult = vds.filterGenotypes("va.locus.position % 2 == 1").summarize().callRate.getOrElse(null)
      val (queryResult2, t2) = vds.queryGenotypes("AGG.fraction(g => (va.locus.position % 2 == 1) && isDefined(g.GT))")
      assert(t2.valuesSimilar(filterCountResult, queryResult2))
      true
    }.check()
  }

  private def isLensedPrefix[T, K](lens: T => K)(prefix: Seq[T], full: Seq[T]): Boolean = {
    prefix.zip(full).forall { case (x, y) => lens(x) == lens(y) }
  }

  private def prefixModuloDisordering[T, K](sortBy: T => K)(prefix: Seq[T], full: Seq[T]): Boolean = {
    def equivClasses(ts: Seq[T]): Map[K, Set[T]] =
      ts.groupBy(sortBy).mapValues(_.toSet)

    if (prefix.isEmpty) {
      true
    } else {
      val sameOrdering = isLensedPrefix(sortBy)(prefix, full)

      val lastKey = sortBy(prefix.last)

      val prefixEquivClasses = equivClasses(prefix)
      val fullEquivClasses = equivClasses(full)

      val wholeClassesPrefix = prefixEquivClasses.filterKeys(_ != lastKey)
      val wholeClassesFull = fullEquivClasses.filterKeys(k => wholeClassesPrefix.keySet.contains(k))

      val wholeClassesSame = wholeClassesFull == wholeClassesPrefix

      val lastKeySubset = prefixEquivClasses(lastKey).subsetOf(fullEquivClasses(lastKey))

      if (sameOrdering) {
        if (wholeClassesSame) {
          if (lastKeySubset) {
            true
          } else {
            println(s"The values at the last key in the prefix, $lastKey, were not a subset of those in the full list: ${ prefixEquivClasses(lastKey) } ${ fullEquivClasses(lastKey) }")
            false
          }
        } else {
          println(s"The values differed at some key:\n$wholeClassesPrefix\n$wholeClassesFull")
          false
        }
      } else {
        println(s"The sequences didn't have the same ordering:\n$prefix\n$full")
        false
      }
    }
  }

  @Test def takeByAndSortByAgree() {
    val rng = new RandomDataGenerator()
    rng.reSeed(Prop.seed)

    Prop.forAll(MatrixTable.gen(hc, VSMSubgen.realistic)) { (vds: MatrixTable) =>
      val Array((a, _), (b, _)) = vds.queryGenotypes(Array("AGG.collect().sortBy(g => g.GQ).map(g => [g.DP, g.GQ])",
        "AGG.map(g => [g.DP, g.GQ]).takeBy(x => x[1], 10)"))
      val sortby = a.asInstanceOf[IndexedSeq[IndexedSeq[java.lang.Integer]]]
      val takeby = b.asInstanceOf[IndexedSeq[IndexedSeq[java.lang.Integer]]]

      if (!prefixModuloDisordering((x: Seq[java.lang.Integer]) => x(1))(takeby, sortby)) {
        println(s"The first sequence is not a prefix, up to irrelevant disorderings, of the second sequence\n$takeby\n$sortby")
        false
      } else {
        true
      }
    }.check()
  }

  @Test def takeByAndSortByAgreeUsingLatentEnvironment() {
    val rng = new RandomDataGenerator()
    rng.reSeed(Prop.seed)

    Prop.forAll(MatrixTable.gen(hc, VSMSubgen.realistic)) { (vds: MatrixTable) =>
      vds.typecheck()

      val Array((a, _), (b, _)) = vds.queryGenotypes(Array("AGG.collect().sortBy(g => g.GQ).map(g => [g.DP, g.GQ])",
        "AGG.map(g => [g.DP, g.GQ]).takeBy(x => g.GQ, 10)"))
      val sortby = a.asInstanceOf[IndexedSeq[IndexedSeq[java.lang.Integer]]]
      val takeby = b.asInstanceOf[IndexedSeq[IndexedSeq[java.lang.Integer]]]

      if (!prefixModuloDisordering((x: Seq[java.lang.Integer]) => x(1))(takeby, sortby)) {
        println(s"The first sequence is not a prefix, up to irrelevant disorderings, of the second sequence\n$takeby\n$sortby")
        false
      } else {
        true
      }
    }.check()
  }

  private def runAggregatorExpression(expr: String, aggregableName: String, aggregableElementType: Type, aggregableValue: TraversableOnce[_]): Any = {
    val ec = EvalContext(Map(aggregableName -> (0, TAggregable(aggregableElementType, Map("x" -> (0, aggregableElementType))))))

    val compiledCode = Parser.parseToAST(expr, ec).compile().run(Seq(), ec)
    val aggs = ec.aggregations.map { case (_, _, agg0) => agg0.copy() }
    for (x <- aggregableValue) {
      ec.a(0) = x
      for ((_, transform, agg) <- ec.aggregations) {
        transform(agg.seqOp)
      }
    }
    for ((resultBox, _, agg) <- ec.aggregations) {
      resultBox.v = agg.result
    }

    compiledCode()
  }

  private val na = null

  private def doubleSeqEq(xs: Seq[java.lang.Double], ys: Seq[java.lang.Double]) =
    xs.zip(ys).forall {
      case (null, null) => true
      case (x, null) => false
      case (null, y) => false
      case (x, y) if x.isNaN && y.isNaN => true
      case (x, y) if x.isNaN => false
      case (x, y) if y.isNaN => false
      case (x, y) => x == y
    }

  @Test def testExistsForAll() {
    val gs = Array(7, 6, 3, na, 1, 2, na, 4, 5, -1)

    assert(runAggregatorExpression("AGG.exists(x => x < 0)", "AGG", TInt32(), gs) == true)
    assert(runAggregatorExpression("AGG.exists(x => x < -2)", "AGG", TInt32(), gs) == false)

    assert(runAggregatorExpression("AGG.forall(x => isMissing(x) || x.abs() < 10)", "AGG", TInt32(), gs) == true)
    assert(runAggregatorExpression("AGG.forall(x => x > -2)", "AGG", TInt32(), gs) == false) // because missing
  }

  @Test def takeByNAIsAlwaysLast() {
    val inf = Double.PositiveInfinity
    val nan = Double.NaN

    val xs = Array(inf, -1.0, 1.0, 0.0, -inf, na, nan)

    val ascending = runAggregatorExpression("xs.takeBy(x => x, 7)", "xs", TFloat64(), xs)
      .asInstanceOf[IndexedSeq[java.lang.Double]]

    assert(doubleSeqEq(ascending, IndexedSeq(-inf, -1.0, 0.0, 1.0, inf, nan, na)),
      s"expected ascending sequence of `java.lang.Double`s, but got: $ascending")

    val descending = runAggregatorExpression("xs.takeBy(x => -x, 7)", "xs", TFloat64(), xs)
      .asInstanceOf[IndexedSeq[java.lang.Double]]

    assert(doubleSeqEq(descending, IndexedSeq(inf, 1.0, 0.0, -1.0, -inf, nan, na)),
      s"expected descending sequence of `java.lang.Double`s, but got: $descending")
  }

  @Test def takeByIntExampleFromDocsIsCorrect() {
    val gs = Array(7, 6, 3, na, 1, 2, na, 4, 5, -1)

    {
      val result = runAggregatorExpression("AGG.takeBy(x => x, 5)", "AGG", TInt32(), gs)
        .asInstanceOf[IndexedSeq[java.lang.Integer]]
      assert(result == IndexedSeq(-1, 1, 2, 3, 4))
    }

    {
      val result = runAggregatorExpression("AGG.takeBy(x => -x, 5)", "AGG", TInt32(), gs)
        .asInstanceOf[IndexedSeq[java.lang.Integer]]
      assert(result == IndexedSeq(7, 6, 5, 4, 3))
    }

    {
      val result = runAggregatorExpression("AGG.takeBy(x => x, 10)", "AGG", TInt32(), gs)
        .asInstanceOf[IndexedSeq[java.lang.Integer]]
      assert(result == IndexedSeq(-1, 1, 2, 3, 4, 5, 6, 7, na, na))
    }
  }

  @Test def takeByMoreThanExist() {
    val result = runAggregatorExpression("xs.takeBy(x => x, 10)", "xs", TInt32(), Array(0, 1, 2))
      .asInstanceOf[IndexedSeq[java.lang.Integer]]
    assert(result == IndexedSeq(0, 1, 2))
  }

  @Test def testCollectAsSet() {
    val kt = Table.range(hc, 100, partitions = Some(10))

    assert(kt.query(Array("AGG.map(r => r.index).collectAsSet()"))(0)._1 == (0 until 100).toSet)
    assert(kt.union(kt, kt).query(Array("AGG.map(r => r.index).collectAsSet()"))(0)._1 == (0 until 100).toSet)
  }
}
