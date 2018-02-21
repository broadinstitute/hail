package is.hail.methods

import is.hail.SparkSuite
import is.hail.expr.Parser
import is.hail.expr.types._
import is.hail.utils._
import is.hail.testUtils._
import org.testng.annotations.Test

import scala.io.Source

class ExportSuite extends SparkSuite {

  @Test def test() {
    var vds = hc.importVCF("src/test/resources/sample.vcf")
    vds = SplitMulti(vds)
    vds = SampleQC(vds)

    val out = tmpDir.createTempFile("out", ".tsv")
    vds.colsTable().select("Sample = row.s",
    "row.qc.callRate",
    "row.qc.nCalled",
    "row.qc.nNotCalled",
    "row.qc.nHomRef",
    "row.qc.nHet",
    "row.qc.nHomVar",
    "row.qc.nSNP",
    "row.qc.nInsertion",
    "row.qc.nDeletion",
    "row.qc.nSingleton",
    "row.qc.nTransition",
    "row.qc.nTransversion",
    "row.qc.nStar",
    "row.qc.dpMean",
    "row.qc.dpStDev",
    "row.qc.gqMean",
    "row.qc.gqStDev",
    "row.qc.nNonRef",
    "row.qc.rTiTv",
    "row.qc.rHetHomVar",
    "row.qc.rInsertionDeletion").export(out)

    val sb = new StringBuilder()
    sb.tsvAppend(Array(1, 2, 3, 4, 5))
    assert(sb.result() == "1,2,3,4,5")

    sb.clear()
    sb.tsvAppend(5.124)
    assert(sb.result() == "5.12400e+00")

    val readBackAnnotated = vds.annotateSamplesTable(hc.importTable(out, types = Map(
      "callRate" -> TFloat64(),
      "nCalled" -> TInt64(),
      "nNotCalled" -> TInt64(),
      "nHomRef" -> TInt64(),
      "nHet" -> TInt64(),
      "nHomVar" -> TInt64(),
      "nSNP" -> TInt64(),
      "nInsertion" -> TInt64(),
      "nDeletion" -> TInt64(),
      "nSingleton" -> TInt64(),
      "nTransition" -> TInt64(),
      "nTransversion" -> TInt64(),
      "nStar" -> TInt64(),
      "dpMean" -> TFloat64(),
      "dpStDev" -> TFloat64(),
      "gqMean" -> TFloat64(),
      "gqStDev" -> TFloat64(),
      "nNonRef" -> TInt64(),
      "rTiTv" -> TFloat64(),
      "rHetHomVar" -> TFloat64(),
      "rInsertionDeletion" -> TFloat64())).keyBy("Sample"),
      root = "readBackQC")

    val (t, qcQuerier) = readBackAnnotated.querySA("sa.qc")
    val (t2, rbQuerier) = readBackAnnotated.querySA("sa.readBackQC")
    assert(t == t2)
    readBackAnnotated.colValues.foreach { annotation =>
      t.valuesSimilar(qcQuerier(annotation), rbQuerier(annotation))
    }
  }

  @Test def testExportSamples() {
    val vds = SplitMulti(hc.importVCF("src/test/resources/sample.vcf")
      .filterSamplesExpr("""sa.s == "C469::HG02026""""))
    assert(vds.numCols == 1)

    // verify exports localSamples
    val f = tmpDir.createTempFile("samples", ".tsv")
    vds.colsTable().select("row.s").export(f, header = false)
    assert(sc.textFile(f).count() == 1)
  }

  @Test def testAllowedNames() {
    val f = tmpDir.createTempFile("samples", ".tsv")
    val f2 = tmpDir.createTempFile("samples", ".tsv")
    val f3 = tmpDir.createTempFile("samples", ".tsv")

    val vds = SplitMulti(hc.importVCF("src/test/resources/sample.vcf"))
    vds.colsTable().select("`S.A.M.P.L.E.ID` = row.s").export(f)
    vds.colsTable().select("`$$$I_HEARD_YOU_LIKE!_WEIRD~^_CHARS****` = row.s", "ANOTHERTHING = row.s").export(f2)
    vds.colsTable().select("`I have some spaces and tabs\\there` = row.s", "`more weird stuff here` = row.s").export(f3)
    hadoopConf.readFile(f) { reader =>
      val lines = Source.fromInputStream(reader)
        .getLines()
      assert(lines.next == "S.A.M.P.L.E.ID")
    }
    hadoopConf.readFile(f2) { reader =>
      val lines = Source.fromInputStream(reader)
        .getLines()
      assert(lines.next == "$$$I_HEARD_YOU_LIKE!_WEIRD~^_CHARS****\tANOTHERTHING")
    }
    hadoopConf.readFile(f3) { reader =>
      val lines = Source.fromInputStream(reader)
        .getLines()
      assert(lines.next == "I have some spaces and tabs\there\tmore weird stuff here")
    }
  }

  @Test def testIf() {

    // this should run without errors
    val f = tmpDir.createTempFile("samples", ".tsv")
    var vds = hc.importVCF("src/test/resources/sample.vcf")
    vds = SplitMulti(vds)
    vds = SampleQC(vds)
    vds
      .colsTable()
      .select("computation = 5 * (if (row.qc.callRate < .95) 0 else 1)")
      .export(f)
  }
}
