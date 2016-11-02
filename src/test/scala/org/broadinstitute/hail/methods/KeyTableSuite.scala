package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.driver._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.keytable.KeyTable
import org.broadinstitute.hail.utils._
import org.testng.annotations.Test

class KeyTableSuite extends SparkSuite {

  @Test def testImportExport() = {
    val inputFile = "src/test/resources/sampleAnnotations.tsv"
    val outputFile = tmpDir.createTempFile("ktImpExp", "tsv")
    var s = State(sc, sqlContext)
    s = ImportKeyTable.run(s, Array("-n", "kt1", "-k", "Sample, Status", inputFile))
    s = ExportKeyTable.run(s, Array("-n", "kt1", "-o", outputFile))

    val importedData = sc.hadoopConfiguration.readLines(inputFile)(_.map(_.value).toIndexedSeq)
    val exportedData = sc.hadoopConfiguration.readLines(outputFile)(_.map(_.value).toIndexedSeq)

    intercept[FatalException] {
      s = ImportKeyTable.run(s, Array("-n", "kt1", "-k", "Sample, Status, BadKeyName", inputFile))
    }

    assert(importedData == exportedData)
  }

  @Test def testFilter() = {
    val data = Array(Array(5, 9, 0), Array(2, 3, 4), Array(1, 2, 3))
    val rdd = sc.parallelize(data.map(Annotation.fromSeq(_)))
    val signature = TStruct(("field1", TInt), ("field2", TInt), ("field3", TInt))
    val keyNames = Array("field1")
    val kt = KeyTable(rdd, signature, keyNames)

    var s = State(sc, sqlContext, ktEnv = Map("kt1" -> kt))

    s = FilterKeyTableExpr.run(s, Array("-n", "kt1", "-c", "field1 < 3", "-d", "kt2", "--keep"))
    assert(s.ktEnv.contains("kt2") && s.ktEnv("kt2").nRows == 2)

    s = FilterKeyTableExpr.run(s, Array("-n", "kt1", "-c", "field1 < 3 && field3 == 4", "-d", "kt3", "--keep"))
    assert(s.ktEnv.contains("kt3") && s.ktEnv("kt3").nRows == 1)

    s = FilterKeyTableExpr.run(s, Array("-n", "kt1", "-c", "field1 == 5 && field2 == 9 && field3 == 0", "-d", "kt3", "--remove"))
    assert(s.ktEnv.contains("kt3") && s.ktEnv("kt3").nRows == 2)

    s = FilterKeyTableExpr.run(s, Array("-n", "kt1", "-c", "field1 < -5 && field3 == 100", "--keep"))
    assert(s.ktEnv.contains("kt1") && s.ktEnv("kt1").nRows == 0)

  }
}
