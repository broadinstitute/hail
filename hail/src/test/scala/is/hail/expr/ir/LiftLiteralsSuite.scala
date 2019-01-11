package is.hail.expr.ir

import is.hail.SparkSuite
import is.hail.expr.types.virtual.TInt64
import is.hail.utils.FastSeq
import is.hail.TestUtils._
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class LiftLiteralsSuite extends SparkSuite {
  @Test def testNestedGlobalsRewrite() {
    val tab = TableLiteral(TableRange(10, 1).execute(hc))
    val ir = TableGetGlobals(
      TableMapGlobals(
        tab,
        Let(
          "global",
          I64(1),
          MakeStruct(
            FastSeq(
              "x" -> ApplyBinaryPrimOp(
                Add(),
                TableCount(tab),
                Ref("global", TInt64())))))))

    assertEvalsTo(ir, Row(11L))
  }
}
