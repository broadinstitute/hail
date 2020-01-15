package is.hail.expr.types.physical

import is.hail.HailSuite
import is.hail.annotations.{Annotation, Region, ScalaToRegionValue}
import is.hail.asm4s._
import is.hail.expr.ir.EmitFunctionBuilder
import is.hail.utils._
import org.testng.annotations.Test

class PCallSuite extends HailSuite {
  @Test def copyTests() {
    def runTests(forceDeep: Boolean, interpret: Boolean = false) {
      PhysicalTestUtils.copyTestExecutor(PCanonicalCall(), PCanonicalCall(),
        2,
        forceDeep = forceDeep, interpret = interpret)

      PhysicalTestUtils.copyTestExecutor(PCanonicalCall(), PCanonicalCall(true),
        2,
        expectCompileErr = true, forceDeep = forceDeep, interpret = interpret)

      PhysicalTestUtils.copyTestExecutor(PCanonicalCall(), PCanonicalCall(true),
        2,
        allowDowncast = true, forceDeep = forceDeep, interpret = interpret)

      PhysicalTestUtils.copyTestExecutor(PArray(PCanonicalCall()), PArray(PCanonicalCall(true)),
        IndexedSeq(2, null),
        allowDowncast = true, expectRuntimeErr = true, forceDeep = forceDeep, interpret = interpret)
    }

    runTests(true)
    runTests(false)

    runTests(true, interpret = true)
    runTests(false, interpret = true)
  }
}
