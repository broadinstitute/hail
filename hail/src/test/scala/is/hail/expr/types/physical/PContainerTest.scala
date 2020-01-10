package is.hail.expr.types.physical

import is.hail.HailSuite
import is.hail.annotations.{Region, SafeIndexedSeq, ScalaToRegionValue, UnsafeUtils}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitFunctionBuilder}
import is.hail.utils._
import org.testng.annotations.Test

class PContainerTest extends HailSuite {

  def nullInByte(nElements: Int, missingElement: Int) = {
    IndexedSeq.tabulate(nElements)(i => {
      if (i == missingElement - 1)
        null
      else
        i + 1L
    })
  }

  def testConvert(sourceType: PArray, destType: PArray, data: IndexedSeq[Any], expectFalse: Boolean) {
    val srcRegion = Region()
    val src = ScalaToRegionValue(srcRegion, sourceType, data)

    log.info(s"Testing $data")

    val fb = EmitFunctionBuilder[Region, Long, Long]("not_empty")
    val codeRegion = fb.getArg[Region](1).load()
    val value = fb.getArg[Long](2)

    fb.emit(destType.checkedConvertFrom(fb.apply_method, codeRegion, value, sourceType, "ShouldHaveNoNull"))

    val f = fb.result()()
    val destRegion = Region()
    if (expectFalse) {
      val thrown = intercept[Exception](f(destRegion,src))
      assert(thrown.getMessage == "ShouldHaveNoNull")
    } else
      f(destRegion,src)
  }

  def testContainsNonZeroBits(sourceType: PArray, data: IndexedSeq[Any]) = {
    val srcRegion = Region()
    val src = ScalaToRegionValue(srcRegion, sourceType, data)

    log.info(s"Testing $data")

    val res = Region.containsNonZeroBits(src + sourceType.lengthHeaderBytes, sourceType.loadLength(src))
    res
  }

  def testContainsNonZeroBitsStaged(sourceType: PArray, data: IndexedSeq[Any]) = {
    val srcRegion = Region()
    val src = ScalaToRegionValue(srcRegion, sourceType, data)

    log.info(s"Testing $data")

    val fb = EmitFunctionBuilder[Long, Boolean]("not_empty")
    val value = fb.getArg[Long](1)

    fb.emit(Region.containsNonZeroBits(value + sourceType.lengthHeaderBytes, sourceType.loadLength(value).toL))

    val res = fb.result()()(src)
    res
  }

  def testHasMissingValues(sourceType: PArray, data: IndexedSeq[Any]) = {
    val srcRegion = Region()
    val src = ScalaToRegionValue(srcRegion, sourceType, data)

    log.info(s"\nTesting $data")

    val fb = EmitFunctionBuilder[Long, Boolean]("not_empty")
    val value = fb.getArg[Long](1)

    fb.emit(sourceType.hasMissingValues(value))

    val res = fb.result()()(src)
    res
  }

  @Test def checkFirstNonZeroByte() {
    val sourceType = PArray(PInt64(false))

    assert(testContainsNonZeroBits(sourceType, nullInByte(0, 0)) == false)

    assert(testContainsNonZeroBits(sourceType, nullInByte(1, 0)) == false)
    assert(testContainsNonZeroBits(sourceType, nullInByte(1, 1)) == true)

    assert(testContainsNonZeroBits(sourceType, nullInByte(8, 0)) == false)
    assert(testContainsNonZeroBits(sourceType, nullInByte(8, 1)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(8, 8)) == true)

    assert(testContainsNonZeroBits(sourceType, nullInByte(32, 0)) == false)
    assert(testContainsNonZeroBits(sourceType, nullInByte(31, 31)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(32, 32)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(33, 33)) == true)

    assert(testContainsNonZeroBits(sourceType, nullInByte(64, 0)) == false)
    assert(testContainsNonZeroBits(sourceType, nullInByte(64, 1)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(64, 32)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(64, 33)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(64, 64)) == true)

    assert(testContainsNonZeroBits(sourceType, nullInByte(68, 0)) == false)
    assert(testContainsNonZeroBits(sourceType, nullInByte(68, 1)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(68, 32)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(68, 33)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(68, 64)) == true)

    assert(testContainsNonZeroBits(sourceType, nullInByte(72, 0)) == false)
    assert(testContainsNonZeroBits(sourceType, nullInByte(72, 1)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(72, 32)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(72, 33)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(72, 64)) == true)

    assert(testContainsNonZeroBits(sourceType, nullInByte(73, 0)) == false)
    assert(testContainsNonZeroBits(sourceType, nullInByte(73, 1)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(73, 32)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(73, 33)) == true)
    assert(testContainsNonZeroBits(sourceType, nullInByte(73, 64)) == true)
  }

  @Test def checkFirstNonZeroByteStaged() {
    val sourceType = PArray(PInt64(false))

    assert(testContainsNonZeroBitsStaged(sourceType, nullInByte(32, 0)) == false)
    assert(testContainsNonZeroBitsStaged(sourceType, nullInByte(73, 64)) == true)
  }

  @Test def checkHasMissingValues() {
    val sourceType = PArray(PInt64(false))

    assert(testHasMissingValues(sourceType, nullInByte(1, 0)) == false)
    assert(testHasMissingValues(sourceType, nullInByte(1, 1)) == true)
  }

  @Test def checkedConvertFromTest() {
    val sourceType = PArray(PInt64(false))
    val destType = PArray(PInt64(true))

    testConvert(sourceType, destType, nullInByte(0, 0), false)

    // 1 byte
    testConvert(sourceType, destType, nullInByte(1, 0), false)
    testConvert(sourceType, destType, nullInByte(1, 1), true)
    testConvert(sourceType, destType, nullInByte(5, 5), true)

    // 1 full byte
    testConvert(sourceType, destType, nullInByte(8, 0), false)
    testConvert(sourceType, destType, nullInByte(8, 1), true)
    testConvert(sourceType, destType, nullInByte(8, 5), true)
    testConvert(sourceType, destType, nullInByte(8, 8), true)

    // 1 byte + remainder
    testConvert(sourceType, destType, nullInByte(11, 0), false)
    testConvert(sourceType, destType, nullInByte(13, 13), true)
    testConvert(sourceType, destType, nullInByte(13, 9), true)
    testConvert(sourceType, destType, nullInByte(13, 8), true)

    // 1 Long
    testConvert(sourceType, destType, nullInByte(64, 0), false)
    testConvert(sourceType, destType, nullInByte(64, 1), true)
    testConvert(sourceType, destType, nullInByte(64, 64), true)

    // 1 Long + remainder
    testConvert(sourceType, destType, nullInByte(67, 0), false)
    testConvert(sourceType, destType, nullInByte(67, 67), true)
    testConvert(sourceType, destType, nullInByte(67, 65), true)
    testConvert(sourceType, destType, nullInByte(67, 64), true)

    // 1 Long + 1 byte + remainder
    testConvert(sourceType, destType, nullInByte(79, 0), false)
    testConvert(sourceType, destType, nullInByte(79, 72), true)
    testConvert(sourceType, destType, nullInByte(79, 8), true)
  }

  @Test def arrayCopyTest() {
    // Note: can't test where data is null due to ArrayStack.top semantics (ScalaToRegionValue: assert(size_ > 0))

    def runTests(forceDeep: Boolean) {
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32()), PArray(PInt64()), IndexedSeq(1, 2, 3, 4, 5, 6, 7, 8, 9),
        expectCompileErr = true, forceDeep = forceDeep)

      PhysicalTestUtils.copyTestExecutor(PArray(PInt32()), PArray(PInt32()), IndexedSeq(1, 2, 3, 4),
        forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32()), PArray(PInt32()), IndexedSeq(1, 2, 3, 4),
        forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32()), PArray(PInt32()), IndexedSeq(1, null, 3, 4),
        forceDeep = forceDeep)

      // test upcast
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32(true)), PArray(PInt32()), IndexedSeq(1, 2, 3, 4),
        forceDeep = forceDeep)

      // test mismatched top-level requiredeness
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32()), PArray(PInt32(), true), IndexedSeq(1, 2, 3, 4),
        forceDeep = forceDeep)

      // test downcast
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32()), PArray(PInt32(true)), IndexedSeq(1, 2, 3, 4),
        expectCompileErr = true, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32()), PArray(PInt32(true)), IndexedSeq(1, 2, 3, 4),
        allowDowncast = true, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32()), PArray(PInt32(true)), IndexedSeq(1, null, 3, 4),
        expectRuntimeErr = true, allowDowncast = true, forceDeep = forceDeep)

      // test empty arrays
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32()), PArray(PInt32()), FastIndexedSeq(),
        forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PInt32(true)), PArray(PInt32(true)), FastIndexedSeq(),
        forceDeep = forceDeep)

      // test missing-only array
      PhysicalTestUtils.copyTestExecutor(PArray(PInt64()), PArray(PInt64()),
        FastIndexedSeq(null), forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PInt64())), PArray(PArray(PInt64())),
        FastIndexedSeq(FastIndexedSeq(null)), forceDeep = forceDeep)

      // test 2D arrays
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PInt64())), PArray(PArray(PInt64())),
        FastIndexedSeq(null, FastIndexedSeq(null), FastIndexedSeq(20L,5L,31L,41L), FastIndexedSeq(1L,2L,3L)),
        forceDeep = forceDeep)

      // test 2D array with missingness
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PInt64())), PArray(PArray(PInt64(), true)),
        FastIndexedSeq(FastIndexedSeq(20L), FastIndexedSeq(1L), FastIndexedSeq(20L,5L,31L,41L), FastIndexedSeq(1L,2L,3L)),
        allowDowncast = true, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PInt64())), PArray(PArray(PInt64(), true)),
        FastIndexedSeq(null, FastIndexedSeq(1L), FastIndexedSeq(20L,5L,31L,41L), FastIndexedSeq(1L,2L,3L)),
        allowDowncast = true, expectRuntimeErr = true, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PInt64())), PArray(PArray(PInt64(true))),
        FastIndexedSeq(FastIndexedSeq(99L), FastIndexedSeq(20L,5L,31L,41L), FastIndexedSeq(1L,2L,3L)),
        allowDowncast = true, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PInt64())), PArray(PArray(PInt64(true))),
        FastIndexedSeq(FastIndexedSeq(99L), FastIndexedSeq(20L,3L,31L,41L), FastIndexedSeq(1L,2L, null)),
        allowDowncast = true, expectRuntimeErr = true, forceDeep = forceDeep)

      // test complex nesting
      val complexNesting = FastIndexedSeq(
        FastIndexedSeq( FastIndexedSeq(20L,30L,31L,41L), FastIndexedSeq(20L,22L,31L,43L) ),
        FastIndexedSeq( FastIndexedSeq(1L,3L,31L,41L), FastIndexedSeq(0L,30L,17L,41L) )
      )

      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PArray(PInt64(true), true), true), true), PArray(PArray(PArray(PInt64()))),
        complexNesting, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PArray(PInt64(true), true), true)), PArray(PArray(PArray(PInt64()))),
        complexNesting, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PArray(PInt64(true), true))), PArray(PArray(PArray(PInt64()))),
        complexNesting, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PArray(PInt64(true)))), PArray(PArray(PArray(PInt64()))),
        complexNesting, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PArray(PInt64()))), PArray(PArray(PArray(PInt64()))),
        complexNesting, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PArray(PInt64()))), PArray(PArray(PArray(PInt64(true)))),
        complexNesting, allowDowncast = true, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PArray(PInt64()))), PArray(PArray(PArray(PInt64(true), true))),
        complexNesting, allowDowncast = true, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PArray(PInt64()))), PArray(PArray(PArray(PInt64(true), true), true)),
        complexNesting, allowDowncast = true, forceDeep = forceDeep)
      PhysicalTestUtils.copyTestExecutor(PArray(PArray(PArray(PInt64()))), PArray(PArray(PArray(PInt64(true), true), true), true),
        complexNesting, allowDowncast = true, forceDeep = forceDeep)
    }

    runTests(true)
    runTests(false)
  }
}
