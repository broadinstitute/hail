package is.hail.expr.ir

import is.hail.HailSuite
import is.hail.annotations.{Region, SafeRow}
import is.hail.asm4s._
import is.hail.expr.ir.agg.TakeByRVAS
import is.hail.expr.types.physical._
import is.hail.utils._
import org.testng.annotations.Test

class TakeByAggregatorSuite extends HailSuite {
  @Test def testPointers() {
    for ((size, n) <- Array((1000, 100), (1, 10), (100, 10000), (1000, 10000))) {
      val fb = EmitFunctionBuilder[Region, Long](ctx, "test_pointers")
      val cb = fb.ecb
      val stringPT = PCanonicalString(true)
      val tba = new TakeByRVAS(PCanonicalString(true), PInt64Optional, PCanonicalArray(stringPT, required = true), cb)
      Region.scoped { r =>
        val argR = fb.getCodeParam[Region](1)
        val i = fb.genFieldThisRef[Long]()
        val off = fb.genFieldThisRef[Long]()
        val rt = tba.resultType

        fb.emitWithBuilder { cb =>
          tba.createState(cb)
          cb += tba.newState(0L)
          cb += tba.initialize(size)
          cb += (i := 0L)
          cb += Code.whileLoop(i < n.toLong,
              argR.invoke[Unit]("clear"),
              off := stringPT.allocateAndStoreString(fb.apply_method, argR, const("str").concat(i.toS)),
              tba.seqOp(false, off, false, -i),
              i := i + 1L)
          tba.result(argR, rt)
        }

        val o = fb.resultWithIndex()(0, r)(r)
        val result = SafeRow.read(rt, o)
        assert(result == ((n - 1) to 0 by -1)
          .iterator
          .map(i => s"str$i")
          .take(size)
          .toFastIndexedSeq, s"size=$size, n=$n")
      }
    }
  }

  @Test def testMissing() {
    val fb = EmitFunctionBuilder[Region, Long](ctx, "take_by_test_missing")
    val cb = fb.ecb
    val tba = new TakeByRVAS(PInt32Optional, PInt32Optional, PCanonicalArray(PInt32Optional, required = true), cb)
    Region.scoped { r =>
      val argR = fb.getCodeParam[Region](1)
      val rt = tba.resultType

      fb.emitWithBuilder { cb =>
        tba.createState(cb)
        cb += tba.newState(0L)
        cb += tba.initialize(7)
        cb += tba.seqOp(true, 0, true, 0)
        cb += tba.seqOp(true, 0, true, 0)
        cb += tba.seqOp(false, 0, false, 0)
        cb += tba.seqOp(false, 1, false, 1)
        cb += tba.seqOp(false, 2, false, 2)
        cb += tba.seqOp(false, 3, false, 3)
        cb += tba.seqOp(true, 0, true, 0)
        cb += tba.seqOp(true, 0, true, 0)
        tba.result(argR, rt)
      }

      val o = fb.resultWithIndex()(0, r)(r)
      val result = SafeRow.read(rt, o)
      assert(result == FastIndexedSeq(0, 1, 2, 3, null, null, null))
    }
  }

  @Test def testRandom() {
    for (n <- Array(1, 2, 10, 100, 1000, 10000, 100000, 1000000)) {
      val nToTake = 1025
      val fb = EmitFunctionBuilder[Region, Long](ctx, "take_by_test_random")
      val cb = fb.ecb

      Region.scoped { r =>
        val argR = fb.getCodeParam[Region](1)
        val i = fb.genFieldThisRef[Int]()
        val random = fb.genFieldThisRef[Int]()
        val resultOff = fb.genFieldThisRef[Long]()

        val tba = new TakeByRVAS(PInt32Required, PInt32Required, PCanonicalArray(PInt32Required, required = true), cb)
        val ab = new agg.StagedArrayBuilder(PInt32Required, cb, argR)
        val rt = tba.resultType
        val er = new EmitRegion(fb.apply_method, argR)
        val rng = er.mb.newRNG(0)

        fb.emitWithBuilder { cb =>
          tba.createState(cb)
          cb += tba.newState(0L)
          cb += tba.initialize(nToTake)
          cb += ab.initialize()
          cb += (i := 0)
          cb += Code.whileLoop(i < n,
            random := rng.invoke[Double, Double, Double]("runif", -10000d, 10000d).toI,
            tba.seqOp(false, random, false, random),
            ab.append(random),
            i := i + 1
          )
          cb += ab.size.cne(n).orEmpty(Code._fatal[Unit]("bad size!"))
          cb += (resultOff := argR.allocate(8L, 16L))
          cb += Region.storeAddress(resultOff, tba.result(argR, rt))
          cb += Region.storeAddress(resultOff + 8L, ab.data)
          resultOff
        }

        val o = fb.resultWithIndex()(0, r)(r)
        val pqOffset = Region.loadAddress(o)
        val pq = SafeRow.read(rt, pqOffset)
        val collOffset = Region.loadAddress(o + 8)
        val collected = SafeRow.read(ab.eltArray, collOffset).asInstanceOf[IndexedSeq[Int]].take(n)
        val minValues = collected.sorted.take(nToTake)
        assert(pq == minValues, s"n=$n")
      }
    }
  }
}
