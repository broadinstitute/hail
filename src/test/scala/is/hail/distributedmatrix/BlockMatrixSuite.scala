package is.hail.distributedmatrix

import breeze.linalg.{DenseMatrix => BDM}
import breeze.stats.distributions.Rand
import is.hail.SparkSuite
import is.hail.check.Arbitrary._
import is.hail.check.Prop._
import is.hail.check._
import is.hail.distributedmatrix.BlockMatrix.ops._
import is.hail.utils._
import org.apache.commons.math3.random.{RandomDataGenerator, RandomGenerator}
import org.testng.annotations.Test

class BlockMatrixSuite extends SparkSuite {

  // row major
  def toLM(rows: Int, cols: Int, data: Array[Double]): BDM[Double] =
    new BDM(rows, cols, data, 0, cols, isTranspose = true)

  def toBM(rows: Int, cols: Int, data: Array[Double]): BlockMatrix =
    toBM(new BDM(rows, cols, data, 0, rows, true))

  def toBM(rows: Seq[Array[Double]]): BlockMatrix =
    toBM(rows, BlockMatrix.defaultBlockSize)

  def toBM(rows: Seq[Array[Double]], blockSize: Int): BlockMatrix = {
    val n = rows.length
    val m = if (n == 0) 0 else rows(0).length

    BlockMatrix.from(sc, new BDM[Double](m, n, rows.flatten.toArray).t, blockSize)
  }

  def toBM(lm: BDM[Double]): BlockMatrix =
    toBM(lm, BlockMatrix.defaultBlockSize)

  def toBM(lm: BDM[Double], blockSize: Int): BlockMatrix =
    BlockMatrix.from(sc, lm, blockSize)

  def blockMatrixPreGen(blockSize: Int): Gen[BlockMatrix] =
    Gen.coin().flatMap(blockMatrixPreGen(blockSize, _))

  def blockMatrixPreGen(blockSize: Int, transposed: Boolean): Gen[BlockMatrix] = for {
    (l, w) <- Gen.nonEmptySquareOfAreaAtMostSize
    m <- blockMatrixPreGen(l, w, blockSize, transposed)
  } yield m

  def blockMatrixPreGen(rows: Int, columns: Int, blockSize: Int): Gen[BlockMatrix] =
    Gen.coin().flatMap(blockMatrixPreGen(rows, columns, blockSize, _))

  def blockMatrixPreGen(rows: Int, columns: Int, blockSize: Int, transposed: Boolean): Gen[BlockMatrix] = for {
    arrays <- Gen.buildableOfN[Seq, Array[Double]](rows, Gen.buildableOfN(columns, arbDouble.arbitrary))
    m = toBM(arrays, blockSize)
  } yield if (transposed) m.t else m

  val squareBlockMatrixGen = for {
    size <- Gen.size
    l <- Gen.interestingPosInt
    s = math.sqrt(math.min(l, size)).toInt
    blockSize <- Gen.interestingPosInt
    m <- blockMatrixPreGen(s, s, blockSize)
    _ = println(s"$l $s $blockSize")
  } yield m

  val blockMatrixGen = for {
    blockSize <- Gen.interestingPosInt
    m <- blockMatrixPreGen(blockSize)
  } yield m

  val twoMultipliableBlockMatrices = for {
    Array(rows, inner, columns) <- Gen.nonEmptyNCubeOfVolumeAtMostSize(3)
    blockSize <- Gen.interestingPosInt
    transposed <- Gen.coin()
    l <- blockMatrixPreGen(rows, inner, blockSize, transposed)
    r <- blockMatrixPreGen(inner, columns, blockSize, transposed)
  } yield if (transposed) (r, l) else (l, r)

  implicit val arbitraryHailBlockMatrix =
    Arbitrary(blockMatrixGen)

  @Test
  def pointwiseSubtractCorrect() {
    val m = toBM(4, 4, Array[Double](
      1,  2,  3,  4,
      5,  6,  7,  8,
      9,  10, 11, 12,
      13, 14, 15, 16))

    val expected = toLM(4, 4, Array[Double](
      0, -3, -6, -9,
      3, 0,  -3, -6,
      6, 3,  0,  -3,
      9, 6,  3,  0))

    val actual = (m :- m.t).toLocalMatrix()
    assert(actual == expected)
  }

  @Test
  def multiplyByLocalMatrix() {
    val ll = toLM(4, 4, Array[Double](
      1,  2,  3,  4,
      5,  6,  7,  8,
      9,  10, 11, 12,
      13, 14, 15, 16))
    val l = toBM(ll)

    val lr = toLM(4, 1, Array[Double](
      1,
      2,
      3,
      4))

    assert(ll * lr === (l * lr).toLocalMatrix())
  }

  @Test
  def randomMultiplyByLocalMatrix() {
    forAll(Gen.twoMultipliableDenseMatrices[Double]) { case (ll, lr) =>
      val l = toBM(ll)
      assert(ll * lr === (l * lr).toLocalMatrix())
      true
    }.check()
  }

  private def arrayEqualNaNEqualsNaN(x: Array[Double], y: Array[Double], absoluteTolerance: Double = 1e-15): Boolean = {
    if (x.length != y.length) {
      false
    } else {
      var i = 0
      while (i < x.length) {
        if (math.abs(x(i) - y(i)) > absoluteTolerance && !(x(i).isNaN && y(i).isNaN)) {
          println(s"inequality found at $i: ${x(i)} and ${y(i)}")
          return false
        }
        i += 1
      }
      true
    }
  }

  @Test
  def multiplySameAsBreeze() {
    Rand.generator.setSeed(Prop.seed)

    {
      val ll = BDM.rand[Double](4, 4)
      val lr = BDM.rand[Double](4, 4)
      val l = toBM(ll, 2)
      val r = toBM(lr, 2)

      assert(arrayEqualNaNEqualsNaN((l * r).toLocalMatrix().toArray, (ll * lr).toArray))
    }

    {
      val ll = BDM.rand[Double](9, 9)
      val lr = BDM.rand[Double](9, 9)
      val l = toBM(ll, 3)
      val r = toBM(lr, 3)

      assert(arrayEqualNaNEqualsNaN((l * r).toLocalMatrix().toArray, (ll * lr).toArray))
    }

    {
      val ll = BDM.rand[Double](9, 9)
      val lr = BDM.rand[Double](9, 9)
      val l = toBM(ll, 2)
      val r = toBM(lr, 2)

      assert(arrayEqualNaNEqualsNaN((l * r).toLocalMatrix().toArray, (ll * lr).toArray))
    }

    {
      val ll = BDM.rand[Double](2, 10)
      val lr = BDM.rand[Double](10, 2)
      val l = toBM(ll, 3)
      val r = toBM(lr, 3)

      assert(arrayEqualNaNEqualsNaN((l * r).toLocalMatrix().toArray, (ll * lr).toArray))
    }
  }

  @Test
  def multiplySameAsBreezeRandomized() {
    forAll(twoMultipliableBlockMatrices) { case (l: BlockMatrix, r: BlockMatrix) =>
      val actual = (l * r).toLocalMatrix()
      val expected = l.toLocalMatrix() * r.toLocalMatrix()

      if (arrayEqualNaNEqualsNaN(actual.toArray, expected.toArray))
        true
      else {
        println(s"${l.toLocalMatrix()}")
        println(s"${r.toLocalMatrix()}")
        println(s"$actual != $expected")
        false
      }
    }.check()
  }

  @Test
  def rowwiseMultiplication() {
    val l = toBM(4, 4, Array[Double](
      1,  2,  3,  4,
      5,  6,  7,  8,
      9,  10, 11, 12,
      13, 14, 15, 16))

    val v = Array[Double](1,2,3,4)

    val result = toLM(4, 4, Array[Double](
      1,  4,   9, 16,
      5,  12, 21, 32,
      9,  20, 33, 48,
      13, 28, 45, 64))

    assert((l --* v).toLocalMatrix() == result)
  }

  @Test
  def rowwiseMultiplicationRandom() {
    val g = for {
      blockSize <- Gen.interestingPosInt
      l <- blockMatrixPreGen(blockSize)
      v <- Gen.buildableOfN[Array, Double](l.cols.toInt, arbitrary[Double])
    } yield (l, v)

    forAll(g) { case (l: BlockMatrix, v: Array[Double]) =>
      val actual = (l --* v).toLocalMatrix()
      val repeatedR = (0 until l.rows.toInt).flatMap(x => v).toArray
      val repeatedRMatrix = new BDM(v.length, l.rows.toInt, repeatedR).t
      val expected = l.toLocalMatrix() :* repeatedRMatrix

      if (arrayEqualNaNEqualsNaN(actual.toArray, expected.toArray))
        true
      else {
        println(s"$actual != $expected")
        false
      }
    }.check()
  }

  @Test
  def colwiseMultiplication() {
    val l = toBM(4, 4, Array[Double](
      1,  2,  3,  4,
      5,  6,  7,  8,
      9,  10, 11, 12,
      13, 14, 15, 16))

    val v = Array[Double](1,2,3,4)

    val result = toLM(4, 4, Array[Double](
      1,  2,  3,  4,
      10, 12, 14, 16,
      27, 30, 33, 36,
      52, 56, 60, 64))

    assert((l :* v).toLocalMatrix() == result)
  }

  @Test
  def colwiseMultiplicationRandom() {
    val g = for {
      blockSize <- Gen.interestingPosInt
      l <- blockMatrixPreGen(blockSize)
      v <- Gen.buildableOfN[Array, Double](l.rows.toInt, arbitrary[Double])
    } yield (l, v)

    forAll(g) { case (l: BlockMatrix, v: Array[Double]) =>
      val actual = (l :* v).toLocalMatrix()
      val repeatedR = (0 until l.cols.toInt).flatMap(x => v).toArray
      val repeatedRMatrix = new BDM(v.length, l.cols.toInt, repeatedR)
      val expected = l.toLocalMatrix() :* repeatedRMatrix

      if (arrayEqualNaNEqualsNaN(actual.toArray, expected.toArray))
        true
      else {
        println(s"${l.toLocalMatrix().toArray.toSeq}\n*\n${v.toSeq}")
        println(s"${actual.toString(10000,10000)}\n!=\n${expected.toString(10000,10000)}")
        false
      }
    }.check()
  }

  @Test
  def colwiseAddition() {
    val l = toBM(4, 4, Array[Double](
      1,  2,  3,  4,
      5,  6,  7,  8,
      9,  10, 11, 12,
      13, 14, 15, 16))

    val v = Array[Double](1,2,3,4)

    val result = toLM(4, 4, Array[Double](
      2,  3,  4,  5,
      7,  8,  9,  10,
      12, 13, 14, 15,
      17, 18, 19, 20))

    assert((l :+ v).toLocalMatrix() == result)
  }

  @Test
  def rowwiseAddition() {
    val l = toBM(4, 4, Array[Double](
      1,  2,  3,  4,
      5,  6,  7,  8,
      9,  10, 11, 12,
      13, 14, 15, 16))

    val v = Array[Double](1,2,3,4)

    val result = toLM(4, 4, Array[Double](
      2,  4,  6,  8,
      6,  8,  10, 12,
      10, 12, 14, 16,
      14, 16, 18, 20))

    assert((l --+ v).toLocalMatrix() == result)
  }

  @Test
  def diagonalTestTiny() {
    val m = toBM(4, 4, Array[Double](
      1,  2,  3,  4,
      5,  6,  7,  8,
      9,  10, 11, 12,
      13, 14, 15, 16))

    assert(m.diag.toSeq == Seq(1,6,11,16))
  }

  @Test
  def diagonalTestRandomized() {
    forAll(squareBlockMatrixGen) { (m: BlockMatrix) =>
      val lm = m.toLocalMatrix()
      val diagonalLength = math.min(lm.rows, lm.cols)
      val diagonal = Array.tabulate(diagonalLength)(i => lm(i,i))

      if (m.diag.toSeq == diagonal.toSeq)
        true
      else {
        println(s"lm: $lm")
        println(s"${m.diag.toSeq} != ${diagonal.toSeq}")
        false
      }
    }.check()
  }

  @Test
  def fromLocalTest() {
    Rand.generator.setSeed(Prop.seed)

    val rows = 100
    val cols = 100
    val local = BDM.rand[Double](rows, cols)
    assert(local === BlockMatrix.from(sc, local, rows - 1).toLocalMatrix())
  }

  @Test
  def readWriteIdentityTrivial() {
    val m = toBM(4, 4, Array[Double](
      1,  2,  3,  4,
      5,  6,  7,  8,
      9,  10, 11, 12,
      13, 14, 15, 16))

    val fname = tmpDir.createTempFile("test")
    BlockMatrix.write(m, fname)
    assert(m.toLocalMatrix() == BlockMatrix.read(hc, fname).toLocalMatrix())
  }

  @Test
  def readWriteIdentityRandom() {
    forAll(blockMatrixGen) { (m: BlockMatrix) =>
      val fname = tmpDir.createTempFile("test")
      BlockMatrix.write(m, fname)
      assert(m.toLocalMatrix() == BlockMatrix.read(hc, fname).toLocalMatrix())
      true
    }.check()
  }

  @Test
  def transpose() {
    forAll(blockMatrixGen) { (m: BlockMatrix) =>
      val transposed = m.toLocalMatrix().t
      assert(transposed.rows == m.cols)
      assert(transposed.cols == m.rows)
      assert(transposed === m.t.toLocalMatrix())
      true
    }.check()
  }

  @Test
  def doubleTransposeIsIdentity() {
    forAll(blockMatrixGen) { (m: BlockMatrix) =>
      val mt = m.t.cache()
      val mtt = m.t.t.cache()
      assert(mtt.rows == m.rows)
      assert(mtt.cols == m.cols)
      assert(arrayEqualNaNEqualsNaN(mtt.toLocalMatrix().toArray, m.toLocalMatrix().toArray))
      assert(arrayEqualNaNEqualsNaN((mt * mtt).toLocalMatrix().toArray, (mt * m).toLocalMatrix().toArray))
      true
    }.check()
  }

  @Test
  def cachedOpsOK() {
    forAll(twoMultipliableBlockMatrices) { case (l: BlockMatrix, r: BlockMatrix) =>
      l.cache()
      r.cache()

      val actual = (l * r).toLocalMatrix()
      val expected = l.toLocalMatrix() * r.toLocalMatrix()

      if (!arrayEqualNaNEqualsNaN(actual.toArray, expected.toArray)) {
        println(s"${l.toLocalMatrix()}")
        println(s"${r.toLocalMatrix()}")
        println(s"$actual != $expected")
        assert(false)
      }

      if (!arrayEqualNaNEqualsNaN(l.t.cache().t.toLocalMatrix().toArray, l.toLocalMatrix.toArray)) {
        println(s"${l.t.cache().t.toLocalMatrix()}")
        println(s"${l.toLocalMatrix()}")
        println(s"$actual != $expected")
        assert(false)
      }

      true
    }.check()
  }

  @Test
  def toIRMToHBMIdentity() {
    forAll(blockMatrixGen) { (m: BlockMatrix) =>
      val roundtrip = m.toIndexedRowMatrix().toHailBlockMatrix(m.blockSize)

      val roundtriplm = roundtrip.toLocalMatrix()
      val lm = m.toLocalMatrix()

      if (roundtriplm != lm) {
        println(roundtriplm)
        println(lm)
        assert(false)
      }

      true
    }.check()
  }

  @Test
  def map2RespectsTransposition() {
    val lm = toLM(4, 2, Array[Double](
      1,5,
      2,6,
      3,7,
      4,8))
    val lmt = toLM(2, 4, Array[Double](
      1,2,3,4,
      5,6,7,8)).t

    val m = toBM(lm)
    val mt = toBM(lmt)

    assert(m.map2(mt.t, _ + _).toLocalMatrix() === lm + lm)
    assert(mt.t.map2(m, _ + _).toLocalMatrix() === lm + lm, s"${mt.toLocalMatrix()}\n${mt.t.toLocalMatrix()}\n${m.toLocalMatrix()}")
  }

  @Test
  def map4RespectsTransposition() {
    val lm = toLM(4, 2, Array[Double](
      1,5,
      2,6,
      3,7,
      4,8))
    val lmt = toLM(2, 4, Array[Double](
      1,2,3,4,
      5,6,7,8)).t

    val m = toBM(lm)
    val mt = toBM(lmt)

    assert(m.map4(m, mt.t, mt.t.t.t, _ + _ + _ + _).toLocalMatrix() === lm + lm + lm + lm)
    assert(mt.map4(mt, m.t, mt.t.t, _ + _ + _ + _).toLocalMatrix() === lm.t + lm.t + lm.t + lm.t)
  }

  @Test
  def mapRespectsTransposition() {
    val lm = toLM(4, 2, Array[Double](
      1,5,
      2,6,
      3,7,
      4,8))
    val lmt = toLM(2, 4, Array[Double](
      1,2,3,4,
      5,6,7,8)).t

    val m = toBM(lm)
    val mt = toBM(lmt)

    assert(m.t.map(_ * 4).toLocalMatrix() === lm.t.map(_ * 4))
    assert(m.t.t.map(_ * 4).toLocalMatrix() === lm.map(_ * 4))
    assert(mt.t.map(_ * 4).toLocalMatrix() === lm.map(_ * 4))
  }

  @Test
  def mapWithIndexRespectsTransposition() {
    val lm = toLM(2, 4, Array[Double](
      1,5,
      2,6,
      3,7,
      4,8))
    val lmt = toLM(4, 2, Array[Double](
      1,2,3,4,
      5,6,7,8)).t

    val m = toBM(lm)
    val mt = toBM(lmt)

    assert(m.t.mapWithIndex((_,_,x) => x * 4).toLocalMatrix() === lm.t.map(_ * 4))
    assert(m.t.t.mapWithIndex((_,_,x) => x * 4).toLocalMatrix() === lm.map(_ * 4))
    assert(mt.t.mapWithIndex((_,_,x) => x * 4).toLocalMatrix() === lm.map(_ * 4))
  }

  @Test
  def map2WithIndexRespectsTransposition() {
    val lm = toLM(2, 4, Array[Double](
      1,5,
      2,6,
      3,7,
      4,8))
    val lmt = toLM(4, 2, Array[Double](
      1,2,3,4,
      5,6,7,8)).t

    val m = toBM(lm)
    val mt = toBM(lmt)

    assert(m.map2WithIndex(mt.t, (_,_,x,y) => x + y).toLocalMatrix() === lm + lm)
    assert(mt.map2WithIndex(m.t, (_,_,x,y) => x + y).toLocalMatrix() === lm.t + lm.t)
    assert(mt.t.map2WithIndex(m, (_,_,x,y) => x + y).toLocalMatrix() === lm + lm)
    assert(m.t.t.map2WithIndex(mt.t, (_,_,x,y) => x + y).toLocalMatrix() === lm + lm)
  }

}
