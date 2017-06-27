package is.hail.distributedmatrix

import is.hail.check.Arbitrary._
import is.hail.check.Prop._
import is.hail.check.Gen._
import is.hail.check._
import is.hail.utils._
import is.hail.SparkSuite

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, _}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.testng.annotations.Test

import scala.util.Random

class BlockMatrixIsDistributedMatrixSuite extends SparkSuite {
  import is.hail.distributedmatrix.DistributedMatrix.implicits._

  val dm = DistributedMatrix[BlockMatrix]
  import dm.ops._

  def toBM(rows: Seq[Array[Double]]): BlockMatrix =
    new IndexedRowMatrix(sc.parallelize(rows.zipWithIndex.map { case (v, i) => IndexedRow(i, new DenseVector(v)) }),
      rows.size, if (rows.isEmpty) 0 else rows.head.length)
      .toBlockMatrixDense()

  def toBM(rows: Seq[Array[Double]], blockSize: Int): BlockMatrix =
    new IndexedRowMatrix(sc.parallelize(rows.zipWithIndex.map { case (v, i) => IndexedRow(i, new DenseVector(v)) }),
      rows.size, if (rows.isEmpty) 0 else rows.head.length)
      .toBlockMatrixDense(blockSize, blockSize)

  def toBM(rows: Seq[Array[Double]], rowsPerBlock: Int, colsPerBlock: Int): BlockMatrix =
    new IndexedRowMatrix(sc.parallelize(rows.zipWithIndex.map { case (v, i) => IndexedRow(i, new DenseVector(v)) }),
      rows.size, if (rows.isEmpty) 0 else rows.head.length)
      .toBlockMatrixDense(rowsPerBlock, colsPerBlock)

  def toBreeze(bm: BlockMatrix): BDM[Double] = {
    val lm = dm.toLocalMatrix(bm)
    new BDM(lm.numRows, lm.numCols, lm.toArray)
  }

  def toBreeze(a: Array[Double]): BDV[Double] =
    new BDV(a)

  def blockMatrixPreGen(rowsPerBlock: Int, colsPerBlock: Int): Gen[BlockMatrix] = for {
    (l, w) <- Gen.nonEmptySquareOfAreaAtMostSize
    arrays <- Gen.buildableOfN[Seq, Array[Double]](l, Gen.buildableOfN(w, arbDouble.arbitrary))
  } yield toBM(arrays, rowsPerBlock, colsPerBlock)

  val blockMatrixGen = for {
    rowsPerBlock <- Gen.interestingPosInt
    colsPerBlock <- Gen.interestingPosInt
    bm <- blockMatrixPreGen(rowsPerBlock, colsPerBlock)
  } yield bm

  val blockMatrixSquareBlocksGen = for {
    blockSize <- Gen.interestingPosInt
    bm <- blockMatrixPreGen(blockSize, blockSize)
  } yield bm

  val twoSquareBlockMatricesSameBlockSizeGen = for {
    blockSize <- Gen.interestingPosInt
    x <- blockMatrixPreGen(blockSize, blockSize)
    y <- blockMatrixPreGen(blockSize, blockSize)
  } yield (x, y)

  implicit val arbitraryBlockMatrix =
    Arbitrary(blockMatrixGen)

  @Test
  def pointwiseSubtractCorrect() {
    val m = toBM(Seq(
      Array[Double](1,2,3,4),
      Array[Double](5,6,7,8),
      Array[Double](9,10,11,12),
      Array[Double](13,14,15,16)))

    val expected = Array[IndexedSeq[Double]](
      Array[Double](0,-3,-6,-9),
      Array[Double](3,0,-3,-6),
      Array[Double](6,3,0,-3),
      Array[Double](9,6,3,0)):IndexedSeq[IndexedSeq[Double]]

    val actual = (m :- (m.t)).toLocalMatrix().rowIter.map(x => x.toArray: IndexedSeq[Double]).toArray[IndexedSeq[Double]]: IndexedSeq[IndexedSeq[Double]]
    assert(actual == expected)
  }

  @Test
  def multiplyByLocalMatrix() {
    val l = toBM(Seq(
      Array[Double](1,2,3,4),
      Array[Double](5,6,7,8),
      Array[Double](9,10,11,12),
      Array[Double](13,14,15,16)))

    val r = new DenseMatrix(4, 1, Array[Double](1,2,3,4))

    assert((l.toIndexedRowMatrix().multiply(r).toBlockMatrix().toLocalMatrix().toArray: IndexedSeq[Double]) == ((l * r).toLocalMatrix().toArray: IndexedSeq[Double]))
  }

  @Test
  def multiplyByLocalMatrix2() {
    val l = toBM(Seq(
      Array[Double](-0.0, -0.0, 0.0),
      Array[Double](0.24999999999999994, 0.5000000000000001, -0.5),
      Array[Double](0.4999999999999998, 2.220446049250313E-16, 2.220446049250313E-16),
      Array[Double](0.75, 0.5, -0.5),
      Array[Double](0.25, -0.5, 0.5),
      Array[Double](0.5000000000000001, 1.232595164407831E-32, -2.220446049250313E-16),
      Array[Double](0.75, -0.5000000000000001, 0.5),
      Array[Double](1.0, -0.0, 0.0)))

    val r = new DenseMatrix(3, 4, Array[Double](1.0,0.0,1.0,
      1.0,1.0,1.0,
      1.0,1.0,0.0,
      1.0,0.0,0.0))

    assert((l.toIndexedRowMatrix().multiply(r).toBlockMatrix().toLocalMatrix().toArray: IndexedSeq[Double]) == ((l * r).toLocalMatrix().toArray: IndexedSeq[Double]))
  }

  @Test
  def multiplySameAsSpark() {
    forAll(twoSquareBlockMatricesSameBlockSizeGen) { case (a: BlockMatrix, b: BlockMatrix) =>
      val truth = dm.toLocalMatrix(a * b)
      val expected = dm.toLocalMatrix(a.multiply(b))

      if (truth == expected)
        true
      else {
        println(s"$truth != $expected")
        false
      }
    }.check()
  }

  @Test
  def rowwiseMultiplication() {
    // row major
    val l = toBM(Seq(
      Array[Double](1,2,3,4),
      Array[Double](5,6,7,8),
      Array[Double](9,10,11,12),
      Array[Double](13,14,15,16)))

    val r = Array[Double](1,2,3,4)

    // col major
    val result = new DenseMatrix(4,4, Array[Double](
      1,5,9,13,
      4,12,20,28,
      9,21,33,45,
      16,32,48,64
    ))

    assert(dm.toLocalMatrix(l --* r) == result)
  }

  @Test
  def rowwiseMultiplicationRandom() {
    val g = for {
      blockSize <- Gen.interestingPosInt
      l <- blockMatrixPreGen(blockSize, blockSize)
      r <- Gen.buildableOfN[Array, Double](l.numCols().toInt, arbitrary[Double])
    } yield (l, r)

    forAll(g.resize(100)) { case (l: BlockMatrix, r: Array[Double]) =>
      val truth = toBreeze(l --* r)
      val repeatedR = (0 until l.numRows().toInt).map(x => r).flatten.toArray
      val repeatedRMatrix = new BDM(r.size, l.numRows().toInt, repeatedR).t
      val expected = toBreeze(l) :* repeatedRMatrix

      if (truth == expected)
        true
      else {
        println(s"${truth.toString(1000,1000)} did not equal ${expected.toString(1000,1000)}")
        val firstMismatch = truth.toArray.zip(expected.toArray).filter { case (x, y) => x != y }.head
        println(s"first differing elements: $firstMismatch")
        false
      }
    }.check()
  }

  @Test
  def colwiseMultiplication() {
    // row major
    val l = toBM(Seq(
      Array[Double](1,2,3,4),
      Array[Double](5,6,7,8),
      Array[Double](9,10,11,12),
      Array[Double](13,14,15,16)
    ))

    val r = Array[Double](1,2,3,4)

    // col major
    val result = new DenseMatrix(4,4, Array[Double](
      1,10,27,52,
      2,12,30,56,
      3,14,33,60,
      4,16,36,64
    ))

    assert(dm.toLocalMatrix(l :* r) == result)
  }

  @Test
  def colwiseAddition() {
    // row major
    val l = toBM(Seq(
      Array[Double](1,2,3,4),
      Array[Double](5,6,7,8),
      Array[Double](9,10,11,12),
      Array[Double](13,14,15,16)
    ))

    val r = Array[Double](1,2,3,4)

    // col major
    val result = new DenseMatrix(4,4, Array[Double](
      2, 7,12,17,
      3, 8,13,18,
      4, 9,14,19,
      5,10,15,20
    ))

    assert(dm.toLocalMatrix(l :+ r) == result)
  }

  @Test
  def colwiseMultiplication() {
    val l = toBM(Seq(
      Array[Double](1,2,3,4),
      Array[Double](5,6,7,8),
      Array[Double](9,10,11,12),
      Array[Double](13,14,15,16)
    ))

    val r = Array[Double](1,2,3,4)

    // col major
    val result = new DenseMatrix(4,4, Array[Double](
      1,10,27,52,
      2,12,30,56,
      3,14,33,60,
      4,16,36,64
    ))

    assert(dm.toLocalMatrix(l :* r) == result)
  }

  @Test
  def colwiseAddition() {
    // row major
    val l = toBM(Seq(
      Array[Double](1,2,3,4),
      Array[Double](5,6,7,8),
      Array[Double](9,10,11,12),
      Array[Double](13,14,15,16)
    ))

    val r = Array[Double](1,2,3,4)

    // col major
    val result = new DenseMatrix(4,4, Array[Double](
      2, 7,12,17,
      3, 8,13,18,
      4, 9,14,19,
      5,10,15,20
    ))

    assert(dm.toLocalMatrix(l :+ r) == result)
  }

  @Test
  def diagonalTestTiny() {
    val l = toBM(Seq(
      Array[Double](1,2,3,4),
      Array[Double](5,6,7,8),
      Array[Double](9,10,11,12),
      Array[Double](13,14,15,16)
    ))

    assert(l.diag.toSeq == Seq(1,6,11,16))
  }

  @Test
  def diagonalTestRandomized() {
    forAll(blockMatrixSquareBlocksGen) { (mat: BlockMatrix) =>
      val lm = mat.toLocalMatrix()
      val diagonalLength = math.min(lm.numRows, lm.numCols)
      val diagonal = (0 until diagonalLength).map(i => lm(i,i)).toArray

      if (mat.diag.toSeq == diagonal.toSeq)
        true
      else {
        println(s"mat: $lm")
        println(s"${mat.diag.toSeq} != ${diagonal.toSeq}")
        false
      }
    }.check()
  }

  @Test
  def fromLocalTest() {
    val numRows = 100
    val numCols = 100
    val breezeLocal: breeze.linalg.DenseMatrix[Double] = breeze.linalg.DenseMatrix.rand[Double](numRows, numCols)
    val sparkLocal = new DenseMatrix(numRows, numCols, breezeLocal.toArray)
    BlockMatrixIsDistributedMatrix.from(sc, sparkLocal, numRows - 1, numCols - 1).blocks.count()
  }

}
