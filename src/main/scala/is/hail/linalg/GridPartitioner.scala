package is.hail.linalg

import org.apache.spark.Partitioner
import breeze.linalg.{DenseVector => BDV}
import is.hail.utils._


case class GridPartitioner(blockSize: Int, nRows: Long, nCols: Long, maybeSparse: Option[Array[Int]] = None) extends Partitioner {
  require(nRows > 0 && nRows <= Int.MaxValue.toLong * blockSize)
  require(nCols > 0 && nCols <= Int.MaxValue.toLong * blockSize)
    
  def indexBlockIndex(index: Long): Int = (index / blockSize).toInt

  def offsetBlockOffset(index: Long): Int = (index % blockSize).toInt

  val nBlockRows: Int = indexBlockIndex(nRows - 1) + 1
  val nBlockCols: Int = indexBlockIndex(nCols - 1) + 1
  
  val lastBlockRowNRows: Int = offsetBlockOffset(nRows - 1) + 1
  val lastBlockColNCols: Int = offsetBlockOffset(nCols - 1) + 1
  
  def blockRowNRows(i: Int): Int = if (i < nBlockRows - 1) blockSize else lastBlockRowNRows
  def blockColNCols(j: Int): Int = if (j < nBlockCols - 1) blockSize else lastBlockColNCols

  def blockBlockRow(bi: Int): Int = bi % nBlockRows
  def blockBlockCol(bi: Int): Int = bi / nBlockRows

  def blockDims(bi: Int): (Int, Int) = (blockRowNRows(blockBlockRow(bi)), blockColNCols(blockBlockCol(bi)))
  
  def blockCoordinates(bi: Int): (Int, Int) = (blockBlockRow(bi), blockBlockCol(bi))

  def coordinatesBlock(i: Int, j: Int): Int = {
    require(0 <= i && i < nBlockRows, s"Block row $i out of range [0, $nBlockRows).")
    require(0 <= j && j < nBlockCols, s"Block column $j out of range [0, $nBlockCols).")
    i + j * nBlockRows
  }
  
  require(maybeSparse.forall(blocks =>
    blocks.isEmpty || (blocks.isIncreasing && blocks.head >= 0 && blocks.last < nBlockRows * nBlockCols)))
  
  val isSparse: Boolean = maybeSparse.isDefined
  
  val partIndexBlockIndex: Int => Int = maybeSparse match {
    case Some(blocks) => blocks
    case None => pi => pi
  }

  val blockIndexPartIndex: Int => Int = maybeSparse match {
    case Some(blocks) => blocks.zipWithIndex.toMap
    case None => bi => bi
  }
  
  def filterBlocks(blocksToKeep: Array[Int]): (GridPartitioner, Array[Int]) = {
    val (filteredBlocks, partsToKeep) = maybeSparse match {
      case Some(blocks) =>
        val blocksToKeepSet = blocksToKeep.toSet
        blocks.zipWithIndex.filter { case (bi, i) => blocksToKeepSet(bi) }.unzip
      case None => (blocksToKeep, blocksToKeep)  // FIXME: error message if not valid
    }
    
    (GridPartitioner(blockSize, nRows, nCols, Some(filteredBlocks)), partsToKeep)
  }
  
  override val numPartitions: Int = maybeSparse match {
    case Some(blocks) => blocks.length
    case None => nBlockRows * nBlockCols
  }
  
  override def getPartition(key: Any): Int = key match {
    case (i: Int, j: Int) => blockIndexPartIndex(coordinatesBlock(i, j))
  }
  
  def transpose: (GridPartitioner, Array[Int]) = {
    val gpT = GridPartitioner(blockSize, nCols, nRows)

    def transposeBI(bi: Int): Int = coordinatesBlock(gpT.blockBlockCol(bi), gpT.blockBlockRow(bi))

    val (transposedBI, transposePI) =
      maybeSparse.getOrElse((0 until numPartitions).toArray)
      .map(transposeBI)
      .zipWithIndex
      .sortBy(_._1)
      .unzip

    val transposedGP = maybeSparse match {
      case Some(blocks) => GridPartitioner(blockSize, nCols, nRows, Some(transposedBI))
      case None => gpT
    }

    val inverseTransposePI = transposePI.zipWithIndex.sortBy(_._1).map(_._2)

    (transposedGP, inverseTransposePI)
  }

  def vectorOnBlockRow(v: BDV[Double], i: Int): BDV[Double] = {
    val firstRow = i * blockSize
    v(firstRow until firstRow + blockRowNRows(i))
  }
  
  def vectorOnBlockCol(v: BDV[Double], j: Int): BDV[Double] = {
    val firstCol = j * blockSize
    v(firstCol until firstCol + blockColNCols(j))
  }
  
  // returns increasing array of all blocks intersecting the diagonal band consisting of
  //   all entries with -lowerBandwidth <= (colIndex - rowIndex) <= upperBandwidth
  def bandedBlocks(lowerBandwidth: Long, upperBandwidth: Long): Array[Int] = {
    require(lowerBandwidth >= 0 && upperBandwidth >= 0)
    
    val lowerBlockBandwidth = indexBlockIndex(lowerBandwidth + blockSize - 1)
    val upperBlockBandwidth = indexBlockIndex(upperBandwidth + blockSize - 1)

    (for { j <- 0 until nBlockCols
           i <- ((j - upperBlockBandwidth) max 0) to
                ((j + lowerBlockBandwidth) min (nBlockRows - 1))
    } yield (j * nBlockRows) + i).toArray
  }

  // returns increasing array of all blocks intersecting the rectangle [firstRow, lastRow] x [firstCol, lastCol]
  def rectangularBlocks(firstRow: Long, lastRow: Long, firstCol: Long, lastCol: Long): Array[Int] = {
    require(firstRow >= 0 && firstRow <= lastRow && lastRow <= nRows)
    require(firstCol >= 0 && firstCol <= lastCol && lastCol <= nCols)
    
    val firstBlockRow = indexBlockIndex(firstRow)
    val lastBlockRow = indexBlockIndex(lastRow)
    val firstBlockCol = indexBlockIndex(firstCol)
    val lastBlockCol = indexBlockIndex(lastCol)
    
    (for { j <- firstBlockCol to lastBlockCol
           i <- firstBlockRow to lastBlockRow
    } yield (j * nBlockRows) + i).toArray
  }

  // returns increasing array of all blocks intersecting the union of rectangles
  def rectangularBlocks(rectangles: Array[Array[Long]]): Array[Int] = {
    require(rectangles.forall(r => r.length == 4))
    val rects = rectangles.foldLeft(Set[Int]())((s, r) => s ++ rectangularBlocks(r(0), r(1), r(2), r(3))).toArray    
    scala.util.Sorting.quickSort(rects)
    rects
  }
}
