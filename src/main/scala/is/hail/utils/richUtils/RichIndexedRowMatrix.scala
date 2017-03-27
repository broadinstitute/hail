package is.hail.utils.richUtils

import org.apache.spark.Partitioner
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRowMatrix}
import org.apache.spark.rdd.RDD

import scala.reflect.classTag

/**
  * Adds toBlockMatrixDense method to IndexedRowMatrix
  *
  * The hope is that this class is temporary, as I'm going to make a PR to Spark with this method
  * since it seems broadly useful.
  */

object RichIndexedRowMatrix {

  implicit class RichIndexedRowMatrix(indexedRowMatrix: IndexedRowMatrix) {
    def toBlockMatrixDense(): BlockMatrix = {
      toBlockMatrixDense(1024, 1024)
    }

    def toBlockMatrixDense(rowsPerBlock: Int, colsPerBlock: Int): BlockMatrix = {
      require(rowsPerBlock > 0,
        s"rowsPerBlock needs to be greater than 0. rowsPerBlock: $rowsPerBlock")
      require(colsPerBlock > 0,
        s"colsPerBlock needs to be greater than 0. colsPerBlock: $colsPerBlock")

      val m = indexedRowMatrix.numRows()
      val n = indexedRowMatrix.numCols()
      val lastRowBlockIndex = m / rowsPerBlock
      val lastColBlockIndex = n / colsPerBlock
      val lastRowBlockSize = (m % rowsPerBlock).toInt
      val lastColBlockSize = (n % colsPerBlock).toInt
      val numRowBlocks = math.ceil(m.toDouble / rowsPerBlock).toInt
      val numColBlocks = math.ceil(n.toDouble / colsPerBlock).toInt

      val intClass = classTag[Int].runtimeClass
      val gpConstructor = Class.forName("org.apache.spark.mllib.linalg.distributed.GridPartitioner")
        .getDeclaredConstructor(intClass,intClass,intClass,intClass)
      def sneakyGridPartitioner(nRowBlocks: Int, nColBlocks: Int, nRowsPerPart: Int, nColsPerPart: Int): Partitioner = try {
        gpConstructor.setAccessible(true)
        gpConstructor.newInstance(nRowBlocks: java.lang.Integer, nColBlocks: java.lang.Integer,
          nRowsPerPart: java.lang.Integer, nColsPerPart: java.lang.Integer).asInstanceOf[Partitioner]
      } finally {
        gpConstructor.setAccessible(false)
      }

      val blocks: RDD[((Int, Int), Matrix)] = indexedRowMatrix.rows.flatMap({ ir =>
        val blockRow = ir.index / rowsPerBlock
        val rowInBlock = ir.index % rowsPerBlock

        ir.vector.toArray
          .grouped(colsPerBlock)
          .zipWithIndex
          .map({ case (values, blockColumn) =>
            ((blockRow.toInt, blockColumn), (rowInBlock.toInt, values))
          })
      }).groupByKey(sneakyGridPartitioner(numRowBlocks, numColBlocks, rowsPerBlock, colsPerBlock)).map({
        case ((blockRow, blockColumn), itr) =>
          val actualNumRows: Int = if (blockRow == lastRowBlockIndex) lastRowBlockSize else rowsPerBlock
          val actualNumColumns: Int = if (blockColumn == lastColBlockIndex) lastColBlockSize else colsPerBlock

          val arraySize = actualNumRows * actualNumColumns
          val matrixAsArray = new Array[Double](arraySize)
          itr.foreach({ case (rowWithinBlock, values) =>
            var i = 0
            while (i < values.length) {
              matrixAsArray.update(i * actualNumRows + rowWithinBlock, values(i))
              i += 1
            }
          })
          ((blockRow, blockColumn), new DenseMatrix(actualNumRows, actualNumColumns, matrixAsArray))
      })
      new BlockMatrix(blocks, rowsPerBlock, colsPerBlock)
    }
  }

}


