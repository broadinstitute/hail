package is.hail.methods

import is.hail.expr.types._
import is.hail.linalg.GridPartitioner
import is.hail.table.Table
import is.hail.utils.plural
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object UpperIndexBounds {
  def groupPositionsByKey(posTable: Table): RDD[Array[Int]] = {
    require(posTable.valueSignature.size == 1 && posTable.valueSignature.types(0) == TInt32(),
      s"Expected table to have one value field of type int32, but found ${ posTable.valueSignature.size } value " +
      s"${ plural(posTable.valueSignature.size, "field") } of ${ plural(posTable.valueSignature.size, "type") } " +
      s"${ posTable.valueSignature.types.mkString(", ") }.")

    val fieldIndex = posTable.valueFieldIdx(0)

    posTable.groupByKey("positions").rdd.map(
      _.get(fieldIndex).asInstanceOf[Vector[Row]].toArray.map(_.get(0).asInstanceOf[Int]))
  }

  // positions is non-decreasing, radius is non-negative
  // for each index i, compute the largest index j such that positions[j]-positions[i] <= radius
  def computeUpperIndexBounds(positions: Array[Int], radius: Int): Array[Int] = {

    val n = positions.length
    val bounds = new Array[Int](n)
    var j = 0

    for (i <- positions.indices) {
      val maxPosition = positions(i) + radius
      while (j < n && positions(j) <= maxPosition) {
        j += 1
      }
      j -= 1
      bounds(i) = j
    }

    bounds
  }

  def shiftUpperIndexBounds(upperIndexBounds: RDD[Array[Int]]): Array[Long] = {
    val firstIndices = upperIndexBounds.map(_.length).collect().scanLeft(0L)(_ + _)

    (firstIndices, upperIndexBounds.collect()).zipped.flatMap {
      case (firstIndex, bounds) => bounds.map(firstIndex + _)
    }
  }

  /* computes the minimum set of blocks necessary to cover all pairs of indices (i, j) such that key[i] == key[j], 
  i <= j, and position[j] - position[i] <= radius.  If includeDiagonal=false, require i < j rather than i <= j. */
  def computeCoverByUpperTriangularBlocks(
    posTable: Table, gp: GridPartitioner, radius: Int, includeDiagonal: Boolean): Array[Int] = {

    val relativeUpperIndexBounds = groupPositionsByKey(posTable).map(positions => {
      scala.util.Sorting.quickSort(positions)
      computeUpperIndexBounds(positions, radius)
    })

    val absoluteUpperIndexBounds = shiftUpperIndexBounds(relativeUpperIndexBounds)

    if (includeDiagonal) {
      gp.rectanglesBlocks(absoluteUpperIndexBounds.zipWithIndex.map {
        case (j, i) => Array(i, i + 1, i, j + 1)
      })
    } else {
      gp.rectanglesBlocks(absoluteUpperIndexBounds.zipWithIndex.flatMap {
        case (j, i) => if (i == j) None else Some(Array(i, i + 1, i + 1, j + 1))
      })
    }
  }
}
