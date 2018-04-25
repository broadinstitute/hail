package is.hail.sparkextras

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class GeneralRDDPartition[T](index: Int, partitionInputs: Array[(Int, Partition)]) extends Partition

class GeneralRDD[T](@transient var sc: SparkContext,
  var rdds: Array[RDD[T]],
  var inputs: Array[Array[(Int, Int)]])(implicit tct: ClassTag[T]) extends RDD[Array[Iterator[T]]](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    // Do not call getPartitions here!
    val parentPartitions = rdds.zipWithIndex.map { case (rdd, i) => (i, rdd.partitions) }.toMap
    inputs.zipWithIndex.map { case (input, i) =>
      val partitionInputs = input.map { case (rddIndex, partitionIndex) => (rddIndex, parentPartitions(rddIndex)(partitionIndex)) }
      new GeneralRDDPartition[T](i, partitionInputs)
    }
  }

  override def compute(
    split: Partition,
    context: TaskContext
  ): Iterator[Array[Iterator[T]]] = {
    // Do not call partitions or getPartitions here!
    val gp = split.asInstanceOf[GeneralRDDPartition[T]]
    Iterator.single(gp.partitionInputs.map { case (rddIndex, partition) =>
      val rdd = rdds(rddIndex)
      rdd.iterator(partition, context)
    })
  }

  override def getDependencies: Seq[Dependency[_]] = {
    inputs
      .zipWithIndex
      .flatMap { case (input, i) => input.map { case (rddIndex, partitionIndex) => (rddIndex, partitionIndex, i) } }
      .groupBy(_._1)
      .map { case (rddIndex, x) =>
        new NarrowDependency[T](rdds(rddIndex)) {
          override def getParents(partitionId: Int): Seq[Int] =
            x.filter {
              partitionId == _._3
            }.map(_._2)
        }
      }.toSeq
  }
}
