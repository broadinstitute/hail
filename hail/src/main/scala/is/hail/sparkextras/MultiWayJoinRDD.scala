package is.hail.sparkextras

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object MultiWayJoinRDD {
  def apply[T: ClassTag , V: ClassTag](
    rdds: IndexedSeq[RDD[T]]
  )(f: (Array[Iterator[T]]) => Iterator[V]): MultiWayJoinRDD[T, V] = {
    new MultiWayJoinRDD(rdds.head.sparkContext, rdds, f)
  }
}

private case class MultiWayJoinPartition(val index: Int, val partitions: IndexedSeq[Partition])
  extends Partition

class MultiWayJoinRDD[T: ClassTag, V: ClassTag](
  sc: SparkContext,
  var rdds: IndexedSeq[RDD[T]],
  var f: (Array[Iterator[T]]) => Iterator[V]
) extends RDD[V](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.length > 0)
  private val numParts = rdds(0).partitions.length
  require(rdds.forall(rdd => rdd.partitions.length == numParts))

  override val partitioner = None

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](numParts) { i =>
      MultiWayJoinPartition(i, rdds.map(rdd => rdd.partitions(i)))
    }
  }

  override def compute(s: Partition, tc: TaskContext) = {
    val partitions = s.asInstanceOf[MultiWayJoinPartition].partitions
    val arr = Array.tabulate(rdds.length)(i => rdds(i).iterator(partitions(i), tc))
    f(arr)
  }

  override def clearDependencies() {
    super.clearDependencies
    rdds = null
    f = null
  }
}
