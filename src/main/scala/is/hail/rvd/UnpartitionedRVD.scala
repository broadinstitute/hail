package is.hail.rvd

import is.hail.annotations.RegionValue
import is.hail.utils._
import is.hail.expr.types.TStruct
import is.hail.sparkextras._
import is.hail.io.CodecSpec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object UnpartitionedRVD {
  def empty(sc: SparkContext, rowType: TStruct): UnpartitionedRVD =
    new UnpartitionedRVD(rowType, ContextRDD.empty[RegionValue](sc, RVDContext.default _))
}

class UnpartitionedRVD(val rowType: TStruct, val crdd: ContextRDD[RVDContext, RegionValue]) extends RVD {
  self =>

  def this(rowType: TStruct, rdd: RDD[RegionValue]) =
    this(rowType, ContextRDD.weaken(rdd, RVDContext.default _))

  val rdd = crdd.run

  def filter(f: (RegionValue) => Boolean): UnpartitionedRVD = new UnpartitionedRVD(rowType, crdd.filter(f))

  def persist(level: StorageLevel): UnpartitionedRVD = {
    val PersistedRVRDD(persistedRDD, iterationRDD) = persistRVRDD(level)
    new UnpartitionedRVD(rowType, iterationRDD) {
      override def storageLevel: StorageLevel = persistedRDD.getStorageLevel

      override def persist(newLevel: StorageLevel): UnpartitionedRVD = {
        if (newLevel == StorageLevel.NONE)
          unpersist()
        else {
          persistedRDD.persist(newLevel)
          this
        }
      }

      override def unpersist(): UnpartitionedRVD = {
        persistedRDD.unpersist()
        self
      }
    }
  }

  def sample(withReplacement: Boolean, p: Double, seed: Long): UnpartitionedRVD =
    new UnpartitionedRVD(
      rowType,
      ContextRDD.weaken(rdd.sample(withReplacement, p, seed), RVDContext.default _))

  def write(path: String, codecSpec: CodecSpec): Array[Long] = {
    val (partFiles, partitionCounts) = rdd.writeRows(path, rowType, codecSpec)
    val spec = UnpartitionedRVDSpec(rowType, codecSpec, partFiles)
    spec.write(sparkContext.hadoopConfiguration, path)
    partitionCounts
  }

  def coalesce(maxPartitions: Int, shuffle: Boolean): UnpartitionedRVD =
    new UnpartitionedRVD(
      rowType,
      ContextRDD.weaken(
        rdd.coalesce(maxPartitions, shuffle = shuffle),
        RVDContext.default _))
}
