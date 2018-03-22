package is.hail.sparkextras

import is.hail.utils._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.storage._
import org.apache.spark.util.random._
import org.apache.spark.util.Utils
import scala.reflect.ClassTag

object ContextRDD {
  def empty[C <: AutoCloseable, T: ClassTag](
    sc: SparkContext,
    mkc: () => C
  ): ContextRDD[C, T] =
    new ContextRDD(sc.emptyRDD[C => Iterator[T]], mkc)

  def empty[C <: AutoCloseable : Pointed, T: ClassTag](
    sc: SparkContext
  ): ContextRDD[C, T] =
    new ContextRDD(sc.emptyRDD[C => Iterator[T]], point[C])

  // this one weird trick permits the caller to specify T without C
  sealed trait Empty[T] {
    def apply[C <: AutoCloseable](
      sc: SparkContext,
      mkc: () => C
    )(implicit tct: ClassTag[T]
    ): ContextRDD[C, T] = empty(sc, mkc)
  }
  private[this] object emptyInstance extends Empty[Nothing]
  def empty[T] = emptyInstance.asInstanceOf[Empty[T]]

  def union[C <: AutoCloseable : Pointed, T: ClassTag](
    sc: SparkContext,
    xs: Seq[ContextRDD[C, T]]
  ): ContextRDD[C, T] = union(sc, xs, point[C])

  def union[C <: AutoCloseable, T: ClassTag](
    sc: SparkContext,
    xs: Seq[ContextRDD[C, T]],
    mkc: () => C
  ): ContextRDD[C, T] =
    new ContextRDD(sc.union(xs.map(_.rdd)), mkc)

  def weaken[C <: AutoCloseable, T: ClassTag](
    rdd: RDD[T],
    mkc: () => C
  ): ContextRDD[C, T] =
    new ContextRDD(rdd.mapPartitions(it => Iterator.single((ctx: C) => it)), mkc)

  def weaken[C <: AutoCloseable : Pointed, T: ClassTag](
    rdd: RDD[T]
  ): ContextRDD[C, T] =
    new ContextRDD(
      rdd.mapPartitions(it => Iterator.single((ctx: C) => it)),
      point[C])

  type ElementType[C, T] = C => Iterator[T]
}

class ContextRDD[C <: AutoCloseable, T: ClassTag](
  val rdd: RDD[C => Iterator[T]],
  val mkc: () => C
) extends Serializable {
  import ContextRDD._

  def run: RDD[T] =
    rdd.mapPartitions { part => using(mkc()) { cc => part.flatMap(_(cc)) } }

  private[this] def inCtx[U: ClassTag](
    f: C => Iterator[U]
  ): Iterator[C => Iterator[U]] = Iterator.single(f)

  private[this] def withoutContext[U: ClassTag](
    f: Iterator[T] => Iterator[U]
  ): ContextRDD[C, U] = new ContextRDD(rdd.map(_.andThen(f)), mkc)

  def map[U: ClassTag](f: T => U): ContextRDD[C, U] =
    withoutContext(_.map(f))

  def filter(f: T => Boolean): ContextRDD[C, T] =
    withoutContext(_.filter(f))

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): ContextRDD[C, U] =
    withoutContext(_.flatMap(f))

  def mapPartitions[U: ClassTag](
    f: Iterator[T] => Iterator[U],
    preservesPartitioning: Boolean = false
  ): ContextRDD[C, U] =
    cmapPartitions((_, part) => f(part), preservesPartitioning)

  def mapPartitionsWithIndex[U: ClassTag](
    f: (Int, Iterator[T]) => Iterator[U]
  ): ContextRDD[C, U] = cmapPartitionsWithIndex((i, _, part) => f(i, part))

  def zipPartitions[U: ClassTag, V: ClassTag](
    that: ContextRDD[C, U],
    preservesPartitioning: Boolean = false
  )(f: (Iterator[T], Iterator[U]) => Iterator[V]
  ): ContextRDD[C, V] =
    czipPartitions[U, V](that, preservesPartitioning)((_, l, r) => f(l, r))

  // FIXME: this should disappear when region values become non-serializable
  def aggregate[U: ClassTag](
    makeZero: () => U,
    seqOp: (U, T) => U,
    combOp: (U, U) => U
  ): U = aggregate[U, U](makeZero, seqOp, combOp, x => x, x => x)

  // FIXME: spark uses spark serialization to clone the zero, but its private,
  // so I just force the user to give me a constructor instead
  def aggregate[U: ClassTag, V: ClassTag](
    makeZero: () => U,
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    serialize: U => V,
    deserialize: V => U
  ): U = {
    val zeroValue = makeZero()
    // FIXME: I need to clean seqOp and combOp but that's private in
    // SparkContext, wtf...
    val aggregatePartition = { (it: Iterator[C => Iterator[T]]) =>
      using(mkc()) { c =>
        serialize(
          it.flatMap(_(c)).aggregate(zeroValue)(seqOp, combOp)) } }
    val localCombiner = { (_: Int, v: V) =>
      result = combOp(result, deserialize(v)) }
    var result = makeZero()
    sparkContext.runJob(rdd, aggregatePartition, localCombiner)
    result
  }

  private[this] def withContext[U: ClassTag](
    f: (C, Iterator[T]) => Iterator[U]
  ): ContextRDD[C, U] =
    new ContextRDD(rdd.map(useCtx => ctx => f(ctx, useCtx(ctx))), mkc)

  def cmap[U: ClassTag](f: (C, T) => U): ContextRDD[C, U] =
    withContext((c, it) => it.map(f(c, _)))

  def cfilter(f: (C, T) => Boolean): ContextRDD[C, T] =
    withContext((c, it) => it.filter(f(c, _)))

  def cflatMap[U: ClassTag](f: (C, T) => TraversableOnce[U]): ContextRDD[C, U] =
    withContext((c, it) => it.flatMap(f(c, _)))

  def cmapPartitions[U: ClassTag](
    f: (C, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false
  ): ContextRDD[C, U] = new ContextRDD(
    rdd.mapPartitions(
      part => inCtx(ctx => f(ctx, part.flatMap(_(ctx)))),
      preservesPartitioning),
    mkc)

  def cmapPartitionsWithIndex[U: ClassTag](
    f: (Int, C, Iterator[T]) => Iterator[U]
  ): ContextRDD[C, U] = new ContextRDD(
    rdd.mapPartitionsWithIndex(
      (i, part) => inCtx(ctx => f(i, ctx, part.flatMap(_(ctx))))),
    mkc)

  def czipPartitions[U: ClassTag, V: ClassTag](
    that: ContextRDD[C, U],
    preservesPartitioning: Boolean = false
  )(f: (C, Iterator[T], Iterator[U]) => Iterator[V]
  ): ContextRDD[C, V] = new ContextRDD(
    rdd.zipPartitions(that.rdd, preservesPartitioning)(
      (l, r) => inCtx(ctx => f(ctx, l.flatMap(_(ctx)), r.flatMap(_(ctx))))),
    mkc)

  def subsetPartitions(keptPartitionIndices: Array[Int]): ContextRDD[C, T] =
    onRDD(_.subsetPartitions(keptPartitionIndices))

  def reorderPartitions(oldIndices: Array[Int]): ContextRDD[C, T] =
    onRDD(_.reorderPartitions(oldIndices))

  def adjustPartitions(
    adjustments: IndexedSeq[Array[Adjustment[T]]]
  ): ContextRDD[C, T] = {
    def contextIgnorantPartitionFunction(
      f: Iterator[T] => Iterator[T]
    ): Iterator[C => Iterator[T]] => Iterator[C => Iterator[T]] =
      it => inCtx(ctx => f(it.flatMap(useCtx => useCtx(ctx))))
    def contextIgnorantAdjustment(a: Adjustment[T]): Adjustment[C => Iterator[T]] =
      Adjustment(a.index, contextIgnorantPartitionFunction(a.f))
    val contextIgnorantAdjustments =
      adjustments.map(as => as.map(a => contextIgnorantAdjustment(a)))
    onRDD(rdd => new AdjustedPartitionsRDD(rdd, contextIgnorantAdjustments))
  }

  def coalesce(numPartitions: Int, shuffle: Boolean = false): ContextRDD[C, T] =
    // NB: the run marks the end of a context lifetime, the next one starts
    // after the shuffle
    if (shuffle)
      ContextRDD.weaken(run.coalesce(numPartitions, shuffle), mkc)
    else
      onRDD(_.coalesce(numPartitions, shuffle))

  def sample(
    withReplacement: Boolean,
    fraction: Double,
    seed: Long
  ): ContextRDD[C, T] = {
    require(fraction >= 0.0 && fraction <= 1.0)
    new ContextRDD(
      (if (withReplacement)
        new ContextSampledRDD(rdd, new PoissonSampler(fraction), seed)
      else
        new ContextSampledRDD(rdd, new BernoulliSampler(fraction), seed)),
      mkc)
  }

  def sparkContext: SparkContext = rdd.sparkContext

  def getNumPartitions: Int = rdd.getNumPartitions

  def partitions: Array[Partition] = rdd.partitions

  private[this] def onRDD(
    f: RDD[C => Iterator[T]] => RDD[C => Iterator[T]]
  ): ContextRDD[C, T] = new ContextRDD(f(rdd), mkc)
}
