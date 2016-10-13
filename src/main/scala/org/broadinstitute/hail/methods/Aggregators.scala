package org.broadinstitute.hail.methods

import org.apache.spark.util.StatCounter
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.driver.HailConfiguration
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.stats.{CallStats, CallStatsCombiner, HistogramCombiner, InfoScoreCombiner}
import org.broadinstitute.hail.utils._
import org.broadinstitute.hail.variant._

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.input.Position

object Aggregators {

  def buildVariantAggregations(vds: VariantDataset, ec: EvalContext): Option[(Variant, Annotation, Iterable[Genotype]) => Unit] = {
    val aggregators = ec.aggregationFunctions.toArray
    val aggregatorA = ec.a

    if (aggregators.nonEmpty) {

      val localSamplesBc = vds.sampleIdsBc
      val localAnnotationsBc = vds.sampleAnnotationsBc

      val f = (v: Variant, va: Annotation, gs: Iterable[Genotype]) => {
        val baseArray = aggregators.map(_.copy())
        aggregatorA(0) = v
        aggregatorA(1) = va
        (gs, localSamplesBc.value, localAnnotationsBc.value).zipped
          .foreach {
            case (g, s, sa) =>
              aggregatorA(2) = s
              aggregatorA(3) = sa
              baseArray.foreach {
                _.seqOp(g)
              }
          }

        baseArray.foreach { agg => aggregatorA(agg.idx) = agg.result }
      }
      Some(f)
    } else None
  }

  def buildSampleAggregations(vds: VariantDataset, ec: EvalContext): Option[(String) => Unit] = {
    val aggregators = ec.aggregationFunctions.toArray
    val aggregatorA = ec.a

    if (aggregators.isEmpty)
      None
    else {

      val localSamplesBc = vds.sampleIdsBc
      val localAnnotationsBc = vds.sampleAnnotationsBc

      val nAggregations = aggregators.length
      val nSamples = vds.nSamples
      val depth = HailConfiguration.treeAggDepth(vds.nPartitions)

      val baseArray = MultiArray2.fill[Aggregator](nSamples, nAggregations)(null)
      for (i <- 0 until nSamples; j <- 0 until nAggregations) {
        baseArray.update(i, j, aggregators(j).copy())
      }

      val result = vds.rdd.treeAggregate(baseArray)({ case (arr, (v, (va, gs))) =>
        aggregatorA(0) = v
        aggregatorA(1) = va
        var i = 0
        gs.foreach { g =>
          aggregatorA(2) = localSamplesBc.value(i)
          aggregatorA(3) = localAnnotationsBc.value(i)

          var j = 0
          while (j < nAggregations) {
            arr(i, j).seqOp(g)
            j += 1
          }
          i += 1
        }
        arr
      }, { case (arr1, arr2) =>
        for (i <- 0 until nSamples; j <- 0 until nAggregations) {
          arr1(i, j).combOp(arr2(i, j))
        }
        arr1
      }, depth = depth)

      val sampleIndex = vds.sampleIds.zipWithIndex.toMap
      Some((s: String) => {
        val i = sampleIndex(s)
        for (j <- 0 until nAggregations) {
          aggregatorA(aggregators(j).idx) = result(i, j).result
        }
      })
    }
  }

  def makeFunctions(ec: EvalContext): (Array[Aggregator], (Array[Aggregator], (Any, Any)) => Array[Aggregator],
    (Array[Aggregator], Array[Aggregator]) => Array[Aggregator], (Array[Aggregator]) => Unit) = {

    val aggregators = ec.aggregationFunctions.toArray

    val arr = ec.a

    val baseArray = Array.fill[Aggregator](aggregators.length)(null)

    val zero = {
      for (i <- baseArray.indices)
        baseArray(i) = aggregators(i).copy()
      baseArray
    }

    val seqOp = (array: Array[Aggregator], b: (Any, Any)) => {
      val (aggT, annotation) = b
      ec.set(0, annotation)
      for (i <- array.indices) {
        array(i).seqOp(aggT)
      }
      array
    }

    val combOp = (arr1: Array[Aggregator], arr2: Array[Aggregator]) => {
      for (i <- arr1.indices) {
        arr1(i).combOp(arr2(i))
      }
      arr1
    }

    val resultOp = (array: Array[Aggregator]) => array.foreach { res => arr(res.idx) = res.result }

    (zero, seqOp, combOp, resultOp)
  }
}

class CountAggregator(val f: (Any) => Any, val idx: Int) extends TypedAggregator[Long, Boolean] {

  var _state = 0L

  override def result = _state

  override def merge(x: Boolean) {
    if (x)
      _state += 1
  }

  override def seqOp(x: Any) {
    merge(f(x) != null)
  }

  override def combOp(agg2: TypedAggregator[_, _]) {
    _state += agg2.asInstanceOf[CountAggregator]._state
  }

  override def copy() = new CountAggregator(f, idx)
}

class FractionAggregator(val f: (Any) => Any, val idx: Int, localA: ArrayBuffer[Any], bodyFn: () => Any, lambdaIdx: Int)
  extends TypedAggregator[Option[Double], Boolean] {

  var _num = 0L
  var _denom = 0L

  override def result = {
    divOption(_num.toDouble, _denom)
  }

  override def merge(x: Boolean) {
    _denom += 1
    if (x)
      _num += 1
  }

  override def seqOp(x: Any) {
    val r = f(x)
    if (r != null) {
      localA(lambdaIdx) = r
      merge(bodyFn().asInstanceOf[Boolean])
    }
  }

  override def combOp(agg2: TypedAggregator[_, _]) {
    val fracAgg = agg2.asInstanceOf[FractionAggregator]
    _num += fracAgg._num
    _denom += fracAgg._denom
  }

  override def copy() = new FractionAggregator(f, idx, localA, bodyFn, lambdaIdx)
}

class StatAggregator(val f: (Any) => Any, val idx: Int) extends TypedAggregator[StatCounter, Double] {

  var _state = new StatCounter()

  override def result = _state

  override def seqOp(x: Any) {
    val r = f(x)
    if (r != null)
      merge(DoubleNumericConversion.to(r))
  }

  override def merge(x: Double) {
    _state.merge(x)
  }

  override def combOp(agg2: TypedAggregator[_, _]) {
    _state.merge(agg2.asInstanceOf[StatAggregator]._state)
  }

  override def copy() = new StatAggregator(f, idx)
}

class HistAggregator(val f: (Any) => Any, val idx: Int, indices: Array[Double])
  extends TypedAggregator[HistogramCombiner, Double] {

  var _state = new HistogramCombiner(indices)

  override def result = _state

  override def seqOp(x: Any) {
    val r = f(x)
    if (r != null)
      merge(DoubleNumericConversion.to(r))
  }

  override def merge(x: Double) {
    _state.merge(x)
  }

  override def combOp(agg2: TypedAggregator[_, _]) {
    _state.merge(agg2.asInstanceOf[HistAggregator]._state)
  }

  override def copy() = new HistAggregator(f, idx, indices)
}

class CollectAggregator(val f: (Any) => Any, val idx: Int) extends TypedAggregator[ArrayBuffer[Any], Any] {

  var _state = new ArrayBuffer[Any]

  override def result = _state

  override def merge(x: Any) {
    _state += x
  }

  override def combOp(agg2: TypedAggregator[_, _]) = _state ++= agg2.asInstanceOf[CollectAggregator]._state

  override def copy() = new CollectAggregator(f, idx)
}

class InfoScoreAggregator(val f: (Any) => Any, val idx: Int) extends TypedAggregator[InfoScoreCombiner, Genotype] {

  var _state = new InfoScoreCombiner()

  override def result = _state

  override def merge(x: Genotype) {
    _state.merge(x)
  }

  override def combOp(agg2: TypedAggregator[_, _]) {
    _state.merge(agg2.asInstanceOf[InfoScoreAggregator]._state)
  }

  override def copy() = new InfoScoreAggregator(f, idx)
}

class SumAggregator(val f: (Any) => Any, val idx: Int) extends TypedAggregator[Double, Double] {
  var _state = 0d

  override def result = _state

  override def merge(x: Double) {
    _state += x
  }

  override def seqOp(x: Any) {
    val r = f(x)
    if (r != null)
      merge(DoubleNumericConversion.to(r))
  }

  override def combOp(agg2: TypedAggregator[_, _]) = _state += agg2.asInstanceOf[SumAggregator]._state

  override def copy() = new SumAggregator(f, idx)
}

class SumArrayAggregator(val f: (Any) => Any, val idx: Int, localPos: Position)
  extends TypedAggregator[IndexedSeq[Double], IndexedSeq[_]] {

  var _state: Array[Double] = null

  override def result =
    if (_state == null)
      null
    else
      _state.toIndexedSeq

  override def merge(x: IndexedSeq[_]) {
    if (_state == null)
      _state = x.map(DoubleNumericConversion.to).toArray
    else {
      if (x.length != _state.length)
        ParserUtils.error(localPos,
          s"""cannot aggregate arrays of unequal length with `sum'
              |  Found conflicting arrays of size (${ _state.length }) and (${ x.length })""".stripMargin)
      else {
        var i = 0
        while (i < _state.length) {
          _state(i) += DoubleNumericConversion.to(x(i))
          i += 1
        }
      }
    }
  }

  override def combOp(agg2: TypedAggregator[_, _]) = {
    val agg2result = agg2.asInstanceOf[SumArrayAggregator]._state
    if (_state.length != agg2result.length)
      ParserUtils.error(localPos,
        s"""cannot aggregate arrays of unequal length with `sum'
            |  Found conflicting arrays of size (${ _state.length }) and (${ agg2result.length })""".stripMargin)
    for (i <- _state.indices)
      _state(i) += agg2result(i)
  }

  override def copy() = new SumArrayAggregator(f, idx, localPos)
}

class CallStatsAggregator(val f: (Any) => Any, val idx: Int, variantF: () => Any)
  extends TypedAggregator[Option[CallStats], Genotype] {

  var combiner: CallStatsCombiner = _
  var tried = false

  def result: Option[CallStats] = Option(combiner).map(_.result())

  override def seqOp(x: Any) {
    if (combiner != null) {
      val r = f(x)
      if (r != null)
        merge(r.asInstanceOf[Genotype])
    } else if (!tried) {
      variantF() match {
        case null => tried = true
        case v: Variant =>
          combiner = new CallStatsCombiner(v)
          seqOp(x)
        case other => fatal(s"got unexpected result $other from variant function")
      }
    }
  }

  def merge(x: Genotype) {
    combiner.merge(x)
  }

  def combOp(agg2: TypedAggregator[_, _]) {
    combiner.merge(agg2.asInstanceOf[CallStatsAggregator].combiner)
  }

  def copy(): TypedAggregator[Option[CallStats], Genotype] = new CallStatsAggregator(f, idx, variantF)
}