package is.hail.utils

import is.hail.annotations._
import is.hail.check._
import is.hail.expr.types.{TBoolean, TInterval, TStruct, Type}
import org.json4s.JValue
import org.json4s.JsonAST.JObject

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

case class Interval(start: Any, end: Any, includeStart: Boolean, includeEnd: Boolean) extends Serializable {

  def contains(pord: ExtendedOrdering, position: Any): Boolean = {
    val compareStart = pord.compare(position, start)
    if (compareStart > 0 || (compareStart == 0 && includeStart)) {
      val compareEnd = pord.compare(position, end)
      compareEnd < 0 || (compareEnd == 0 && includeEnd)
    } else
      false
  }

  def probablyOverlaps(pord: ExtendedOrdering, other: Interval): Boolean = {
    !definitelyDisjoint(pord, other)
  }

  def definitelyDisjoint(pord: ExtendedOrdering, other: Interval): Boolean =
    definitelyEmpty(pord) || other.definitelyEmpty(pord) ||
      disjointAndLessThan(pord, other) || disjointAndGreaterThan(pord, other)

  def definitelyEmpty(pord: ExtendedOrdering): Boolean = if (includeStart && includeEnd) pord.gt(start, end) else pord.gteq(start, end)

  def copy(start: Any = start, end: Any = end, includeStart: Boolean = includeStart, includeEnd: Boolean = includeEnd): Interval =
    Interval(start, end, includeStart, includeEnd)

  def toJSON(f: (Any) => JValue): JValue =
    JObject("start" -> f(start),
      "end" -> f(end),
      "includeStart" -> TBoolean().toJSON(includeStart),
      "includeEnd" -> TBoolean().toJSON(includeEnd))

  def disjointAndLessThan(pord: ExtendedOrdering, other: Interval): Boolean = {
    val c = pord.compare(this.end, other.start)
    c < 0 || (c == 0 && (!this.includeEnd || !other.includeStart))
  }

  def disjointAndGreaterThan(pord: ExtendedOrdering, other: Interval): Boolean = {
    val c = pord.compare(this.start, other.end)
    c > 0 || (c == 0 && (!this.includeStart || !other.includeEnd))
  }

  def adjacent(pord: ExtendedOrdering, other: Interval): Boolean = {
    !definitelyEmpty(pord) && !other.definitelyEmpty(pord) &&
      ((this.start == other.end && (this.includeStart || other.includeEnd)) ||
      (this.end == other.start && (this.includeEnd || other.includeStart)))
  }

  def union(pord: ExtendedOrdering, other: Interval): Array[Interval] = {
    unionNonEmpty(pord, other) match {
      case Some(i) => Array(i)
      case None => Array(this, other).filter(!_.definitelyEmpty(pord)).sorted(Interval.ordering(pord).toOrdering)
    }
  }
  // only unions if overlapping or adjacent
  def unionNonEmpty(pord: ExtendedOrdering, other: Interval): Option[Interval] = {
    if (probablyOverlaps(pord, other) || adjacent(pord, other)) {
      val (s, is) = pord.compare(this.start, other.start) match {
        case 0 => (this.start, this.includeStart || other.includeStart)
        case x if x < 0 => (this.start, this.includeStart)
        case x if x > 0 => (other.start, other.includeStart)
      }
      val (e, ie) = pord.compare(this.end, other.end) match {
        case 0 => (this.end, this.includeEnd || other.includeEnd)
        case x if x > 0 => (this.end, this.includeEnd)
        case x if x < 0 => (other.end, other.includeEnd)
      }
      Some(Interval(s, e, is, ie))
    } else None
  }

  def minimumSpanningInterval(pord: ExtendedOrdering, other: Interval): Interval = {
    union(pord, other) match {
      case Array() => this
      case Array(i1) => i1
      case Array(i1, i2) => Interval(i1.start, i2.end, i1.includeStart, i2.includeEnd)
    }
  }

  // only intersects if overlapping
  def intersect(pord: ExtendedOrdering, other: Interval): Option[Interval] = {
    if (probablyOverlaps(pord, other)) {
      val (s, is) = pord.compare(this.start, other.start) match {
        case 0 => (this.start, this.includeStart && other.includeStart)
        case x if x > 0 => (this.start, this.includeStart)
        case x if x < 0 => (other.start, other.includeStart)
      }
      val (e, ie) = pord.compare(this.end, other.end) match {
        case 0 => (this.end, this.includeEnd && other.includeEnd)
        case x if x < 0 => (this.end, this.includeEnd)
        case x if x > 0 => (other.end, other.includeEnd)
      }
      Some(Interval(s, e, is, ie))
    } else None
  }

  override def toString: String = (if (includeStart) "[" else "(") + start + "-" + end + (if (includeEnd) "]" else ")")
}

object Interval {
  def gen[P](pord: ExtendedOrdering, pgen: Gen[P]): Gen[Interval] =
    Gen.zip(pgen, pgen, Gen.coin(), Gen.coin())
      .map { case (x, y, s, e) =>
        if (pord.lt(x, y))
          Interval(x, y, s, e)
        else
          Interval(y, x, s, e)
      }

  def ordering(pord: ExtendedOrdering): ExtendedOrdering = new ExtendedOrdering {
    def compareNonnull(x: Any, y: Any, missingGreatest: Boolean): Int = {
      val xi = x.asInstanceOf[Interval]
      val yi = y.asInstanceOf[Interval]

      val c = pord.compare(xi.start, yi.start, missingGreatest)
      if (c != 0)
        return c
      if (xi.includeStart != yi.includeStart)
        return if (xi.includeStart) -1 else 1

      val c2 = pord.compare(xi.end, yi.end, missingGreatest)
      if (c2 != 0)
        return c2
      if (xi.includeEnd != yi.includeEnd)
        if (xi.includeEnd) 1 else -1
      else 0
    }
  }

  def fromRegionValue(iType: TInterval, region: Region, offset: Long): Interval = {
    val ur = new UnsafeRow(iType.fundamentalType.asInstanceOf[TStruct], region, offset)
    Interval(ur.get(0), ur.get(1), ur.getAs[Boolean](2), ur.getAs[Boolean](3))
  }
}

case class IntervalTree[U: ClassTag](root: Option[IntervalTreeNode[U]]) extends
  Traversable[(Interval, U)] with Serializable {
  override def size: Int = root.map(_.size).getOrElse(0)

  def definitelyEmpty(pord: ExtendedOrdering): Boolean = root.forall(_.definitelyEmpty(pord))

  def contains(pord: ExtendedOrdering, position: Any): Boolean = root.exists(_.contains(pord, position))

  def probablyOverlaps(pord: ExtendedOrdering, interval: Interval): Boolean = root.exists(_.probablyOverlaps(pord, interval))

  def definitelyDisjoint(pord: ExtendedOrdering, interval: Interval): Boolean = root.forall(_.definitelyDisjoint(pord, interval))

  def queryIntervals(pord: ExtendedOrdering, position: Any): Array[Interval] = {
    val b = Array.newBuilder[Interval]
    root.foreach(_.query(pord, b, position))
    b.result()
  }

  def queryValues(pord: ExtendedOrdering, position: Any): Array[U] = {
    val b = Array.newBuilder[U]
    root.foreach(_.queryValues(pord, b, position))
    b.result()
  }

  def queryOverlappingValues(pord: ExtendedOrdering, interval: Interval): Array[U] = {
    val b = Array.newBuilder[U]
    root.foreach(_.queryOverlappingValues(pord, b, interval))
    b.result()
  }

  def foreach[V](f: ((Interval, U)) => V) {
    root.foreach(_.foreach(f))
  }
}

object IntervalTree {
  def annotationTree[U: ClassTag](pord: ExtendedOrdering, values: Array[(Interval, U)]): IntervalTree[U] = {
    val iord = Interval.ordering(pord)
    val sorted = values.sortBy(_._1)(iord.toOrdering.asInstanceOf[Ordering[Interval]])
    new IntervalTree[U](fromSorted(pord, sorted, 0, sorted.length))
  }

  def apply(pord: ExtendedOrdering, intervals: Array[Interval]): IntervalTree[Unit] = {
    val iord = Interval.ordering(pord)
    val sorted = if (intervals.nonEmpty) {
      val unpruned = intervals.sorted(iord.toOrdering.asInstanceOf[Ordering[Interval]])
      var i = 0
      while (i < unpruned.length && unpruned(i).definitelyEmpty(pord)) {
        i += 1
      }
      if (i == unpruned.length)
        Array[Interval]()
      else {
        val ab = new ArrayBuilder[Interval](intervals.length)
        var tmp = unpruned(i)
        while (i < unpruned.length) {
          tmp.union(pord, unpruned(i)) match {
            case Array(i1, i2) =>
              ab += i1
              tmp = i2
            case Array(interval) =>
              tmp = interval
          }
          i += 1
        }
        ab += tmp
        ab.result()
      }
    } else intervals

    new IntervalTree[Unit](fromSorted(pord, sorted.map(i => (i, ())), 0, sorted.length))
  }

  def fromSorted[U: ClassTag](pord: ExtendedOrdering, intervals: Array[(Interval, U)]): IntervalTree[U] =
    new IntervalTree[U](fromSorted(pord, intervals, 0, intervals.length))

  private def fromSorted[U](pord: ExtendedOrdering, intervals: Array[(Interval, U)], start: Int, end: Int): Option[IntervalTreeNode[U]] = {
    if (start >= end)
      None
    else {
      val mid = (start + end) / 2
      val (i, v) = intervals(mid)
      val left = fromSorted(pord, intervals, start, mid)
      val right = fromSorted(pord, intervals, mid + 1, end)
      val range = Array(left, right).flatten.map(_.range)
      Some(IntervalTreeNode(i, left, right, {
        left.map(_.range.start)
        val min1 = left.map(x => pord.min(x.minimum, i.start)).getOrElse(i.start)
        right.map(x => pord.min(x.minimum, min1)).getOrElse(min1)
          val max1 = left.map(x => pord.max(x.maximum, i.end)).getOrElse(i.end)
        right.map(x => pord.max(x.maximum, max1)).getOrElse(max1)
        }, v))
    }
  }

  def gen[T](pord: ExtendedOrdering, pgen: Gen[T]): Gen[IntervalTree[Unit]] = {
    Gen.buildableOf[Array](Interval.gen(pord, pgen)).map(a => IntervalTree.apply(pord, a))
  }
}

case class IntervalTreeNode[U](i: Interval,
  left: Option[IntervalTreeNode[U]],
  right: Option[IntervalTreeNode[U]],
  range: Interval, value: U) extends Traversable[(Interval, U)] {

  override val size: Int =
    left.map(_.size).getOrElse(0) + right.map(_.size).getOrElse(0) + 1

  def definitelyEmpty(pord: ExtendedOrdering): Boolean = {
    left.forall(_.definitelyEmpty(pord)) &&
      i.definitelyEmpty(pord) &&
      right.forall(_.definitelyEmpty(pord))
  }

  def contains(pord: ExtendedOrdering, position: Any): Boolean = {
    range.contains(pord, position) &&
      (left.exists(_.contains(pord, position)) ||
        (pord.gteq(position, i.start) &&
          (i.contains(pord, position) ||
            right.exists(_.contains(pord, position)))))
  }

  def probablyOverlaps(pord: ExtendedOrdering, interval: Interval): Boolean = {
    !definitelyDisjoint(pord, interval)
  }

  def definitelyDisjoint(pord: ExtendedOrdering, interval: Interval): Boolean =
    pord.lt(interval.end, minimum) || pord.gt(interval.start, maximum) ||
      (left.forall(_.definitelyDisjoint(pord, interval)) &&
        i.definitelyDisjoint(pord, interval) &&
        right.forall(_.definitelyDisjoint(pord, interval)))

  def query(pord: ExtendedOrdering, b: mutable.Builder[Interval, _], position: Any) {
    if (pord.gteq(position, minimum) && pord.lteq(position, maximum)) {
      left.foreach(_.query(pord, b, position))
      if (pord.gteq(position, i.start)) {
        if (i.contains(pord, position))
          b += i
        right.foreach(_.query(pord, b, position))
      }
    }
  }

  def queryValues(pord: ExtendedOrdering, b: mutable.Builder[U, _], position: Any) {
    if (pord.gteq(position, minimum) && pord.lteq(position, maximum)) {
      left.foreach(_.queryValues(pord, b, position))
      if (pord.gteq(position, i.start)) {
        if (i.contains(pord, position))
          b += value
        right.foreach(_.queryValues(pord, b, position))
      }
    }
  }

  def queryOverlappingValues(pord: ExtendedOrdering, b: mutable.Builder[U, _], interval: Interval) {
    if (pord.gteq(interval.end, minimum) && pord.lteq(interval.start, maximum)) {
      left.foreach(_.queryOverlappingValues(pord, b, interval))
      if (pord.gteq(interval.end, i.start)) {
        if (i.probablyOverlaps(pord, interval))
          b += value
        right.foreach(_.queryOverlappingValues(pord, b, interval))
      }
    }
  }

  def foreach[V](f: ((Interval, U)) => V) {
    left.foreach(_.foreach(f))
    f((i, value))
    right.foreach(_.foreach(f))
  }
}
