package is.hail.utils

import java.util

object BufferedAggregatorIterator {
  val loadFactor: Float = 0.75f
}

class BufferedAggregatorIterator[T, U, K](
  it: Iterator[T],
  makeComb: () => U,
  makeKey: T => K,
  sequence: (T, U) => Unit,
  bufferSize: Int
) extends Iterator[(K, U)] {

  private val fb = it.toFlipbookIterator
  private var popped: util.Map.Entry[K, U] = _
  private var remainder: Iterator[(K, U)] = Iterator.empty

  // TODO: use a heavy hitters buffer here
  private val buffer = new util.LinkedHashMap[K, U](
    (bufferSize / BufferedAggregatorIterator.loadFactor).toInt,
    BufferedAggregatorIterator.loadFactor,
    true) {
    override def removeEldestEntry(eldest: util.Map.Entry[K, U]): Boolean = {
      if (size() > bufferSize) {
        popped = eldest
        true
      } else false
    }
  }

  def hasNext: Boolean = {
    fb.isValid || buffer.size() > 0
  }

  def next(): (K, U) = {
    if (!hasNext)
      throw new NoSuchElementException
    while (fb.isValid) {
      val value = fb.value
      val key = makeKey(value)
      if (buffer.containsKey(key)) {
        sequence(value, buffer.get(key))
        fb.advance()
      } else {
        val agg = makeComb()
        sequence(value, agg)
        fb.advance()
        buffer.put(key, agg)
        if (popped != null) {
          val cp = popped
          popped = null
          return cp.getKey -> cp.getValue
        }
      }
    }
    val next = buffer.entrySet().iterator().next()
    buffer.remove(next.getKey)
    next.getKey -> next.getValue
  }
}
