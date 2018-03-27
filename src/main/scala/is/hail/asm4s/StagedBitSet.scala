package is.hail.asm4s

import org.objectweb.asm.tree._
import scala.collection.generic.Growable

class StagedBitSet(fb: FunctionBuilder[_]) {
  private var used = 0
  private var bits: LocalRef[Long] = null
  private var count = 0

  def newBit(): SettableBit = {
    if (used >= 64 || bits == null) {
      bits = fb.newLocal[Long](s"settable$count")
      count += 1
      fb.emit(bits.store(0L))
      used = 0
    }

    used += 1
    new SettableBit(bits, used - 1)
  }
}

class SettableBit(bits: LocalRef[Long], i: Int) extends Settable[Boolean] {
  assert(i >= 0)
  assert(i < 64)

  def store(b: Code[Boolean]): Code[Unit] = {
    bits := bits & ~(1L << i) | (b.toI.toL << i)
  }

  def load(): Code[Boolean] = (bits >> i & 1L).toI.toZ
}
