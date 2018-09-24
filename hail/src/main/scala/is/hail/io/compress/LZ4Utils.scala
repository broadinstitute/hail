package is.hail.io.compress

import net.jpountz.lz4.LZ4Factory

object LZ4Utils {
  val factory = LZ4Factory.fastestInstance()
  val compressor = factory.highCompressor()
  val decompressor = factory.fastDecompressor()

  def maxCompressedLength(decompLen: Int): Int =
    compressor.maxCompressedLength(decompLen)

  def compress(comp: Array[Byte], compOff: Int, decomp: Array[Byte], decompLen: Int): Int = {
    val maxLen = maxCompressedLength(decompLen)
    assert(comp.length >= compOff + maxLen)
    val compressedLen = compressor.compress(decomp, 0, decompLen, comp, compOff, maxLen)
    compressedLen
  }

  def decompress(decomp: Array[Byte], decompOff: Int, decompLen: Int, comp: Array[Byte], compOff: Int, compLen: Int) {
    val compLen2 = decompressor.decompress(comp, compOff, decomp, decompOff, decompLen)
    assert(compLen2 == compLen)
  }
}
