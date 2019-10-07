package is.hail.shuffler

import is.hail.annotations._
import is.hail.expr.types.physical._
import is.hail.io._
import is.hail.utils._
import java.io.{ ByteArrayOutputStream, ByteArrayInputStream }

final class Codec (
  spec: TypedCodecSpec
) {
  val (memType, buildDecoder) = spec.buildDecoder(spec._vType)
  val buildEncoder = spec.buildEncoder(memType)

  def encode(region: Region, offset: Long): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    using(buildEncoder(baos))(_.writeRegionValue(region, offset))
    baos.toByteArray
  }

  def decode(bytes: Array[Byte], region: Region): Long = {
    val bais = new ByteArrayInputStream(bytes)
    buildDecoder(bais).readRegionValue(region)
  }
}

