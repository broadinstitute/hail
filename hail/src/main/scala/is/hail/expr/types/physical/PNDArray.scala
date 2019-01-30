package is.hail.expr.types.physical

import is.hail.annotations.CodeOrdering
import is.hail.expr.ir.EmitMethodBuilder
import is.hail.expr.types.virtual.TNDArray

final case class PNDArray(elementType: PType, override val required: Boolean = false) extends ComplexPType {
  lazy val virtualType: TNDArray = TNDArray(elementType.virtualType, required)

  val representation: PType = PStruct(
    "flags" -> PInt64Required, // record row vs column major
    "shape" -> PArray(PInt64Required), // length is ndim
    "offset" -> PInt64Required, // offset into data
    "strides" -> PArray(PInt64Required),
    "data" -> PArray(elementType)
  )
    
  def _toPretty = s"NDArray[$elementType]"

  def codeOrdering(mb: EmitMethodBuilder, other: PType): CodeOrdering = {
    assert(this isOfType other)
    representation.codeOrdering(mb)
  }
}
