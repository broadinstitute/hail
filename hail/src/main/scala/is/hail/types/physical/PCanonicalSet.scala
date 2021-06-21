package is.hail.types.physical

import is.hail.annotations.{Annotation, Region}
import is.hail.expr.ir.EmitCodeBuilder
import is.hail.types.physical.stypes.concrete.{SIndexablePointer, SIndexablePointerCode}
import is.hail.types.physical.stypes.interfaces.SIndexableCode
import is.hail.types.virtual.{TSet, Type}
import is.hail.utils._

object PCanonicalSet {
  def coerceArrayCode(cb: EmitCodeBuilder, contents: SIndexableCode): SIndexableCode = {
    contents.st match {
      case SIndexablePointer(PCanonicalArray(elt, r)) =>
        PCanonicalSet(elt, r).sType.fromCodes(contents.makeCodeTuple(cb))
    }
  }
}

final case class PCanonicalSet(elementType: PType,  required: Boolean = false) extends PSet with PArrayBackedContainer {
  val arrayRep = PCanonicalArray(elementType, required)

  override def setRequired(required: Boolean): PCanonicalSet = if (required == this.required) this else PCanonicalSet(elementType, required)

  def _asIdent = s"set_of_${elementType.asIdent}"

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean = false) {
    sb.append("PCSet[")
    elementType.pretty(sb, indent, compact)
    sb.append("]")
  }

  override def deepRename(t: Type) = deepRenameSet(t.asInstanceOf[TSet])

  private def deepRenameSet(t: TSet) =
    PCanonicalSet(this.elementType.deepRename(t.elementType), this.required)

  override def unstagedStoreJavaObject(annotation: Annotation, region: Region): Long = {
    val s: IndexedSeq[Annotation] = annotation.asInstanceOf[Set[Annotation]]
      .toFastIndexedSeq
      .sorted(elementType.virtualType.ordering.toOrdering)
    arrayRep.unstagedStoreJavaObject(s, region)
  }
}
