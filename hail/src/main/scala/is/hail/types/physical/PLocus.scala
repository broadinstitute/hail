package is.hail.types.physical

import is.hail.asm4s._
import is.hail.expr.ir.EmitCodeBuilder
import is.hail.types.physical.stypes.interfaces.{SLocusCode, SLocusValue}
import is.hail.types.virtual.TLocus
import is.hail.variant._

abstract class PLocus extends PType {
  def rgBc: BroadcastRG

  lazy val virtualType: TLocus = TLocus(rgBc)

  def rg: ReferenceGenome

  def contig(value: Long): String

  def contigType: PString

  def position(value: Code[Long])(implicit line: LineNumber): Code[Int]

  def position(value: Long): Int

  def positionType: PInt32
}

abstract class PLocusValue extends PValue with SLocusValue

abstract class PLocusCode extends PCode with SLocusCode {
  def pt: PLocus

  def memoize(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PLocusValue

  def memoizeField(cb: EmitCodeBuilder, name: String)(implicit line: LineNumber): PLocusValue
}
