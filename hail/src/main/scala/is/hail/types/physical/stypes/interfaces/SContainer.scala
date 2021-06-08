package is.hail.types.physical.stypes.interfaces

import is.hail.annotations.Region
import is.hail.asm4s._
import is.hail.expr.ir.{EmitCodeBuilder, IEmitCode}
import is.hail.types.physical.stypes.{EmitType, SCode, SType, SValue}

trait SContainer extends SType {
  def elementType: SType
  def elementEmitType: EmitType
  override def coerceOrCopy(cb: EmitCodeBuilder, region: Value[Region], value: SCode, deepCopy: Boolean): SIndexableCode
}

trait SIndexableValue extends SValue {
  def st: SContainer

  def loadLength(): Value[Int]

  def isElementMissing(i: Code[Int]): Code[Boolean]

  def isElementDefined(i: Code[Int]): Code[Boolean] = !isElementMissing(i)

  def loadElement(cb: EmitCodeBuilder, i: Code[Int]): IEmitCode

  def hasMissingValues(cb: EmitCodeBuilder): Code[Boolean]

  override def get: SIndexableCode

  def forEachDefined(cb: EmitCodeBuilder)(f: (EmitCodeBuilder, Value[Int], SCode) => Unit): Unit = {
    val length = loadLength()
    val idx = cb.newLocal[Int]("foreach_idx", 0)
    cb.whileLoop(idx < length, {

      loadElement(cb, idx).consume(cb,
        {}, /*do nothing if missing*/
        { eltCode =>
          f(cb, idx, eltCode)
        })
      cb.assign(idx, idx + 1)
    })
  }
}

trait SIndexableCode extends SCode {
  def st: SContainer

  def loadLength(): Code[Int]

  def memoize(cb: EmitCodeBuilder, name: String): SIndexableValue

  def memoizeField(cb: EmitCodeBuilder, name: String): SIndexableValue

  def castToArray(cb: EmitCodeBuilder): SIndexableCode

  override def castTo(cb: EmitCodeBuilder, region: Value[Region], destType: SType): SIndexableCode =
    castTo(cb, region, destType, false)
  override def castTo(cb: EmitCodeBuilder, region: Value[Region], destType: SType, deepCopy: Boolean): SIndexableCode =
    destType.asInstanceOf[SContainer].coerceOrCopy(cb, region, this, deepCopy)
}

