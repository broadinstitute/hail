package is.hail.expr.types

import is.hail.annotations.{Region, UnsafeOrdering, _}
import is.hail.check.Arbitrary._
import is.hail.check.Gen
import is.hail.expr.LongNumericConversion
import is.hail.utils._

import scala.reflect.{ClassTag, _}

case object TInt64Optional extends TInt64(false)
case object TInt64Required extends TInt64(true)

class TInt64(override val required: Boolean) extends TIntegral {
  def _toString = "Int64"

  val conv = LongNumericConversion

  def _typeCheck(a: Any): Boolean = a.isInstanceOf[Long]

  override def genNonmissingValue: Gen[Annotation] = arbitrary[Long]

  override def scalaClassTag: ClassTag[java.lang.Long] = classTag[java.lang.Long]

  override def unsafeOrdering(missingGreatest: Boolean): UnsafeOrdering = new UnsafeOrdering {
    def compare(r1: Region, o1: Long, r2: Region, o2: Long): Int = {
      java.lang.Long.compare(r1.loadLong(o1), r2.loadLong(o2))
    }
  }

  val ordering: ExtendedOrdering =
    ExtendedOrdering.extendToNull(implicitly[Ordering[Long]])

  override def byteSize: Long = 8
}

object TInt64 {
  def apply(required: Boolean = false): TInt64 = if (required) TInt64Required else TInt64Optional

  def unapply(t: TInt64): Option[Boolean] = Option(t.required)
}
