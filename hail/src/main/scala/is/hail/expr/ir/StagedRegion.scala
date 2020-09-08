package is.hail.expr.ir

import is.hail.annotations.{Region, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.types.physical.{PCode, PType}
import is.hail.utils._

object StagedRegion {
  def apply(
    r: Value[Region],
    allowSubregions: Boolean = false,
    parent: Option[ParentStagedRegion] = None,
    description: String = "root"
  ): ParentStagedRegion =
    new ParentStagedRegion(r, parent, allowSubregions, description)

  def apply(r: Value[Region]): StagedRegion = new StagedRegion {
    val code = r
  }

  def swap(mb: EmitMethodBuilder[_], x: OwnedStagedRegion, y: OwnedStagedRegion): Code[Unit] = {
    x.parent assertEqual y.parent
    (x, y) match {
      case (x: RealOwnedStagedRegion, y: RealOwnedStagedRegion) =>
        val temp = mb.newLocal[Region]("sr_swap")
        Code(temp := x.r, x.r := y.r, y.r := temp)
      case (x: DummyOwnedStagedRegion, y: DummyOwnedStagedRegion) =>
        Code._empty
    }
  }
}

abstract class StagedRegion {
  def code: Value[Region]

  final def asParent(allowAllocations: Boolean, description: String): ParentStagedRegion = {
    val parent = this match {
      case child: ChildStagedRegion => Some(child.parent)
      case _ => None
    }
    StagedRegion(code, allowAllocations, parent, description)
  }
}

class ParentStagedRegion(
  val code: Value[Region],
  val parent: Option[ParentStagedRegion],
  val allowSubregions: Boolean,
  desc: String
) extends StagedRegion { self =>
  final def description: String = parent match {
    case Some(p) => s"$desc < ${ p.description }"
    case None => desc
  }

  def createChildRegion(mb: EmitMethodBuilder[_]): OwnedStagedRegion =
    if (allowSubregions) {
      val newR = mb.genFieldThisRef[Region]("staged_region_child")
      new RealOwnedStagedRegion(newR, this)
    } else {
      new DummyOwnedStagedRegion(code, this)
    }

  def createChildRegionArray(mb: EmitMethodBuilder[_], length: Int): OwnedStagedRegionArray =
    if (allowSubregions) {
      val regionArray = mb.genFieldThisRef[Array[Region]]("staged_region_child_array")

      def get(i: Value[Int]): Settable[Region] = new Settable[Region] {
        def get: Code[Region] = regionArray(i)

        def store(rhs: Code[Region]): Code[Unit] = regionArray.update(i, rhs)
      }

      new OwnedStagedRegionArray {
        def apply(i: Value[Int]): OwnedStagedRegion = new RealOwnedStagedRegion(get(i), self)

        def allocateRegions(mb: EmitMethodBuilder[_], size: Int): Code[Unit] = {
          val i = mb.newLocal[Int]("sora_alloc_i")
          Code(
            regionArray := Code.newArray(length),
            Code.forLoop(i := 0, i < length, i := i + 1, apply(i).allocateRegion(size)))
        }

        def freeAll(mb: EmitMethodBuilder[_]): Code[Unit] = {
          val i = mb.newLocal[Int]("sora_free_i")
          Code(
            Code.forLoop(i := 0, i < length, i := i + 1, apply(i).free()),
            regionArray := Code._null)
        }
      }
    } else {
      new OwnedStagedRegionArray {
        def apply(i: Value[Int]): OwnedStagedRegion = new DummyOwnedStagedRegion(code, self)

        def allocateRegions(mb: EmitMethodBuilder[_], size: Int): Code[Unit] =
          Code._empty

        def freeAll(mb: EmitMethodBuilder[_]): Code[Unit] =
          Code._empty
      }
    }

  override def equals(that: Any): Boolean = that match {
    case that: ParentStagedRegion =>
      (this.allowSubregions == that.allowSubregions) && (this.code eq that.code)
    case _ => false
  }

  final def <=(that: ParentStagedRegion): Boolean =
    (this == that) || parent.exists(_ <= that)

  def assertEqual(that: ParentStagedRegion) {
    assert(this == that, s"${this.description}\n${that.description}")
  }

  def assertSubRegion(that: ParentStagedRegion) {
    assert(this <= that, s"${this.description}\n${that.description}")
  }
}

abstract class ChildStagedRegion extends StagedRegion {
  def parent: ParentStagedRegion

  def otherAncestors: Seq[ParentStagedRegion]

  def asSubregionOf(that: ParentStagedRegion): ChildStagedRegion

  final def createSiblingRegion(mb: EmitMethodBuilder[_]): OwnedStagedRegion =
    parent.createChildRegion(mb)

  final def createSiblingRegionArray(mb: EmitMethodBuilder[_], length: Int): OwnedStagedRegionArray =
    parent.createChildRegionArray(mb, length)

  final def copyToParent(mb: EmitMethodBuilder[_], value: PCode, destType: PType): PCode =
    copyToParentOrSibling(mb, value, parent, destType)

  final def copyToParent(mb: EmitMethodBuilder[_], value: PCode): PCode =
    copyToParentOrSibling(mb, value, parent)

  final def copyTo(mb: EmitMethodBuilder[_], value: PCode, dest: ChildStagedRegion, destType: PType): PCode = {
    parent assertEqual dest.parent
    copyToParentOrSibling(mb, value, dest, destType)
  }

  final def copyTo(mb: EmitMethodBuilder[_], value: PCode, dest: ChildStagedRegion): PCode = {
    parent assertEqual dest.parent
    copyToParentOrSibling(mb, value, dest)
  }

  def assertSubRegion(that: ParentStagedRegion) {
    assert((this.parent <= that) || otherAncestors.exists(_ <= that),
           s"${this.parent.description}\n${that.description}")
  }

  protected def copyToParentOrSibling(mb: EmitMethodBuilder[_], value: PCode, dest: StagedRegion, destType: PType): PCode

  protected def copyToParentOrSibling(mb: EmitMethodBuilder[_], value: PCode, dest: StagedRegion): PCode
}

trait OwnedStagedRegion extends ChildStagedRegion {
  def allocateRegion(size: Int): Code[Unit]

  def free(): Code[Unit]

  def clear(): Code[Unit]

  def giveToParent(): Code[Unit]

  def giveToSibling(dest: ChildStagedRegion): Code[Unit]

  def shareWithSibling(dest: ChildStagedRegion): Code[Unit]

  def addToParentRVB(srvb: StagedRegionValueBuilder, value: PCode): Code[Unit]
}

abstract class OwnedStagedRegionArray {
  def apply(i: Value[Int]): OwnedStagedRegion

  def allocateRegions(mb: EmitMethodBuilder[_], size: Int): Code[Unit]

  def freeAll(mb: EmitMethodBuilder[_]): Code[Unit]
}

class RealOwnedStagedRegion(
  val r: Settable[Region],
  val parent: ParentStagedRegion,
  val otherAncestors: Seq[ParentStagedRegion] = Seq()
) extends OwnedStagedRegion {
  assert(parent.allowSubregions)

  def code: Value[Region] = r

  def asSubregionOf(that: ParentStagedRegion): ChildStagedRegion =
    new RealOwnedStagedRegion(r, parent, otherAncestors :+ that)

  def allocateRegion(size: Int): Code[Unit] = r := Region.stagedCreate(size)

  def free(): Code[Unit] = Code(r.invalidate(), r := Code._null)

  def clear(): Code[Unit] = (r: Value[Region]).clear()

  def giveToParent(): Code[Unit] = r.invoke[Region, Unit]("move", parent.code)

  def giveToSibling(dest: ChildStagedRegion): Code[Unit] = {
    dest assertSubRegion  parent
    r.invoke[Region, Unit]("move", dest.code)
  }

  def shareWithSibling(dest: ChildStagedRegion): Code[Unit] = {
    dest assertSubRegion parent
    dest.code.invoke[Region, Unit]("addReferenceTo", r)
  }

  def addToParentRVB(srvb: StagedRegionValueBuilder, value: PCode): Code[Unit] =
    srvb.addIRIntermediate(value, deepCopy = true)

  protected def copyToParentOrSibling(mb: EmitMethodBuilder[_], value: PCode, dest: StagedRegion, destType: PType): PCode =
    value.copyToRegion(mb, dest.code, destType)

  protected def copyToParentOrSibling(mb: EmitMethodBuilder[_], value: PCode, dest: StagedRegion): PCode =
    copyToParentOrSibling(mb, value, dest, value.pt)
}

class DummyOwnedStagedRegion(
  val code: Value[Region],
  val parent: ParentStagedRegion,
  val otherAncestors: Seq[ParentStagedRegion] = Seq()
) extends OwnedStagedRegion {
  assert(!parent.allowSubregions)

  def asSubregionOf(that: ParentStagedRegion): ChildStagedRegion =
    new DummyOwnedStagedRegion(code, parent, otherAncestors :+ that)

  def allocateRegion(size: Int): Code[Unit] = Code._empty

  def free(): Code[Unit] = Code._empty

  def clear(): Code[Unit] = Code._empty

  def giveToParent(): Code[Unit] = Code._empty

  def giveToSibling(dest: ChildStagedRegion): Code[Unit] = {
    dest assertSubRegion parent
    Code._empty
  }

  def shareWithSibling(dest: ChildStagedRegion): Code[Unit] = {
    dest assertSubRegion parent
    Code._empty
  }

  def addToParentRVB(srvb: StagedRegionValueBuilder, value: PCode): Code[Unit] =
    srvb.addIRIntermediate(value, deepCopy = false)

  protected def copyToParentOrSibling(mb: EmitMethodBuilder[_], value: PCode, dest: StagedRegion, destType: PType): PCode =
    value.castTo(mb, parent.code, destType)

  protected def copyToParentOrSibling(mb: EmitMethodBuilder[_], value: PCode, dest: StagedRegion): PCode =
    value
}
