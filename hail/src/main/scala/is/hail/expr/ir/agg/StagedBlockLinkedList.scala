package is.hail.expr.ir.agg

import is.hail.annotations.{Region, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitTriplet, EmitRegion, EmitMethodBuilder}
import is.hail.expr.types.physical._
import is.hail.utils._

object StagedBlockLinkedList {
  val defaultBlockCap: Int = 64

  // src : Code[PointerTo[T]]
  // dst : Code[PointerTo[T]]
  private def deepCopy(er: EmitRegion, typ: PType, src: Code[Long], dst: Code[Long]): Code[Unit] =
    storeDeepCopy(er, typ, Region.loadIRIntermediate(typ)(src), dst)

  // v : Code[T]
  // dst : Code[PointerTo[T]]
  private def storeDeepCopy(er: EmitRegion, typ: PType, v: Code[_], dst: Code[Long]): Code[Unit] = {
    val ftyp = typ.fundamentalType
    if (ftyp.isPrimitive)
      Region.storePrimitive(ftyp, dst)(v)
    else
      StagedRegionValueBuilder.deepCopy(er, typ, coerce[Long](v), dst)
  }
}

class StagedBlockLinkedList(val elemType: PType, val mb: EmitMethodBuilder) {
  import StagedBlockLinkedList._

  val firstNode = mb.newField[Long]
  val lastNode = mb.newField[Long]
  val totalCount = mb.newField[Int]

  val storageType = PStruct(
    "firstNode" -> PInt64Required,
    "lastNode" -> PInt64Required,
    "totalCount" -> PInt32Required)

  def load(src: Code[Long]): Code[Unit] = Code(
    firstNode := Region.loadAddress(storageType.fieldOffset(src, 0)),
    lastNode := Region.loadAddress(storageType.fieldOffset(src, 1)),
    totalCount := Region.loadInt(storageType.fieldOffset(src, 2)))

  def store(dst: Code[Long]): Code[Unit] = Code(
    Region.storeAddress(storageType.fieldOffset(dst, 0), firstNode),
    Region.storeAddress(storageType.fieldOffset(dst, 1), lastNode),
    Region.storeInt(storageType.fieldOffset(dst, 2), totalCount))

  val i = mb.newField[Int]
  val p = mb.newField[Boolean]
  val tmpNode = mb.newField[Long]

  type Node = Code[Long]

  val bufferType = PArray(elemType.setRequired(false), required = true)

  val nodeType = PStruct(
    "buf" -> bufferType,
    "count" -> PInt32Required,
    "next" -> PInt64Optional)

  private def buffer(n: Node): Code[Long] =
    Region.loadAddress(nodeType.fieldOffset(n, 0))

  private[agg] def capacity(n: Node): Code[Int] =
    bufferType.loadLength(buffer(n))

  private def count(n: Node): Code[Int] =
    Region.loadInt(nodeType.fieldOffset(n, 1))

  private def incrCount(n: Node): Code[Unit] =
    Region.storeInt(nodeType.fieldOffset(n, 1), count(n) + 1)

  private def next(n: Node): Node =
    Region.loadAddress(nodeType.fieldOffset(n, 2))

  private def hasNext(n: Node): Code[Boolean] =
    nodeType.isFieldDefined(n, 2)

  private def setNext(n: Node, nNext: Node): Code[Unit] = Code(
    nodeType.setFieldPresent(n, 2),
    Region.storeAddress(nodeType.fieldOffset(n, 2), nNext))

  private def initNode(n: Node, buf: Code[Long], count: Code[Int]): Code[Unit] =
    Code(
      Region.storeAddress(nodeType.fieldOffset(n, 0), buf),
      Region.storeInt(nodeType.fieldOffset(n, 1), count),
      nodeType.setFieldMissing(n, 2))

  private def elemAddress(n: Node, i: Code[Int]): EmitTriplet =
    EmitTriplet(Code._empty,
      bufferType.isElementMissing(buffer(n), i),
      bufferType.elementOffset(buffer(n), capacity(n), i))

  private def pushPresent(n: Node, store: Code[Long] => Code[Unit]): Code[Unit] =
    Code(
      bufferType.setElementPresent(buffer(n), count(n)),
      store(bufferType.elementOffset(buffer(n), capacity(n), count(n))),
      incrCount(n))

  private def pushMissing(n: Node): Code[Unit] =
    Code(
      bufferType.setElementMissing(buffer(n), count(n)),
      incrCount(n))

  private def allocateNode(r: Code[Region], cap: Code[Int]): (Code[Unit], Node) = {
    val setup = Code(
      tmpNode := r.allocate(nodeType.alignment, nodeType.byteSize),
      initNode(tmpNode,
        buf = r.allocate(bufferType.contentsAlignment, bufferType.contentsByteSize(cap)),
        count = 0),
      bufferType.stagedInitialize(buffer(tmpNode), cap))
    (setup, tmpNode)
  }

  def initWithCapacity(r: Code[Region], initialCap: Code[Int]): Code[Unit] = {
    val (setup, n) = allocateNode(r, initialCap)
    Code(setup,
      firstNode := n,
      lastNode := n,
      totalCount := 0)
  }

  def init(r: Code[Region]): Code[Unit] =
    initWithCapacity(r, defaultBlockCap)

  private[agg] def pushNewBlockNode(r: Code[Region], cap: Code[Int]): Code[Unit] = {
    val (setup, newNode) = allocateNode(r, cap)
    Code(setup,
      setNext(lastNode, newNode),
      lastNode := newNode)
  }

  private[agg] def foreachNode(f: Node => Code[Unit]): Code[Unit] = {
    Code(
      tmpNode := firstNode,
      p := true,
      Code.whileLoop(p,
        f(tmpNode),
        p := hasNext(tmpNode),
        tmpNode := next(tmpNode)))
  }

  def foreachElemAddress(f: EmitTriplet => Code[Unit]): Code[Unit] = {
    foreachNode { n => Code(
      i := 0,
      Code.whileLoop(i < count(n),
        f(elemAddress(n, i)),
        i := i + 1))
    }
  }

  def push(r: Code[Region], elt: EmitTriplet): Code[Unit] = {
    val er = EmitRegion(mb, r)
    Code(
      (count(lastNode) >= capacity(lastNode)).orEmpty(
        pushNewBlockNode(r, defaultBlockCap)), // push a new block if lastNode is full
      elt.setup,
      elt.m.mux(
        pushMissing(lastNode),
        pushPresent(lastNode, storeDeepCopy(er, elemType, elt.v, _))),
      totalCount := totalCount + 1)
  }

  def pushFromAddress(r: Code[Region], addr: EmitTriplet): Code[Unit] =
    push(r, EmitTriplet(addr.setup, addr.m,
      Region.loadIRIntermediate(elemType)(addr.value)))

  def append(r: Code[Region], bll: StagedBlockLinkedList): Code[Unit] = {
    assert(bll.elemType.isOfType(elemType))
    bll.foreachElemAddress(pushFromAddress(r, _))
  }

  def appendShallow(r: Code[Region], atyp: PArray, aoff: Code[Long]): Code[Unit] = {
    assert(atyp.isOfType(bufferType))
    assert(!atyp.elementType.required)
    val len = atyp.loadLength(r, aoff)
    Code(
      tmpNode := r.allocate(nodeType.alignment, nodeType.byteSize),
      initNode(tmpNode,
        buf = aoff,
        count = len),
      setNext(lastNode, tmpNode),
      lastNode := tmpNode,
      totalCount := totalCount + len)
  }

  def writeToSRVB(srvb: StagedRegionValueBuilder): Code[Unit] = {
    val er = EmitRegion(mb, srvb.region)
    assert(srvb.typ.isOfType(bufferType))
    Code(
      srvb.start(totalCount, init = true),
      foreachElemAddress { addr =>
        Code(
          addr.m.mux(
            srvb.setMissing(),
            deepCopy(er, elemType, addr.value, srvb.currentOffset)),
          srvb.advance())
      })
  }

  def toArray(er: EmitRegion): Code[Long] = {
    val srvb = new StagedRegionValueBuilder(er, bufferType)
    Code(writeToSRVB(srvb), srvb.end())
  }
}
