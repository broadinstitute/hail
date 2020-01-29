package is.hail.compatibility

import is.hail.HailContext
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.ir.ExecuteContext
import is.hail.expr.types.encoded._
import is.hail.expr.types.physical.{PArray, PField, PInt64, PStruct, PType}
import is.hail.expr.types.virtual._
import is.hail.io._
import is.hail.rvd.{AbstractRVDSpec, IndexSpec2, IndexedRVDSpec2, RVD, RVDPartitioner}
import is.hail.utils.{FastIndexedSeq, Interval}
import org.json4s.JValue

case class IndexSpec private(
  relPath: String,
  keyType: String,
  annotationType: String,
  offsetField: Option[String]
) {
  val baseSpec = LEB128BufferSpec(
    BlockingBufferSpec(32 * 1024,
      LZ4BlockBufferSpec(32 * 1024,
        new StreamBlockBufferSpec)))

  val (keyVType, keyEType) = LegacyEncodedTypeParser.parseTypeAndEType(keyType)
  val (annotationVType, annotationEType) = LegacyEncodedTypeParser.parseTypeAndEType(annotationType)

  val leafEType = EBaseStruct(FastIndexedSeq(
    EField("first_idx", EInt64Required, 0),
    EField("keys", EArray(EBaseStruct(FastIndexedSeq(
      EField("key", keyEType, 0),
      EField("offset", EInt64Required, 1),
      EField("annotation", annotationEType, 2)
    ), required = true), required = true), 1)
  ))
  val leafPType = PStruct(FastIndexedSeq(
    PField("first_idx", PInt64(true), 0),
    PField("keys", PArray(PStruct(FastIndexedSeq(
      PField("key", PType.canonical(keyVType), 0),
      PField("offset", PInt64(true), 1),
      PField("annotation", PType.canonical(annotationVType), 2)
    ), required = true), required = true), 1)))

  val internalNodeEType = EBaseStruct(FastIndexedSeq(
    EField("children", EArray(EBaseStruct(FastIndexedSeq(
      EField("index_file_offset", EInt64Required, 0),
      EField("first_idx", EInt64Required, 1),
      EField("first_key", keyEType, 2),
      EField("first_record_offset", EInt64Required, 3),
      EField("first_annotation", annotationEType, 4)
    ), required = true), required = true), 0)
  ))

  val internalNodePType = PStruct(FastIndexedSeq(
    PField("children", PArray(PStruct(FastIndexedSeq(
      PField("index_file_offset", PInt64(true), 0),
      PField("first_idx", PInt64(true), 1),
      PField("first_key", PType.canonical(keyVType), 2),
      PField("first_record_offset", PInt64(true), 3),
      PField("first_annotation", PType.canonical(annotationVType), 4)
    ), required = true), required = true), 0)
  ))


  val leafCodec: AbstractTypedCodecSpec = TypedCodecSpec(leafEType, leafPType, baseSpec)
  val internalNodeCodec: AbstractTypedCodecSpec = TypedCodecSpec(internalNodeEType, internalNodePType, baseSpec)

  def toIndexSpec2: IndexSpec2 = IndexSpec2(
    relPath, leafCodec, internalNodeCodec, keyVType, annotationVType, offsetField
  )
}

case class PackCodecSpec private(child: BufferSpec)

case class LegacyRVDType(rowType: TStruct, rowEType: EType, key: IndexedSeq[String]) {
  def keyType: TStruct = rowType.select(key)._1
}

trait ShimRVDSpec extends AbstractRVDSpec {

  val shim: AbstractRVDSpec

  final def key: IndexedSeq[String] = shim.key

  override def partitioner: RVDPartitioner = shim.partitioner

  override def read(
    hc: HailContext,
    path: String,
    requestedType: TStruct,
    ctx: ExecuteContext,
    newPartitioner: Option[RVDPartitioner],
    filterIntervals: Boolean
  ): RVD = shim.read(hc, path, requestedType, ctx, newPartitioner, filterIntervals)

  override def typedCodecSpec: AbstractTypedCodecSpec = shim.typedCodecSpec

  override def partFiles: Array[String] = shim.partFiles

  override lazy val indexed: Boolean = shim.indexed

  lazy val attrs: Map[String, String] = shim.attrs
}

case class IndexedRVDSpec private(
  rvdType: String,
  codecSpec: PackCodecSpec,
  indexSpec: IndexSpec,
  override val partFiles: Array[String],
  jRangeBounds: JValue
) extends ShimRVDSpec {
  private val lRvdType = LegacyEncodedTypeParser.parseLegacyRVDType(rvdType)

  lazy val shim = IndexedRVDSpec2(lRvdType.key,
    TypedCodecSpec(lRvdType.rowEType, PType.canonical(lRvdType.rowType), codecSpec.child),
    indexSpec.toIndexSpec2, partFiles, jRangeBounds, Map.empty[String, String])
}

case class UnpartitionedRVDSpec private(
  rowType: String,
  codecSpec: PackCodecSpec,
  partFiles: Array[String]
) extends AbstractRVDSpec {
  private val (rowVType: TStruct, rowEType) = LegacyEncodedTypeParser.parseTypeAndEType(rowType)

  def partitioner: RVDPartitioner = RVDPartitioner.unkeyed(partFiles.length)

  def key: IndexedSeq[String] = FastIndexedSeq()

  def typedCodecSpec: AbstractTypedCodecSpec = TypedCodecSpec(rowEType, PType.canonical(rowVType), codecSpec.child)

  val attrs: Map[String, String] = Map.empty
}

case class OrderedRVDSpec private(
  rvdType: String,
  codecSpec: PackCodecSpec,
  partFiles: Array[String],
  jRangeBounds: JValue
) extends AbstractRVDSpec {
  private val lRvdType = LegacyEncodedTypeParser.parseLegacyRVDType(rvdType)

  def key: IndexedSeq[String] = lRvdType.key

  def partitioner: RVDPartitioner = {
    val rangeBoundsType = TArray(TInterval(lRvdType.keyType))
    new RVDPartitioner(lRvdType.keyType,
      JSONAnnotationImpex.importAnnotation(jRangeBounds, rangeBoundsType, padNulls = false).asInstanceOf[IndexedSeq[Interval]])
  }

  override def typedCodecSpec: AbstractTypedCodecSpec = TypedCodecSpec(lRvdType.rowEType, PType.canonical(lRvdType.rowType), codecSpec.child)

  val attrs: Map[String, String] = Map.empty
}

