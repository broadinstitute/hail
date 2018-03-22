package is.hail.annotations

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import is.hail.expr.types._
import is.hail.io._
import is.hail.utils._
import is.hail.variant.{RGBase, Locus}
import org.apache.spark.sql.Row

object UnsafeIndexedSeq {
  def apply(t: TArray, elements: Array[RegionValue]): UnsafeIndexedSeq = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(t)
    rvb.startArray(elements.length)
    var i = 0
    while (i < elements.length) {
      rvb.addRegionValue(t.elementType, elements(i))
      i += 1
    }
    rvb.endArray()

    new UnsafeIndexedSeq(t, region, rvb.end())
  }

  def apply(t: TArray, a: IndexedSeq[Annotation]): UnsafeIndexedSeq = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(t)
    rvb.startArray(a.length)
    var i = 0
    while (i < a.length) {
      rvb.addAnnotation(t.elementType, a(i))
      i += 1
    }
    rvb.endArray()
    new UnsafeIndexedSeq(t, region, rvb.end())
  }

  def empty(t: TArray): UnsafeIndexedSeq = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(t)
    rvb.startArray(0)
    rvb.endArray()
    new UnsafeIndexedSeq(t, region, rvb.end())
  }
}

class UnsafeIndexedSeq(
  var t: TContainer,
  var region: Region, var aoff: Long) extends IndexedSeq[Annotation] with KryoSerializable {

  var length: Int = t.loadLength(region, aoff)

  def apply(i: Int): Annotation = {
    if (i < 0 || i >= length)
      throw new IndexOutOfBoundsException(i.toString)
    if (t.isElementDefined(region, aoff, i)) {
      UnsafeRow.read(t.elementType, region, t.loadElement(region, aoff, length, i))
    } else
      null
  }

  override def write(kryo: Kryo, output: Output) {
    kryo.writeObject(output, t)

    val aos = new ByteArrayOutputStream()
    val enc = CodecSpec.default.buildEncoder(aos)
    enc.writeRegionValue(t, region, aoff)
    enc.flush()

    val a = aos.toByteArray
    output.writeInt(a.length)
    output.write(a, 0, a.length)
  }

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(t)

    val aos = new ByteArrayOutputStream()
    val enc = CodecSpec.default.buildEncoder(aos)
    enc.writeRegionValue(t, region, aoff)
    enc.flush()

    val a = aos.toByteArray
    out.writeInt(a.length)
    out.write(a, 0, a.length)
  }

  override def read(kryo: Kryo, input: Input) {
    t = kryo.readObject(input, classOf[TArray])

    val smallInOff = input.readInt()
    val a = new Array[Byte](smallInOff)
    input.readFully(a, 0, smallInOff)
    using(CodecSpec.default.buildDecoder(new ByteArrayInputStream(a))) { dec =>

      region = Region()
      aoff = dec.readRegionValue(t, region)

      length = region.loadInt(aoff)
    }
  }

  private def readObject(in: ObjectInputStream) {
    t = in.readObject().asInstanceOf[TArray]

    val smallInOff = in.readInt()
    val a = new Array[Byte](smallInOff)
    in.readFully(a, 0, smallInOff)
    using(CodecSpec.default.buildDecoder(new ByteArrayInputStream(a))) { dec =>
      region = Region()
      aoff = dec.readRegionValue(t, region)

      length = region.loadInt(aoff)
    }
  }
}

object UnsafeRow {
  def readBinary(region: Region, boff: Long): Array[Byte] = {
    val binLength = TBinary.loadLength(region, boff)
    region.loadBytes(TBinary.bytesOffset(boff), binLength)
  }

  def readArray(t: TContainer, region: Region, aoff: Long): IndexedSeq[Any] =
    new UnsafeIndexedSeq(t, region, aoff)

  def readBaseStruct(t: TBaseStruct, region: Region, offset: Long): UnsafeRow =
    new UnsafeRow(t, region, offset)

  def readString(region: Region, boff: Long): String =
    new String(readBinary(region, boff))

  def readLocus(region: Region, offset: Long, rg: RGBase): Locus = {
    val ft = rg.locusType.fundamentalType.asInstanceOf[TStruct]
    Locus(
      readString(region, ft.loadField(region, offset, 0)),
      region.loadInt(ft.loadField(region, offset, 1)))
  }

  def read(t: Type, region: Region, offset: Long): Any = {
    t match {
      case _: TBoolean =>
        region.loadBoolean(offset)
      case _: TInt32 | _: TCall => region.loadInt(offset)
      case _: TInt64 => region.loadLong(offset)
      case _: TFloat32 => region.loadFloat(offset)
      case _: TFloat64 => region.loadDouble(offset)
      case t: TArray =>
        readArray(t, region, offset)
      case t: TSet =>
        readArray(t, region, offset).toSet
      case _: TString => readString(region, offset)
      case _: TBinary => readBinary(region, offset)
      case td: TDict =>
        val a = readArray(td, region, offset)
        a.asInstanceOf[IndexedSeq[Row]].map(r => (r.get(0), r.get(1))).toMap
      case t: TBaseStruct => readBaseStruct(t, region, offset)
      case x: TLocus => readLocus(region, offset, x.rg)
      case x: TInterval =>
        val ft = x.fundamentalType.asInstanceOf[TStruct]
        val start: Annotation =
          if (ft.isFieldDefined(region, offset, 0))
            read(x.pointType, region, ft.loadField(region, offset, 0))
          else
            null
        val end =
          if (ft.isFieldDefined(region, offset, 1))
            read(x.pointType, region, ft.loadField(region, offset, 1))
          else
            null
        val includesStart = read(TBooleanRequired, region, ft.loadField(region, offset, 2)).asInstanceOf[Boolean]
        val includesEnd = read(TBooleanRequired, region, ft.loadField(region, offset, 3)).asInstanceOf[Boolean]
        Interval(start, end, includesStart, includesEnd)
    }
  }
}

class UnsafeRow(var t: TBaseStruct,
  var region: Region, var offset: Long) extends Row with KryoSerializable {

  def this(t: TBaseStruct, rv: RegionValue) = this(t, rv.region, rv.offset)

  def this(t: TBaseStruct) = this(t, null, 0)

  def this() = this(null, null, 0)

  def set(newRegion: Region, newOffset: Long) {
    region = newRegion
    offset = newOffset
  }

  def set(rv: RegionValue): Unit = set(rv.region, rv.offset)

  def length: Int = t.size

  private def assertDefined(i: Int) {
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
  }

  def get(i: Int): Any = {
    if (isNullAt(i))
      null
    else
      UnsafeRow.read(t.types(i), region, t.loadField(region, offset, i))
  }

  def copy(): Row = new UnsafeRow(t, region, offset)

  override def getInt(i: Int): Int = {
    assertDefined(i)
    region.loadInt(t.loadField(region, offset, i))
  }

  override def getLong(i: Int): Long = {
    assertDefined(i)
    region.loadLong(t.loadField(region, offset, i))
  }

  override def getFloat(i: Int): Float = {
    assertDefined(i)
    region.loadFloat(t.loadField(region, offset, i))
  }

  override def getDouble(i: Int): Double = {
    assertDefined(i)
    region.loadDouble(t.loadField(region, offset, i))
  }

  override def getBoolean(i: Int): Boolean = {
    assertDefined(i)
    region.loadBoolean(t.loadField(region, offset, i))
  }

  override def getByte(i: Int): Byte = {
    assertDefined(i)
    region.loadByte(t.loadField(region, offset, i))
  }

  override def isNullAt(i: Int): Boolean = {
    if (i < 0 || i >= t.size)
      throw new IndexOutOfBoundsException(i.toString)
    !t.isFieldDefined(region, offset, i)
  }

  override def write(kryo: Kryo, output: Output) {
    output.writeBoolean(t.isInstanceOf[TStruct])
    kryo.writeObject(output, t)

    val aos = new ByteArrayOutputStream()
    val enc = CodecSpec.default.buildEncoder(aos)
    enc.writeRegionValue(t, region, offset)
    enc.flush()

    val a = aos.toByteArray
    output.writeInt(a.length)
    output.write(a, 0, a.length)
  }

  private def writeObject(out: ObjectOutputStream) {
    out.writeObject(t)

    val aos = new ByteArrayOutputStream()
    val enc = CodecSpec.default.buildEncoder(aos)
    enc.writeRegionValue(t, region, offset)
    enc.flush()

    val a = aos.toByteArray
    out.writeInt(a.length)
    out.write(a, 0, a.length)
  }

  override def read(kryo: Kryo, input: Input) {
    val isStruct = input.readBoolean()
    t = kryo.readObject(input, if (isStruct) classOf[TStruct] else classOf[TTuple])

    val smallInOff = input.readInt()
    val a = new Array[Byte](smallInOff)
    input.readFully(a, 0, smallInOff)
    using(CodecSpec.default.buildDecoder(new ByteArrayInputStream(a))) { dec =>
      region = Region()
      offset = dec.readRegionValue(t, region)
    }
  }

  private def readObject(in: ObjectInputStream) {
    t = in.readObject().asInstanceOf[TBaseStruct]

    val smallInOff = in.readInt()
    val a = new Array[Byte](smallInOff)
    in.readFully(a, 0, smallInOff)
    using(CodecSpec.default.buildDecoder(new ByteArrayInputStream(a))) { dec =>
      region = Region()
      offset = dec.readRegionValue(t, region)
    }
  }
}

class KeyedRow(var row: Row, keyFields: Array[Int]) extends Row {
  def length: Int = row.size
  def get(i: Int): Any = row.get(keyFields(i))
  def copy(): Row = new KeyedRow(row, keyFields)
  def set(newRow: Row): KeyedRow = {
    row = newRow
    this
  }
}
