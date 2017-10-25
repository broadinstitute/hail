package is.hail.expr

import is.hail.annotations.Annotation
import is.hail.utils.{Interval, _}
import is.hail.variant.{AltAllele, GenomeReference, Genotype, Locus, Variant}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.{JsonMethods, Serialization}

abstract class AnnotationImpex[T, A] {
  // FIXME for now, schema must be specified on import
  def exportType(t: Type): T

  def exportAnnotation(a: Annotation, t: Type): A

  def importAnnotation(a: A, t: Type): Annotation
}

object SparkAnnotationImpex extends AnnotationImpex[DataType, Any] {
  val invalidCharacters: Set[Char] = " ,;{}()\n\t=".toSet

  def escapeColumnName(name: String): String = {
    name.map { c =>
      if (invalidCharacters.contains(c))
        "_"
      else
        c
    }.mkString
  }

  def requiresConversion(t: Type): Boolean = t match {
    case TArray(elementType, _) => requiresConversion(elementType)
    case TSet(_, _) | TDict(_, _, _) | TGenotype | TAltAllele | TVariant(_) | TLocus(_) | TInterval(_) => true
    case TStruct(fields) =>
      fields.isEmpty || fields.exists(f => requiresConversion(f.typ))
    case _ => false
  }

  def importType(t: DataType): Type = t match {
    case BooleanType => TBoolean
    case IntegerType => TInt32
    case LongType => TInt64
    case FloatType => TFloat32
    case DoubleType => TFloat64
    case StringType => TString
    case BinaryType => TBinary
    case ArrayType(elementType, _) => TArray(importType(elementType))
    case StructType(fields) =>
      TStruct(fields.zipWithIndex
        .map { case (f, i) =>
          (f.name, importType(f.dataType))
        }: _*)
  }

  def annotationImporter(t: Type): (Any) => Annotation = {
    if (requiresConversion(t))
      (a: Any) => importAnnotation(a, t)
    else
      (a: Any) => a
  }

  def importAnnotation(a: Any, t: Type): Annotation = {
    if (a == null)
      null
    else
      t match {
        case TArray(elementType, req) =>
          a.asInstanceOf[Seq[_]].map(elem => {
            assert(!req || elem != null)
            importAnnotation(elem, elementType)
          })
        case TSet(elementType, req) =>
          a.asInstanceOf[Seq[_]].map(elem => {
            assert(!req || elem != null)
            importAnnotation(elem, elementType)
          }).toSet
        case TDict(keyType, valueType, req) =>
          val kvPairs = a.asInstanceOf[IndexedSeq[Annotation]]
          kvPairs
            .map(_.asInstanceOf[Row])
            .map(r => {
              assert(!req || r.get(0) != null)
              (importAnnotation(r.get(0), keyType), importAnnotation(r.get(1), valueType))
            })
            .toMap
        case TGenotype =>
          val r = a.asInstanceOf[Row]
          Genotype(Option(r.get(0)).map(_.asInstanceOf[Int]),
            Option(r.get(1)).map(_.asInstanceOf[Seq[Int]].toArray),
            Option(r.get(2)).map(_.asInstanceOf[Int]),
            Option(r.get(3)).map(_.asInstanceOf[Int]),
            Option(r.get(4)).map(_.asInstanceOf[Seq[Int]].toArray),
            r.get(5).asInstanceOf[Boolean])
        case TAltAllele =>
          val r = a.asInstanceOf[Row]
          AltAllele(r.getAs[String](0), r.getAs[String](1))
        case _: TVariant =>
          val r = a.asInstanceOf[Row]
          Variant(r.getAs[String](0), r.getAs[Int](1), r.getAs[String](2),
            r.getAs[Seq[Row]](3).map(aa =>
              importAnnotation(aa, TAltAllele).asInstanceOf[AltAllele]).toArray)
        case _: TLocus =>
          val r = a.asInstanceOf[Row]
          Locus(r.getAs[String](0), r.getAs[Int](1))
        case x: TInterval =>
          val r = a.asInstanceOf[Row]
          Interval(importAnnotation(r.get(0), TLocus(x.gr)).asInstanceOf[Locus], importAnnotation(r.get(1), TLocus(x.gr)).asInstanceOf[Locus])
        case TStruct(fields) =>
          if (fields.isEmpty)
            if (a.asInstanceOf[Boolean]) Annotation.empty else null
          else {
            val r = a.asInstanceOf[Row]
            Annotation.fromSeq(r.toSeq.zip(fields).map { case (v, f) =>
              importAnnotation(v, f.typ)
            })
          }
        case _ => a
      }
  }

  def exportType(t: Type): DataType = (t: @unchecked) match {
    case TBoolean => BooleanType
    case TInt32 => IntegerType
    case TInt64 => LongType
    case TFloat32 => FloatType
    case TFloat64 => DoubleType
    case TString => StringType
    case TBinary => BinaryType
    case TArray(elementType, _) => ArrayType(exportType(elementType))
    case TSet(elementType, _) => ArrayType(exportType(elementType))
    case TDict(keyType, valueType, _) =>
      ArrayType(StructType(Array(
        StructField("key", keyType.schema),
        StructField("value", valueType.schema))))
    case TAltAllele => AltAllele.sparkSchema
    case _: TVariant => Variant.sparkSchema
    case _: TLocus => Locus.sparkSchema
    case _: TInterval => StructType(Array(
      StructField("start", Locus.sparkSchema, nullable = false),
      StructField("end", Locus.sparkSchema, nullable = false)))
    case TGenotype => Genotype.sparkSchema
    case TCall => IntegerType
    case TStruct(fields) =>
      if (fields.isEmpty)
        BooleanType //placeholder
      else
        StructType(fields
          .map(f =>
            StructField(escapeColumnName(f.name), f.typ.schema)))
  }

  def annotationExporter(t: Type): (Annotation) => Any = {
    if (requiresConversion(t))
      (a: Annotation) => exportAnnotation(a, t)
    else
      (a: Annotation) => a
  }

  def exportAnnotation(a: Annotation, t: Type): Any = {
    if (a == null)
      null
    else
      t match {
        case TArray(elementType, req) =>
          a.asInstanceOf[IndexedSeq[_]].map(elem => {
            assert(!req || elem != null)
            exportAnnotation(elem, elementType)
          })
        case TSet(elementType, req) =>
          a.asInstanceOf[Set[_]].toSeq.map(elem => {
            assert(!req || elem != null)
            exportAnnotation(elem, elementType)
          })
        case TDict(keyType, valueType, req) =>
          a.asInstanceOf[Map[_, _]]
            .map { case (k, v) => {
              assert(!req || k != null)
              Row.fromSeq(Seq(exportAnnotation(k, keyType), exportAnnotation(v, valueType)))
            }
            }.toIndexedSeq
        case TGenotype =>
          Genotype.toRow(a.asInstanceOf[Genotype])
        case TAltAllele =>
          val aa = a.asInstanceOf[AltAllele]
          Row(aa.ref, aa.alt)
        case TVariant(gr) =>
          val v = a.asInstanceOf[Variant]
          Row(v.contig, v.start, v.ref, v.altAlleles.map(aa => Row(aa.ref, aa.alt)))
        case TLocus(gr) =>
          val l = a.asInstanceOf[Locus]
          Row(l.contig, l.position)
        case TInterval(gr) =>
          val i = a.asInstanceOf[Interval[_]]
          Row(exportAnnotation(i.start, TLocus(gr)), exportAnnotation(i.end, TLocus(gr)))
        case TStruct(fields) =>
          if (fields.isEmpty)
            a != null
          else {
            val r = a.asInstanceOf[Row]
            Annotation.fromSeq(r.toSeq.zip(fields).map {
              case (v, f) => exportAnnotation(v, f.typ)
            })
          }
        case _ => a
      }
  }
}

case class JSONExtractGenotype(
  gt: Option[Int],
  ad: Option[Array[Int]],
  dp: Option[Int],
  gq: Option[Int],
  px: Option[Array[Int]],
  fakeRef: Boolean) {
  def toGenotype =
    Genotype(gt, ad, dp, gq, px, fakeRef)
}

case class JSONExtractVariant(contig: String,
  start: Int,
  ref: String,
  altAlleles: List[AltAllele]) {
  def toVariant =
    Variant(contig, start, ref, altAlleles.toArray)
}

case class JSONExtractInterval(start: Locus, end: Locus) {
  def toInterval = Interval(start, end)
}

case class JSONExtractContig(name: String, length: Int)

case class JSONExtractGenomeReference(name: String, contigs: Array[JSONExtractContig], xContigs: Set[String],
  yContigs: Set[String], mtContigs: Set[String], par: Array[JSONExtractInterval]) {

  def toGenomeReference: GenomeReference = GenomeReference(name, contigs.map(_.name),
    contigs.map(c => (c.name, c.length)).toMap, xContigs, yContigs, mtContigs, par.map(_.toInterval))
}

object JSONAnnotationImpex extends AnnotationImpex[Type, JValue] {
  def jsonExtractVariant(t: Type, variantFields: String): Any => Variant = {
    val ec = EvalContext(Map(
      "root" -> (0, t)))

    val (types, f) = Parser.parseExprs(variantFields, ec)

    if (types.length != 4)
      fatal(s"wrong number of variant field expressions: expected 4, got ${ types.length }")

    if (types(0) != TString)
      fatal(s"wrong type for chromosome field: expected String, got ${ types(0) }")
    if (types(1) != TInt32)
      fatal(s"wrong type for pos field: expected Int, got ${ types(1) }")
    if (types(2) != TString)
      fatal(s"wrong type for ref field: expected String, got ${ types(2) }")
    if (types(3) != TArray(TString))
      fatal(s"wrong type for alt field: expected Array[String], got ${ types(3) }")

    (root: Annotation) => {
      ec.setAll(root)

      val vfs = f()

      val chr = vfs(0)
      val pos = vfs(1)
      val ref = vfs(2)
      val alt = vfs(3)

      if (chr != null && pos != null && ref != null && alt != null)
        Variant(chr.asInstanceOf[String],
          pos.asInstanceOf[Int],
          ref.asInstanceOf[String],
          alt.asInstanceOf[IndexedSeq[String]].toArray)
      else
        null
    }
  }

  def jsonExtractSample(t: Type, sampleExpr: String): Any => String = {
    val ec = EvalContext(Map(
      "root" -> (0, t)))

    val f: () => String = Parser.parseTypedExpr[String](sampleExpr, ec)

    (root: Annotation) => {
      ec.setAll(root)
      f()
    }
  }

  def exportType(t: Type): Type = t

  def exportAnnotation(a: Annotation, t: Type): JValue =
    if (a == null)
      JNull
    else {
      (t: @unchecked) match {
        case TBoolean => JBool(a.asInstanceOf[Boolean])
        case TInt32 => JInt(a.asInstanceOf[Int])
        case TInt64 => JInt(a.asInstanceOf[Long])
        case TFloat32 => JDouble(a.asInstanceOf[Float])
        case TFloat64 => JDouble(a.asInstanceOf[Double])
        case TString => JString(a.asInstanceOf[String])
        case TArray(elementType, req) =>
          val arr = a.asInstanceOf[Seq[Any]]
          JArray(arr.map(elem => {
            assert(!req || elem != null)
            exportAnnotation(elem, elementType)
          }).toList)
        case TSet(elementType, req) =>
          val arr = a.asInstanceOf[Set[Any]]
          JArray(arr.map(elem => {
            assert(!req || elem != null)
            exportAnnotation(elem, elementType)
          }).toList)
        case TDict(keyType, valueType, req) =>
          val m = a.asInstanceOf[Map[_, _]]
          JArray(m.map { case (k, v) => {
            assert(!req || k != null)
            JObject(
              "key" -> exportAnnotation(k, keyType),
              "value" -> exportAnnotation(v, valueType))
          }
          }.toList)
        case TCall => JInt(a.asInstanceOf[Int])
        case TGenotype => Genotype.toJSON(a.asInstanceOf[Genotype])
        case TAltAllele => a.asInstanceOf[AltAllele].toJSON
        case TVariant(_) => a.asInstanceOf[Variant].toJSON
        case TLocus(_) => a.asInstanceOf[Locus].toJSON
        case TInterval(gr) => a.asInstanceOf[Interval[Locus]].toJSON(TLocus(gr).toJSON(_))
        case TStruct(fields) =>
          val row = a.asInstanceOf[Row]
          JObject(fields
            .map(f => (f.name, exportAnnotation(row.get(f.index), f.typ)))
            .toList)
      }
    }

  def importAnnotation(jv: JValue, t: Type): Annotation =
    importAnnotation(jv, t, "<root>")

  def importAnnotation(jv: JValue, t: Type, parent: String): Annotation = {
    implicit val formats = Serialization.formats(NoTypeHints)

    (jv, t) match {
      case (JNull | JNothing, _) => null
      case (JInt(x), TInt32) => x.toInt
      case (JInt(x), TInt64) => x.toLong
      case (JInt(x), TFloat64) => x.toDouble
      case (JInt(x), TString) => x.toString
      case (JDouble(x), TFloat64) => x
      case (JString("Infinity"), TFloat64) => Double.PositiveInfinity
      case (JString("-Infinity"), TFloat64) => Double.NegativeInfinity
      case (JString("Infinity"), TFloat32) => Float.PositiveInfinity
      case (JString("-Infinity"), TFloat32) => Float.NegativeInfinity
      case (JDouble(x), TFloat32) => x.toFloat
      case (JString(x), TString) => x
      case (JString(x), TInt32) =>
        x.toInt
      case (JString(x), TFloat64) =>
        if (x.startsWith("-:"))
          x.drop(2).toDouble
        else
          x.toDouble
      case (JBool(x), TBoolean) => x

      // back compatibility
      case (JObject(a), TDict(TString, valueType, _)) =>
        a.map { case (key, value) =>
          (key, importAnnotation(value, valueType, parent))
        }
          .toMap

      case (JArray(arr), TDict(keyType, valueType, _)) =>
        arr.map { case JObject(a) =>
          a match {
            case List(k, v) =>
              (k, v) match {
                case (("key", ka), ("value", va)) =>
                  (importAnnotation(ka, keyType, parent), importAnnotation(va, valueType, parent))
              }
            case _ =>
              warn(s"Can't convert JSON value $jv to type $t at $parent.")
              null

          }
        case _ =>
          warn(s"Can't convert JSON value $jv to type $t at $parent.")
          null
        }.toMap

      case (JObject(jfields), t: TStruct) =>
        if (t.size == 0)
          Annotation.empty
        else {
          val a = Array.fill[Any](t.size)(null)

          for ((name, jv2) <- jfields) {
            t.selfField(name) match {
              case Some(f) =>
                a(f.index) = importAnnotation(jv2, f.typ, parent + "." + name)

              case None =>
                warn(s"$t has no field $name at $parent")
            }
          }

          Annotation(a: _*)
        }
      case (_, TAltAllele) =>
        jv.extract[AltAllele]
      case (_, TVariant(_)) =>
        jv.extract[JSONExtractVariant].toVariant
      case (_, TLocus(_)) =>
        jv.extract[Locus]
      case (_, TInterval(_)) =>
        jv.extract[JSONExtractInterval].toInterval
      case (_, TGenotype) =>
        jv.extract[JSONExtractGenotype].toGenotype
      case (JInt(x), TCall) => x.toInt

      case (JArray(a), TArray(elementType, _)) =>
        a.iterator.map(jv2 => importAnnotation(jv2, elementType, parent + ".<array>")).toArray[Any]: IndexedSeq[Any]

      case (JArray(a), TSet(elementType, _)) =>
        a.iterator.map(jv2 => importAnnotation(jv2, elementType, parent + ".<array>")).toSet[Any]

      case _ =>
        warn(s"Can't convert JSON value $jv to type $t at $parent.")
        null
    }
  }
}

object TableAnnotationImpex extends AnnotationImpex[Unit, String] {

  private val sb = new StringBuilder

  // Tables have no schema
  def exportType(t: Type): Unit = ()

  def exportAnnotation(a: Annotation, t: Type): String = {
    if (a == null)
      "NA"
    else {
      t match {
        case TFloat64 => a.asInstanceOf[Double].formatted("%.4e")
        case TString => a.asInstanceOf[String]
        case d: TDict => JsonMethods.compact(d.toJSON(a))
        case it: TIterable => JsonMethods.compact(it.toJSON(a))
        case t: TStruct => JsonMethods.compact(t.toJSON(a))
        case TGenotype => JsonMethods.compact(t.toJSON(a))
        case _: TInterval =>
          val i = a.asInstanceOf[Interval[Locus]]
          if (i.start.contig == i.end.contig)
            s"${ i.start }-${ i.end.position }"
          else s"${ i.start }-${ i.end }"
        case _ => a.toString
      }
    }
  }

  def importAnnotation(a: String, t: Type): Annotation = {
    (t: @unchecked) match {
      case TString => a
      case TInt32 => a.toInt
      case TInt64 => a.toLong
      case TFloat32 => a.toFloat
      case TFloat64 => if (a == "nan") Double.NaN else a.toDouble
      case TBoolean => a.toBoolean
      case _: TLocus => Locus.parse(a)
      case _: TInterval => Locus.parseInterval(a)
      case _: TVariant => Variant.parse(a)
      case TAltAllele => a.split("/") match {
        case Array(ref, alt) => AltAllele(ref, alt)
      }
      case TGenotype => JSONAnnotationImpex.importAnnotation(JsonMethods.parse(a), t)
      case TCall => a.toInt
      case t: TArray => JSONAnnotationImpex.importAnnotation(JsonMethods.parse(a), t)
      case t: TSet => JSONAnnotationImpex.importAnnotation(JsonMethods.parse(a), t)
      case t: TDict => JSONAnnotationImpex.importAnnotation(JsonMethods.parse(a), t)
      case t: TStruct => JSONAnnotationImpex.importAnnotation(JsonMethods.parse(a), t)
    }
  }
}
