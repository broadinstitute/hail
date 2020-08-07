package is.hail.types.virtual

import is.hail.annotations.{Annotation, ExtendedOrdering, UnsafeIndexedSeq, UnsafeRow}
import is.hail.asm4s.Code
import is.hail.expr.{Nat, NatBase}
import is.hail.types.physical.PNDArray
import is.hail.utils
import org.apache.spark.sql.Row

import scala.reflect.{ClassTag, classTag}

object TNDArray {
  def matMulNDims(l: Int, r: Int): Int = {
    (l, r) match {
      case (1, 1) => 0
      case (1, n) => n - 1
      case (n, 1) => n - 1
      case (_, _) => l
    }
  }

  def validateData(ar: AnyRef): Unit = {
    val r: Row = ar.asInstanceOf[Row]
    val shape = r.getAs[Row](0)
    val strides = r.getAs[Row](1)
    val data = r.getAs[IndexedSeq[_]](2)

    //assert(data.length == shape.toSeq.foldLeft(1L) { case (acc, a) => acc * a.asInstanceOf[Long]}, s"validation error! shape=${shape}, data=${data}")
  }

  def validateData(r: Code[AnyRef]): Code[Unit] = {
    Code.invokeScalaObject1[AnyRef, Unit](
      TNDArray.getClass, "validateData", r
    )
  }
}

final case class TNDArray(elementType: Type, nDimsBase: NatBase) extends Type {
  lazy val nDims: Int = {
    assert(nDimsBase.isInstanceOf[Nat], s"Missing concrete number of dimensions.")
    nDimsBase.asInstanceOf[Nat].n
  }

  override def fundamentalType: Type = representation.fundamentalType

  override def pyString(sb: StringBuilder): Unit = {
    sb.append("ndarray<")
    elementType.pyString(sb)
    sb.append(", ")
    sb.append(nDims)
    sb.append('>')
  }

  def _toPretty = s"NDArray[$elementType,$nDims]"

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean = false) {
    sb.append("NDArray[")
    elementType.pretty(sb, indent, compact)
    sb.append(",")
    sb.append(nDims)
    sb.append("]")
  }

  override def str(a: Annotation): String = {
    if (a == null) "NA" else {
      val a_row = a.asInstanceOf[Row]
      val shape = a_row(this.representation.fieldIdx("shape")).asInstanceOf[Row].toSeq.asInstanceOf[Seq[Long]].map(_.toInt)
      val data = a_row(this.representation.fieldIdx("data")).asInstanceOf[UnsafeIndexedSeq]

      def dataToNestedString(data: Iterator[Annotation], shape: Seq[Int], sb: StringBuilder):Unit  = {
        if (shape.isEmpty) {
          sb.append(data.next().toString)
        }
        else {
          sb.append("[")
          val howMany = shape.head
          var repeat = 0
          while (repeat < howMany) {
            dataToNestedString(data, shape.tail, sb)
            if (repeat != howMany - 1) {
              sb.append(", ")
            }
            repeat += 1
          }
          sb.append("]")
        }
      }

      val stringBuilder = new StringBuilder("")
      dataToNestedString(data.iterator, shape, stringBuilder)
      val prettyData = stringBuilder.result()
      val prettyShape = "(" + shape.mkString(", ") + ")"

      s"ndarray{shape=${prettyShape}, data=${prettyData}}"
    }
  }

  override def unify(concrete: Type): Boolean = {
    concrete match {
      case TNDArray(cElementType, cNDims) => elementType.unify(cElementType) && nDimsBase.unify(cNDims)
      case _ => false
    }
  }

  override def clear(): Unit = {
    elementType.clear()
    nDimsBase.clear()
  }

  override def subst(): TNDArray = TNDArray(elementType.subst(), nDimsBase.subst())

  override def scalaClassTag: ClassTag[Row] = classTag[Row]

  def _typeCheck(a: Any): Boolean = representation._typeCheck(a)

  override def mkOrdering(missingEqual: Boolean): ExtendedOrdering = null

  override def valuesSimilar(a1: Annotation, a2: Annotation, tolerance: Double = utils.defaultTolerance, absolute: Boolean = false): Boolean = {
    val equal = a1 == a2
    //is.hail.utils.warn(s"Stack Trace for valuesSimilar = \n${Thread.currentThread().getStackTrace.mkString("\n")}")
    is.hail.utils.warn(s"TNDArray.valuesSimilar returning $equal: ${a1.toString} vs ${a2.toString}. a1.type = ${a1.getClass.toString}")
    val uia1 = a1.asInstanceOf[Row].getAs[UnsafeIndexedSeq](2)
    val uia2 = a2.asInstanceOf[Row].getAs[UnsafeIndexedSeq](2)
    equal
  }

  lazy val shapeType: TTuple = TTuple(Array.fill(nDims)(TInt64): _*)

  lazy val representation = TStruct(
    ("shape", shapeType),
    ("strides", TTuple(Array.fill(nDims)(TInt64): _*)),
    ("data", TArray(elementType))
  )
}
