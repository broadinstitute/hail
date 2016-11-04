package org.broadinstitute.hail.keytable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.check.Gen
import org.broadinstitute.hail.expr.{BaseType, EvalContext, Parser, TBoolean, TStruct, Type}
import org.broadinstitute.hail.methods.Filter
import org.broadinstitute.hail.utils._

import scala.reflect.ClassTag


object KeyTable extends Serializable {
  def annotationToSeq(a: Annotation, nFields: Int) = Option(a).map(_.asInstanceOf[Row].toSeq).getOrElse(Seq.fill[Any](nFields)(null))

  def setEvalContext(ec: EvalContext, k: Annotation, v: Annotation, nKeys: Int, nValues: Int) =
    ec.setAll(annotationToSeq(k, nKeys) ++ annotationToSeq(v, nValues): _*)

  def setEvalContext(ec: EvalContext, a: Annotation, nFields: Int) =
    ec.setAll(annotationToSeq(a, nFields): _*)

  def toSingleRDD(rdd: RDD[(Annotation, Annotation)], nKeys: Int, nValues: Int): RDD[Annotation] =
    rdd.map{ case (k, v) =>
      val x = Annotation.fromSeq(annotationToSeq(k, nKeys) ++ annotationToSeq(v, nValues))
      x
    }

  def apply(rdd: RDD[Annotation], signature: TStruct, keyNames: Array[String]): KeyTable = {
    val keyFields = signature.fields.filter(fd => keyNames.contains(fd.name))
    val keyIndices = keyFields.map(_.index)

    val valueFields = signature.fields.filterNot(fd => keyNames.contains(fd.name))
    val valueIndices = valueFields.map(_.index)

    assert(keyIndices.toSet.intersect(valueIndices.toSet).isEmpty)

    val nFields = signature.size

    val newKeySignature = TStruct(keyFields.map(fd => (fd.name, fd.`type`)): _*)
    val newValueSignature = TStruct(valueFields.map(fd => (fd.name, fd.`type`)): _*)

    val newRDD = rdd.map { a =>
      val r = annotationToSeq(a, nFields).zipWithIndex
      val keyRow = keyIndices.map( i => r(i)._1)
      val valueRow = valueIndices.map( i => r(i)._1)
      (Annotation.fromSeq(keyRow), Annotation.fromSeq(valueRow))
    }

    KeyTable(newRDD, newKeySignature, newValueSignature)
  }
}



case class KeyTable(rdd: RDD[(Annotation, Annotation)], keySignature: TStruct, valueSignature: TStruct) {

  require(fieldNames.toSet.size == fieldNames.length)

  def signature = keySignature.merge(valueSignature)._1
  def fields = signature.fields

  def keySchema = keySignature.schema
  def valueSchema = valueSignature.schema
  def schema = signature.schema

  def keyNames = keySignature.fields.map(_.name)
  def valueNames = valueSignature.fields.map(_.name)
  def fieldNames = keyNames ++ valueNames

  def nRows = rdd.count()
  def nFields = fields.length
  def nKeys = keySignature.size
  def nValues = valueSignature.size

  def same(other: KeyTable): Boolean = {
    if (fields.toSet != other.fields.toSet) {
      println(s"signature: this=${ schema } other=${ other.schema }")
      false
    } else if (keyNames.toSet != other.keyNames.toSet) {
      println(s"keyNames: this=${ keyNames.mkString(",") } other=${ other.keyNames.mkString(",")}")
      false
    } else {
      val thisFieldNames = valueNames
      val otherFieldNames = other.valueNames

      rdd.groupByKey().fullOuterJoin(other.rdd.groupByKey()).forall { case (k, (v1, v2)) =>
        (v1, v2) match {
          case (None, None) => true
          case (Some(x), Some(y)) =>
            val r1 = x.map(r => thisFieldNames.zip(r.asInstanceOf[Row].toSeq).toMap).toSet
            val r2 = y.map(r => otherFieldNames.zip(r.asInstanceOf[Row].toSeq).toMap).toSet
            val res = r1 == r2
            if (!res)
              println(s"k=$k r1=${r1.mkString(",")} r2=${r2.mkString(",")}")
            res
          case _ =>
            println(s"k=$k v1=$v1 v2=$v2")
            false
        }
      }
    }
  }

  def mapAnnotations[T](f: (Annotation) => T)(implicit tct: ClassTag[T]): RDD[T] =
    KeyTable.toSingleRDD(rdd, nKeys, nValues).map(a => f(a))

  def mapAnnotations[T](f: (Annotation, Annotation) => T)(implicit tct: ClassTag[T]): RDD[T] =
    rdd.map{ case (k, v) => f(k, v)}

  def query(code: String): (BaseType, (Annotation, Annotation) => Option[Any]) = {
    val ec = EvalContext(fields.map(f => (f.name, f.`type`)): _*)

    val (t, f) = Parser.parse(code, ec)

    val f2: (Annotation, Annotation) => Option[Any] = { case (k, v) =>
      KeyTable.setEvalContext(ec, k, v, nKeys, nValues)
      f()
    }

    (t, f2)
  }

  def querySingle(code: String): (BaseType, Querier) = {
    val ec = EvalContext(fields.map(f => (f.name, f.`type`)): _*)

    val (t, f) = Parser.parse(code, ec)

    val f2: (Annotation) => Option[Any] = { a =>
      KeyTable.setEvalContext(ec, a, nFields)
      f()
    }

    (t, f2)
  }

  def filter(p: (Annotation, Annotation) => Boolean): KeyTable = copy(rdd = rdd.filter { case (k, v) => p(k, v) })

  def filterExpr(cond: String, keep: Boolean): KeyTable = {
    val ec = EvalContext(fields.map(f => (f.name, f.`type`)): _*)

    val f: () => Option[Boolean] = Parser.parse[Boolean](cond, ec, TBoolean)

    val p = (k: Annotation, v: Annotation) => {
      KeyTable.setEvalContext(ec, k, v, nKeys, nValues)
      Filter.keepThis(f(), keep)
    }

    filter(p)
  }

  def leftJoin(other: KeyTable): KeyTable = {
    require(keySignature == other.keySignature)

    val (newValueSignature, merger) = valueSignature.merge(other.valueSignature)
    val newRDD = rdd.leftOuterJoin(other.rdd).map{ case (k, (vl, vr)) => (k, merger(vl, vr.orNull)) }

    KeyTable(newRDD, keySignature, newValueSignature)
  }

  def rightJoin(other: KeyTable): KeyTable = {
    require(keySignature == other.keySignature)

    val (newValueSignature, merger) = valueSignature.merge(other.valueSignature)
    val newRDD = rdd.rightOuterJoin(other.rdd).map{ case (k, (vl, vr)) => (k, merger(vl.orNull, vr)) }

    KeyTable(newRDD, keySignature, newValueSignature)
  }

  def outerJoin(other: KeyTable): KeyTable = {
    require(keySignature == other.keySignature)

    val (newValueSignature, merger) = valueSignature.merge(other.valueSignature)
    val newRDD = rdd.fullOuterJoin(other.rdd).map{ case (k, (vl, vr)) => (k, merger(vl.orNull, vr.orNull)) }

    KeyTable(newRDD, keySignature, newValueSignature)
  }

  def innerJoin(other: KeyTable): KeyTable = {
    require(keySignature == other.keySignature)

    val (newValueSignature, merger) = valueSignature.merge(other.valueSignature)
    val newRDD = rdd.join(other.rdd).map{ case (k, (vl, vr)) => (k, merger(vl, vr)) }

    KeyTable(newRDD, keySignature, newValueSignature)
  }

}