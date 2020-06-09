package is.hail.types.virtual

import is.hail.annotations.ExtendedOrdering
import is.hail.utils._
import org.apache.spark.sql.Row

object TTuple {
  val empty: TTuple = TTuple()

  def apply(args: Type*): TTuple = TTuple(args.iterator.zipWithIndex.map { case (t, i) => TupleField(i, t) }.toArray)
}

case class TupleField(index: Int, typ: Type)

final case class TTuple(_types: IndexedSeq[TupleField]) extends TBaseStruct {
  lazy val types: Array[Type] = _types.map(_.typ).toArray

  lazy val fields: IndexedSeq[Field] = _types.map { tf => Field(s"${ tf.index }", tf.typ, tf.index) }

  lazy val fieldIndex: Map[Int, Int] = _types.zipWithIndex.map { case (tf, idx) => tf.index -> idx }.toMap

  override lazy val ordering: ExtendedOrdering = mkOrdering()

  override def mkOrdering(missingEqual: Boolean): ExtendedOrdering =
    TBaseStruct.getOrdering(types, missingEqual)

  override lazy val _isCanonical: Boolean = _types.indices.forall(i => i == _types(i).index)

  def size: Int = types.length

  override def truncate(newSize: Int): TTuple =
    TTuple(_types.take(newSize))

  override def canCompare(other: Type): Boolean = other match {
    case t: TTuple => size == t.size && _types.zip(t._types).forall { case (t1, t2) => t1.index == t2.index && t1.typ.canCompare(t2.typ) }
    case _ => false
  }

  override def unify(concrete: Type): Boolean = concrete match {
    case TTuple(ctypes) =>
      size == ctypes.length &&
        (types, ctypes).zipped.forall { case (t, ct) =>
          t.unify(ct.typ)
        }
    case _ => false
  }

  override def subst() = TTuple(_types.map(tf => tf.copy(typ = tf.typ.subst())))

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean) {
    if (!_isCanonical) {
      sb.append("TupleSubset[")
      _types.foreachBetween { fd =>
        sb.append(fd.index)
        sb.append(':')
        fd.typ.pretty(sb, indent, compact)
      }(sb += ',')
      sb += ']'
    } else {
      sb.append("Tuple[")
      _types.foreachBetween { fd => fd.typ.pretty(sb, indent, compact) }(sb += ',')
      sb += ']'
    }
  }

  override def pyString(sb: StringBuilder): Unit = {
    sb.append("tuple(")
    if (!_isCanonical) {
      fields.foreachBetween({ field =>
        sb.append(field.index)
        sb.append(':')
        field.typ.pyString(sb)
      }) { sb.append(", ") }
      sb.append(')')
    } else {
      fields.foreachBetween({ field => field.typ.pyString(sb) }) { sb.append(", ") }
    }
    sb.append(')')
  }


  override lazy val fundamentalType: TTuple = {
    val fundamentalFieldTypes = _types.map(tf => tf.copy(typ = tf.typ.fundamentalType))
    if ((_types, fundamentalFieldTypes).zipped
      .forall { case (t, ft) => t == ft })
      this
    else
      TTuple(fundamentalFieldTypes)
  }

  override def valueSubsetter(subtype: Type): Any => Any = {
    if (this == subtype)
      return identity

    val subTuple = subtype.asInstanceOf[TTuple]
    val subsetFields = subTuple.fields.map(f => (fieldIndex(f.index), fields(f.index).typ.valueSubsetter(f.typ)))

    { (a: Any) =>
      val r = a.asInstanceOf[Row]
      Row.fromSeq(subsetFields.map { case (i, subset) => subset(r.get(i)) })
    }
  }
}
