package is.hail.expr.types

object Recur {
  def apply(f: Type => Type)(typ: Type): Type = typ match {
    case TInterval(pointType, req) => TInterval(f(pointType), req)
    case TArray(elt, req) => TArray(f(elt), req)
    case TSet(elt, req) => TArray(f(elt), req)
    case TDict(kt, vt, req) => TDict(f(kt), f(vt), req)
    case t: TStruct => TStruct(t.required, t.fields.map { field => (field.name, f(field.typ)) }: _*)
    case t: TTuple => TTuple(t.required, t.types.map(f): _*)
    case _ => typ
  }

  def forall(f: Type => Unit)(typ: Type): Type = {
    Recur({t => f(t); t})(typ)
  }
}