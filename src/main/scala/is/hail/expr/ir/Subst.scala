package is.hail.expr.ir

import is.hail.expr.types.TAggregable

object Subst {
  def apply(e: IR): IR = apply(e, Env.empty)

  def apply(e: IR, env: Env[IR]): IR = {
    def subst(e: IR, env: Env[IR] = env): IR = apply(e, env)

    e match {
      case x@Ref(name, typ) =>
        val s = env.lookupOption(name).getOrElse(x)
        println(Pretty(x))
        println(Pretty(s))
        assert(s.typ == x.typ, s"$name: ${ x.typ.parsableString() }, ${ s.typ.parsableString() }")
        s
      case Let(name, v, body) =>
        Let(name, subst(v), subst(body, env.delete(name)))
      case ArrayMap(a, name, body) =>
        ArrayMap(subst(a), name, subst(body, env.delete(name)))
      case ArrayFilter(a, name, cond) =>
        ArrayFilter(subst(a), name, subst(cond, env.delete(name)))
      case ArrayFlatMap(a, name, body) =>
        ArrayFlatMap(subst(a), name, subst(body, env.delete(name)))
      case ArrayFold(a, zero, accumName, valueName, body) =>
        ArrayFold(subst(a), subst(zero), accumName, valueName, subst(body, env.delete(accumName).delete(valueName)))
      case ApplyAggOp(a, op, args) =>
        val substitutedArgs = args.map(arg => Recur(subst(_))(arg))
        ApplyAggOp(subst(a, aggEnv, Env.empty), op, substitutedArgs, typ)
      case _ =>
        Recur(subst(_))(e)
    }
  }
}
