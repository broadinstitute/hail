package is.hail.cxx

import is.hail.expr.types

object EmitTriplet {
  def apply(pType: types.physical.PType, setup: Code, m: Code, v: Code): EmitTriplet =
    new EmitTriplet(pType, setup, s"($m)", s"($v)")
}

class EmitTriplet private(val pType: types.physical.PType, val setup: Code, val m: Code, val v: Code) {
  def memoize(fb: FunctionBuilder): EmitTriplet = {
    val mv = fb.variable("memm", "bool", m)
    val vv = fb.variable("memv", typeToCXXType(pType))

    EmitTriplet(pType,
      s"""
         |$setup
         |${ mv.define }
         |${ vv.define }
         |if (!$mv)
         |  $vv = $v;
         |""".stripMargin,
      mv.toString, vv.toString)
  }
}
