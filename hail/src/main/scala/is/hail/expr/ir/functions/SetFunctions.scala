package is.hail.expr.ir.functions

import is.hail.expr.ir._
import is.hail.expr.types.{virtual, _}
import is.hail.expr.types
import is.hail.expr.types.virtual.{TArray, TBoolean, TInt32, TSet}
import is.hail.utils.FastSeq

object SetFunctions extends RegistryFunctions {
  def contains(set: IR, elem: IR) = {
    val i = Ref(genSym("i"), TInt32())

    If(IsNA(set),
      NA(TBoolean()),
      Let(i.name,
        LowerBoundOnOrderedCollection(set, elem, onKey = false),
        If(i.ceq(ArrayLen(ToArray(set))),
          False(),
          ApplyComparisonOp(EQWithNA(elem.typ), ArrayRef(ToArray(set), i), elem))))
  }

  def registerAll() {
    registerIR("toSet", TArray(tv("T"))) { a =>
      ToSet(a)
    }

    registerIR("isEmpty", TSet(tv("T"))) { s =>
      ArrayFunctions.isEmpty(ToArray(s))
    }

    registerIR("contains", TSet(tv("T")), tv("T"))(contains)

    registerIR("remove", TSet(tv("T")), tv("T")) { (s, v) =>
      val t = v.typ
      val x = genSym("x")
      ToSet(
        ArrayFilter(
          ToArray(s),
          x,
          ApplyComparisonOp(NEQWithNA(t), Ref(x, t), v)))
    }

    registerIR("add", TSet(tv("T")), tv("T")) { (s, v) =>
      val t = v.typ
      val x = genSym("x")
      ToSet(
        ArrayFlatMap(
          MakeArray(FastSeq(ToArray(s), MakeArray(FastSeq(v), TArray(t))), TArray(TArray(t))),
          x,
          Ref(x, TArray(t))))
    }

    registerIR("union", TSet(tv("T")), TSet(tv("T"))) { (s1, s2) =>
      val t = -s1.typ.asInstanceOf[TSet].elementType
      val x = genSym("x")
      ToSet(
        ArrayFlatMap(
          MakeArray(FastSeq(ToArray(s1), ToArray(s2)), TArray(TArray(t))),
          x,
          Ref(x, TArray(t))))
    }

    registerIR("intersection", TSet(tv("T")), TSet(tv("T"))) { (s1, s2) =>
      val t = -s1.typ.asInstanceOf[TSet].elementType
      val x = genSym("x")
      ToSet(
        ArrayFilter(ToArray(s1), x,
          contains(s2, Ref(x, t))))
    }

    registerIR("difference", TSet(tv("T")), TSet(tv("T"))) { (s1, s2) =>
      val t = -s1.typ.asInstanceOf[TSet].elementType
      val x = genSym("x")
      ToSet(
        ArrayFilter(ToArray(s1), x,
          ApplyUnaryPrimOp(Bang(), contains(s2, Ref(x, t)))))
    }

    registerIR("isSubset", TSet(tv("T")), TSet(tv("T"))) { (s, w) =>
      val t = -s.typ.asInstanceOf[TSet].elementType
      val a = genSym("a")
      val x = genSym("x")
      ArrayFold(ToArray(s), True(), a, x,
        // FIXME short circuit
        ApplySpecial("&&",
          FastSeq(Ref(a, TBoolean()), contains(w, Ref(x, t)))))
    }

    registerIR("sum", TSet(tnum("T"))) { s =>
      ArrayFunctions.sum(ToArray(s))
    }

    registerIR("product", TSet(tnum("T"))) { s =>
      ArrayFunctions.product(ToArray(s))
    }

    registerIR("min", TSet(tnum("T"))) { s =>
      val t = s.typ.asInstanceOf[TSet].elementType
      val a = genSym("a")
      val size = genSym("size")
      val last = genSym("last")

      Let(a, ToArray(s),
        Let(size, ArrayLen(Ref(a, TArray(t))),
          If(ApplyComparisonOp(EQ(TInt32()), Ref(size, TInt32()), I32(0)),
            NA(t),
            If(IsNA(ArrayRef(Ref(a, TArray(t)), ApplyBinaryPrimOp(Subtract(), Ref(size, TInt32()), I32(1)))),
              NA(t),
              ArrayRef(Ref(a, TArray(t)), I32(0))))))
    }

    registerIR("max", TSet(tnum("T"))) { s =>
      val t = s.typ.asInstanceOf[TSet].elementType
      val a = genSym("a")
      val size = genSym("size")
      val last = genSym("last")

      Let(a, ToArray(s),
        Let(size, ArrayLen(Ref(a, TArray(t))),
          If(ApplyComparisonOp(EQ(TInt32()), Ref(size, TInt32()), I32(0)),
            NA(t),
            ArrayRef(Ref(a, TArray(t)), ApplyBinaryPrimOp(Subtract(), Ref(size, TInt32()), I32(1))))))
    }

    registerIR("mean", TSet(tnum("T"))) { s => ArrayFunctions.mean(ToArray(s)) }

    registerIR("median", TSet(tnum("T"))) { s =>
      val t = -s.typ.asInstanceOf[TSet].elementType
      val a = Ref(genSym("a"), TArray(t))
      val size = Ref(genSym("size"), TInt32())
      val lastIdx = size - 1
      val midIdx = lastIdx.floorDiv(2)
      def ref(i: IR) = ArrayRef(a, i)
      val len: IR = ArrayLen(a)
      def div(a: IR, b: IR): IR = ApplyBinaryPrimOp(BinaryOp.defaultDivideOp(t), a, b)

      Let(a.name, ToArray(s),
        If(IsNA(a),
          NA(t),
          Let(size.name,
            If(len.ceq(0), len, If(IsNA(ref(len - 1)), len - 1, len)),
            If(size.ceq(0),
              NA(t),
              If(invoke("%", size, 2).cne(0),
                ref(midIdx), // odd number of non-missing elements
                div(ref(midIdx) + ref(midIdx + 1), Cast(2, t)))))))
    }
  }
}
