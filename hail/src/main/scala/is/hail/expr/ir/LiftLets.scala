package is.hail.expr.ir

import scala.collection.mutable

case class LetBinding(name: String, value: IR, bindings: List[LetBinding])

object LiftLets {

  def breaksScope(x: BaseIR): Boolean = {
    (x: @unchecked) match {
      case _: TableAggregate => true
      case _: MatrixAggregate => true
      case _: TableCollect => true
      case _: ApplyAggOp => true
      case _: ApplyScanOp => true
      case _: AggFilter => true
      case _: AggGroupBy => true
      case _: AggExplode => true
      case _: IR => false
      case _ => true
    }
  }

  def prependBindings(x: IR, bindings: List[LetBinding]): IR = {
    val m = mutable.Map.empty[IR, String]
    bindings.foldLeft(x) { case (ir, binding) =>
      // reduce equivalent lets
      val ir1 = m.get(binding.value) match {
        case Some(prevName) =>
          if (prevName != binding.name)
            Subst(ir, Env(binding.name -> Ref(prevName, binding.value.typ)))
          else ir
        case None =>
          m += binding.value -> binding.name
          Let(binding.name, binding.value, ir)
      }

      prependBindings(ir1, binding.bindings)
    }
  }

  def apply(ir0: BaseIR): BaseIR = {
    val (lifted, lb) = lift(ir0)
    if (lb.nonEmpty)
      prependBindings(lifted.asInstanceOf[IR], lb)
    else
      lifted
  }

  def lift(ir0: BaseIR): (BaseIR, List[LetBinding]) = {
    (ir0: @unchecked) match {
      case Let(name, value, body) =>
        val (liftedBody, bodyBindings) = lift(body)
        val (liftedValue: IR, valueBindings) = lift(value)
        val subInclusion = bodyBindings.map(lb => lb.name == name || Mentions(lb.value, name))
        val lb = (LetBinding(name, liftedValue, bodyBindings.zip(subInclusion).filter(_._2).map(_._1))
          :: valueBindings
          ::: bodyBindings.zip(subInclusion).filter { case (_, sub) => !sub }.map(_._1))
        liftedBody -> lb
      case ir1 if breaksScope(ir1) =>
        ir1.copy(ir1.children.map(apply)) -> Nil
      case ir0: IR =>
        val bindings = Bindings(ir0)
        val (newChildren, letBindings) = Children(ir0)
          .zipWithIndex
          .map { case (c, i) =>
            val (liftedChild, lbs) = lift(c)
            if (lbs.nonEmpty) {
              val subInclusion = lbs.map(lb => bindings.exists(b => lb.name == b || Mentions(lb.value, b)))
              prependBindings(liftedChild.asInstanceOf[IR], lbs.zip(subInclusion).filter(_._2).map(_._1)) -> lbs.zip(subInclusion).filter(t => !t._2).map(_._1)
            } else liftedChild -> Nil
          }.unzip
        ir0.copy(newChildren) -> letBindings.fold(Nil)(_ ::: _)
    }
  }
}