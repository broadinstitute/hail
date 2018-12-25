package is.hail.expr.ir

import is.hail.expr.ir.functions.AnonymizeBindings
import is.hail.utils._

object Optimize {
  private def optimize(ir0: BaseIR, noisy: Boolean, canGenerateLiterals: Boolean): BaseIR = {
    if (noisy)
      log.info("optimize: before:\n" + Pretty(ir0, elideLiterals = true))

    var ir = ir0
    Range(0, 3).foreach { _ =>
      ir = FoldConstants(ir, canGenerateLiterals = canGenerateLiterals)
      ir = AnonymizeBindings(ir)
      ir = MaximizeLets(ir)
      ir = Simplify(ir)
      ir = MinimizeLets(ir)
      ir = PruneDeadFields(ir)
    }

    if (ir.typ != ir0.typ)
      fatal(s"optimization changed type!\n  before: ${ ir0.typ }\n  after:  ${ ir.typ }" +
        s"\n  Before IR:\n  ----------\n${ Pretty(ir0) }\n  After IR:\n  ---------\n${ Pretty(ir) }")

    if (noisy)
      log.info("optimize: after:\n" + Pretty(ir, elideLiterals = true))

    ir
  }

  def apply(ir: TableIR, noisy: Boolean, canGenerateLiterals: Boolean): TableIR =
    optimize(ir, noisy, canGenerateLiterals).asInstanceOf[TableIR]

  def apply(ir: TableIR): TableIR = apply(ir, true, true)

  def apply(ir: MatrixIR, noisy: Boolean, canGenerateLiterals: Boolean): MatrixIR =
   optimize(ir, noisy, canGenerateLiterals).asInstanceOf[MatrixIR]

  def apply(ir: MatrixIR): MatrixIR = apply(ir, true, true)

  def apply(ir: IR, noisy: Boolean, canGenerateLiterals: Boolean): IR =
    optimize(ir, noisy, canGenerateLiterals).asInstanceOf[IR]

  def apply(ir: IR): IR = apply(ir, true, true)
}
