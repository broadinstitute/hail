import builtins
from typing import Callable

from hail.expr.expressions import construct_variable, construct_expr, expr_any, to_expr, unify_all
from hail.expr.types import hail_type
from hail.ir.ir import Loop, Recur
from hail.typecheck import anytype, typecheck
from hail.utils.java import Env


# FIXME, infer loop type?
@typecheck(f=anytype, typ=hail_type, exprs=expr_any)
def loop(f: Callable, typ, *exprs):
    """Expression for writing tail recursive expressions.

    Example
    -------

    >>> x = hl.experimental.loop(lambda recur, acc, n: hl.cond(n > 10, acc, recur(acc + n, n + 1)), hl.tint32, 0, 0)
    >>> hl.eval(x)
    55

    Notes
    -----
    The first argument to the lambda is a marker for the recursive call.

    Some infinite loop detection is done, and if an infinite loop is detected, `loop` will not
    typecheck.

    Parameters
    ----------
    f : function ( (marker, *args) -> :class:`.Expression`
        Function of one callable marker, denoting where the recursive call (or calls) is located,
        and many `exprs`, the loop variables.
    typ : :obj:`str` or :class:`.HailType`
        Type the loop returns.
    exprs : variable-length args of :class:`.Expression`
        Expressions to initialize the loop values.

    Returns
    -------
    :class:`.Expression`
        Result of the loop with `exprs` as inital loop values.
    """
    @typecheck(recur_exprs=expr_any)
    def make_loop(*recur_exprs):
        if len(recur_exprs) != len(exprs):
            raise TypeError('loop and recursion must have the same number of arguments')
        irs = [expr._ir for expr in recur_exprs]
        indices, aggregations = unify_all(*recur_exprs)
        return construct_expr(Recur(irs, typ), typ, indices, aggregations)

    uid_irs = []
    args = []

    for expr in exprs:
        uid = Env.get_uid()
        args.append(construct_variable(uid, expr._type, expr._indices, expr._aggregations))
        uid_irs.append((uid, expr._ir))

    lambda_res = to_expr(f(make_loop, *args))
    indices, aggregations = unify_all(*exprs, lambda_res)
    ir = Loop(uid_irs, lambda_res._ir)
    assert ir.typ == typ, f"requested type {typ} does not match inferred type {ir.typ}"
    return construct_expr(ir, lambda_res.dtype, indices, aggregations)
