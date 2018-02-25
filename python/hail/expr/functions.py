from hail.typecheck import *
from hail.expr.expression import *
from hail.expr.ast import *
from hail.genetics import Locus, Call, GenomeReference

std_len = len
std_str = str
std_all = all


def _func(name, ret_type, *args):
    indices, aggregations, joins, refs = unify_all(*args)
    return construct_expr(ApplyMethod(name, *(a._ast for a in args)), ret_type, indices, aggregations, joins, refs)


@typecheck(t=Type)
def null(t):
    """Creates an expression representing a missing value of a specified type.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.null(hl.tstr))
        None

    Notes
    -----
    This method is useful for constructing an expression that includes missing
    values, since :obj:`None` cannot be interpreted as an expression.

    Parameters
    ----------
    t : :class:`.Type`
        Type of the missing expression.

    Returns
    -------
    :class:`.Expression`
        A missing expression of type `t`.
    """
    return construct_expr(Literal('NA: {}'.format(t._jtype.toString())), t)


def capture(x):
    """Captures a Python variable or object as an expression.


    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.capture(5))
        5

        >>> hl.eval_expr(hl.capture([1, 2, 3]))
        [1, 2, 3]

    Warning
    -------
    For large objects, use :meth:`.broadcast`.

    Parameters
    ----------
    x
        Python variable to capture as an expression.

    Returns
    -------
    :class:`.Expression`
        An expression representing `x`.
    """
    return to_expr(x)


def broadcast(x):
    """Broadcasts a Python variable or object as an expression.

    Examples
    --------
    .. doctest::

        >>> table = hl.utils.range_table(8)
        >>> greetings = hl.broadcast({1: 'Good morning', 4: 'Good afternoon', 6 : 'Good evening'})
        >>> table.annotate(greeting = greetings.get(table.idx)).show()
        +-------+----------------+
        | index | greeting       |
        +-------+----------------+
        | Int32 | String         |
        +-------+----------------+
        |     0 | NA             |
        |     1 | Good morning   |
        |     2 | NA             |
        |     3 | NA             |
        |     4 | Good afternoon |
        |     5 | NA             |
        |     6 | Good evening   |
        |     7 | NA             |
        +-------+----------------+

    Notes
    -----
    Use this function to capture large Python objects for use in expressions. This
    function provides an alternative to adding an object as a global annotation on a
    :class:hail.Table or :class:hail.MatrixTable.

    Parameters
    ----------
    x
        Python variable to broadcast as an expression.

    Returns
    -------
    :class:`.Expression`
        An expression representing `x`.
    """
    expr = to_expr(x)
    uid = Env._get_uid()

    def joiner(obj):
        from hail.table import Table
        from hail.matrixtable import MatrixTable
        if isinstance(obj, Table):
            return Table(obj._jt.annotateGlobalExpr('{} = {}'.format(uid, expr._ast.to_hql())))
        else:
            assert isinstance(obj, MatrixTable)
            return MatrixTable(obj._jvds.annotateGlobalExpr('global.{} = {}'.format(uid, expr._ast.to_hql())))

    return construct_expr(GlobalJoinReference(uid), expr._type, joins=LinkedList(Join).push(Join(joiner, [uid], uid)))


@typecheck(condition=expr_bool, consequent=expr_any, alternate=expr_any)
def cond(condition, consequent, alternate):
    """Expression for an if/else statement; tests a condition and returns one of two options based on the result.

    Examples
    --------
    .. doctest::

        >>> x = 5
        >>> hl.eval_expr( hl.cond(x < 2, 'Hi', 'Bye') )
        'Bye'

        >>> a = hl.capture([1, 2, 3, 4])
        >>> hl.eval_expr( hl.cond(hl.len(a) > 0,
        ...                   2.0 * a,
        ...                   a / 2.0) )
        [2.0, 4.0, 6.0, 8.0]

    Notes
    -----

    If `condition` evaluates to ``True``, returns `consequent`. If `condition`
    evaluates to ``False``, returns `alternate`. If `predicate` is missing, returns
    missing.

    Note
    ----
    The type of `consequent` and `alternate` must be the same.

    Parameters
    ----------
    condition : :class:`.BooleanExpression`
        Condition to test.
    consequent : :class:`.Expression`
        Branch to return if the condition is ``True``.
    alternate : :class:`.Expression`
        Branch to return if the condition is ``False``.

    Returns
    -------
    :class:`.Expression`
        One of `consequent`, `alternate`, or missing, based on `condition`.
    """
    indices, aggregations, joins, refs = unify_all(condition, consequent, alternate)
    t = unify_types_limited(consequent._type, alternate._type)
    if not t:
        raise TypeError("'cond' requires the 'consequent' and 'alternate' arguments to have the same type\n"
                        "    consequent: type {}\n"
                        "    alternate:  type {}".format(consequent._type, alternate._type))
    return construct_expr(Condition(condition._ast, consequent._ast, alternate._ast),
                          t, indices, aggregations, joins, refs)


def case():
    """Chain multiple if-else statements with a :class:`.CaseBuilder`.

    Examples
    --------
    .. doctest::

        >>> x = hl.capture('foo bar baz')
        >>> expr = (hl.case()
        ...                  .when(x[:3] == 'FOO', 1)
        ...                  .when(hl.len(x) == 11, 2)
        ...                  .when(x == 'secret phrase', 3)
        ...                  .default(0))
        >>> hl.eval_expr(expr)
        2

    See Also
    --------
    :class:`.CaseBuilder`

    Returns
    -------
    :class:`.CaseBuilder`.
    """
    from hail.expr.utils import CaseBuilder
    return CaseBuilder()


@typecheck(expr=expr_any)
def switch(expr):
    """Build a conditional tree on the value of an expression.

    Examples
    --------
    .. doctest::

        >>> csq = hl.capture('loss of function')
        >>> expr = (hl.switch(csq)
        ...                  .when('synonymous', 1)
        ...                  .when('SYN', 1)
        ...                  .when('missense', 2)
        ...                  .when('MIS', 2)
        ...                  .when('loss of function', 3)
        ...                  .when('LOF', 3)
        ...                  .or_missing())
        >>> hl.eval_expr(expr)
        3

    See Also
    --------
    :class:`.SwitchBuilder`

    Parameters
    ----------
    expr : :class:`.Expression`
        Value to match against.

    Returns
    -------
    :class:`.SwitchBuilder`
    """
    from hail.expr.utils import SwitchBuilder
    return SwitchBuilder(expr)


@typecheck(expr=expr_any, f=func_spec(1, expr_any))
def bind(expr, f):
    """Bind a temporary variable and use it in a function.

    Examples
    --------
    Expressions are "inlined", leading to perhaps unexpected behavior
    when randomness is involved. For example, let us define a variable
    `x` from the :meth:`.rand_unif` method:

    >>> x = hl.rand_unif(0, 1)

    Note that evaluating `x` multiple times returns different results.
    The value of evaluating `x` is unknown when the expression is defined.

    .. doctest::

        >>> hl.eval_expr(x)
        0.3189309481038456

        >>> hl.eval_expr(x)
        0.20842918568366375

    What if we evaluate `x` multiple times in the same invocation of
    :meth:`~hail.expr.eval_expr`?

    .. doctest::

        >>> hl.eval_expr([x, x, x])
        [0.49582541026815163, 0.8549329234134524, 0.7016124997911775]

    The random number generator is called separately for each inclusion
    of `x`. This method, `bind`, is the solution to this problem!

    .. doctest::

        >>> hl.eval_expr(hl.bind(x, lambda y: [y, y, y]))
        [0.7897028763765286, 0.7897028763765286, 0.7897028763765286]

    Parameters
    ----------
    expr : :class:`.Expression`
        Expression to bind.
    f : function ( (arg) -> :class:`.Expression`)
        Function of `expr`.

    Returns
    -------
    :class:`.Expression`
        Result of evaluating `f` with `expr` as an argument.
    """
    uid = Env._get_uid()
    expr = to_expr(expr)

    f_input = construct_expr(Reference(uid), expr._type, expr._indices, expr._aggregations, expr._joins, expr._refs)
    lambda_result = to_expr(f(f_input))

    indices, aggregations, joins, refs = unify_all(expr, lambda_result)
    ast = Bind(uid, expr._ast, lambda_result._ast)
    return construct_expr(ast, lambda_result._type, indices, aggregations, joins, refs)


@typecheck(c1=expr_int32, c2=expr_int32, c3=expr_int32, c4=expr_int32)
def chisq(c1, c2, c3, c4):
    """Calculates p-value (Chi-square approximation) and odds ratio for a 2x2 table.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.chisq(10, 10,
        ...                   10, 10))
        Struct(oddsRatio=1.0, pValue=1.0)

        >>> hl.eval_expr(hl.chisq(30, 30,
        ...                   50, 10))
        Struct(oddsRatio=0.2, pValue=0.000107511176729)

    Parameters
    ----------
    c1 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 1.
    c2 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 2.
    c3 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 3.
    c4 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 4.

    Returns
    -------
    :class:`.StructExpression`
        A struct expression with two fields, `pValue` (``Float64``) and `oddsRatio` (``Float64``).
    """
    ret_type = tstruct(['pValue', 'oddsRatio'], [tfloat64, tfloat64])
    return _func("chisq", ret_type, c1, c2, c3, c4)


@typecheck(c1=expr_int32, c2=expr_int32, c3=expr_int32, c4=expr_int32, min_cell_count=expr_int32)
def ctt(c1, c2, c3, c4, min_cell_count):
    """Calculates p-value and odds ratio for 2x2 table.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.ctt(10, 10,
        ...              10, 10, min_cell_count=15))
        Struct(oddsRatio=1.0, pValue=1.0)

        >>> hl.eval_expr(hl.ctt(30, 30,
        ...              50, 10, min_cell_count=15))
        Struct(oddsRatio=0.202874620964, pValue=0.000190499944324)

    Notes
    -----
     If any cell is lower than `min_cell_count`, Fisher's exact test is used. Otherwise, faster
     chi-squared approximation is used.

    Parameters
    ----------
    c1 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 1.
    c2 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 2.
    c3 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 3.
    c4 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 4.
    min_cell_count : int or :class:`.Expression` of type :class:`.TInt32`
        Minimum cell count for chi-squared approximation.

    Returns
    -------
    :class:`.StructExpression`
        A struct expression with two fields, `pValue` (``Float64``) and `oddsRatio` (``Float64``).
    """
    ret_type = tstruct(['pValue', 'oddsRatio'], [tfloat64, tfloat64])
    return _func("ctt", ret_type, c1, c2, c3, c4, min_cell_count)


@typecheck(keys=expr_array, values=expr_array)
def dict(keys, values):
    """Creates a dictionary from a list of keys and a list of values.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.dict(['foo', 'bar', 'baz'], [1, 2, 3]))
        {u'bar': 2, u'baz': 3, u'foo': 1}

    Notes
    -----
    `keys` and `values` must be have the same length.

    Parameters
    ----------
    keys : list or :class:`.ArrayExpression`
        The keys of the resulting dictionary.
    values : list or :class:`.ArrayExpression`
        The values of the resulting dictionary.

    Returns
    -------
    :class:`.DictExpression`
        A dictionary expression constructed from `keys` and `values`.

    """
    keys = to_expr(keys)
    values = to_expr(values)
    ret_type = tdict(keys.dtype.element_type, values.dtype.element_type)
    return _func("Dict", ret_type, keys, values)


@typecheck(x=expr_numeric, a=expr_numeric, b=expr_numeric)
def dbeta(x, a, b):
    """
    Returns the probability density at `x` of a `beta distribution
    <https://en.wikipedia.org/wiki/Beta_distribution>`__ with parameters `a`
    (alpha) and `b` (beta).

    Examples
    --------
    .. doctest::

        >> hl.eval_expr(hl.dbeta(.2, 5, 20))
        4.900377563180943

    Parameters
    ----------
    x : :obj:`float` or :class:`.Expression` of type :class:`.TFloat64`
        Point in [0,1] at which to sample. If a < 1 then x must be positive.
        If b < 1 then x must be less than 1.
    a : :obj:`float` or :class:`.Expression` of type :class:`.TFloat64`
        The alpha parameter in the beta distribution. The result is undefined
        for non-positive a.
    b : :obj:`float` or :class:`.Expression` of type :class:`.TFloat64`
        The beta parameter in the beta distribution. The result is undefined
        for non-positive b.
    """
    return _func("dbeta", tfloat64, x, a, b)


@typecheck(x=expr_numeric, lamb=expr_numeric, log_p=expr_bool)
def dpois(x, lamb, log_p=False):
    """Compute the (log) probability density at x of a Poisson distribution with rate parameter `lamb`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.dpois(5, 3))
        0.10081881344492458

    Parameters
    ----------
    x : :obj:`float` or :class:`.Expression` of type :class:`.TFloat64`
        Non-negative number at which to compute the probability density.
    lamb : :obj:`float` or :class:`.Expression` of type :class:`.TFloat64`
        Poisson rate parameter. Must be non-negative.
    log_p : :obj:`bool` or :class:`.BooleanExpression`
        If true, the natural logarithm of the probability density is returned.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
        The (log) probability density.
    """
    return _func("dpois", tfloat64, x, lamb, log_p)


@typecheck(x=expr_numeric)
def exp(x):
    """Computes `e` raised to the power `x`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.exp(2))
        7.38905609893065

    Parameters
    ----------
    x : float or :class:`.Expression` of type :class:`.TFloat64`

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("exp", tfloat64, x)


@typecheck(c1=expr_int32, c2=expr_int32, c3=expr_int32, c4=expr_int32)
def fisher_exact_test(c1, c2, c3, c4):
    """Calculates the p-value, odds ratio, and 95% confidence interval with Fisher's exact test for a 2x2 table.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.fisher_exact_test(10, 10,
        ...                                   10, 10))
        Struct(oddsRatio=1.0, pValue=1.0)

        >>> hl.eval_expr(hl.fisher_exact_test(30, 30,
        ...                                   50, 10))
        Struct(oddsRatio=0.202874620964, pValue=0.000190499944324)

    Notes
    -----
    This method is identical to the version implemented in
    `R <https://stat.ethz.ch/R-manual/R-devel/library/stats/html/fisher.test.html>`_ with default
    parameters (two-sided, alpha = 0.05, null hypothesis that the odds ratio equals 1).

    Parameters
    ----------
    c1 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 1.
    c2 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 2.
    c3 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 3.
    c4 : int or :class:`.Expression` of type :class:`.TInt32`
        Value for cell 4.

    Returns
    -------
    :class:`.StructExpression`
        A struct expression with four fields, `pValue` (``Float64``), `oddsRatio` (``Float64``),
        ci95Lower (``Float64``), and ci95Upper(``Float64``).
    """
    ret_type = tstruct(['pValue', 'oddsRatio', 'ci95Lower', 'ci95Upper'],
                       [tfloat64, tfloat64, tfloat64, tfloat64])
    return _func("fet", ret_type, c1, c2, c3, c4)


@typecheck(x=expr_numeric)
def floor(x):
    """The largest integral value that is less than or equal to `x`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.floor(3.1))
        3.0

    Returns
    -------
    :obj:`.Float64Expression`
    """
    return _func("floor", unify_types(x.dtype, tfloat32), x)


@typecheck(x=expr_numeric)
def ceil(x):
    """The smallest integral value that is greater than or equal to `x`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.ceil(3.1))
        4.0

    Returns
    -------
    :obj:`.Float64Expression`
    """
    return _func("ceil", unify_types(x.dtype, tfloat32), x)


@typecheck(num_hom_ref=expr_int32, num_het=expr_int32, num_hom_var=expr_int32)
def hardy_weinberg_p(num_hom_ref, num_het, num_hom_var):
    """Compute Hardy-Weinberg Equilbrium p-value and heterozygosity ratio.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.hardy_weinberg_p(20, 50, 26))
        Struct(rExpectedHetFrequency=0.500654450262, pHWE=0.762089599352)

        >>> hl.eval_expr(hl.hardy_weinberg_p(37, 200, 85))
        Struct(rExpectedHetFrequency=0.489649643074, pHWE=1.13372103832e-06)

    Notes
    -----
    For more information, see the
    `Wikipedia page <https://en.wikipedia.org/wiki/Hardy%E2%80%93Weinberg_principle>`__

    Parameters
    ----------
    num_hom_ref : int or :class:`.Expression` of type :class:`.TInt32`
        Homozygous reference count.
    num_het : int or :class:`.Expression` of type :class:`.TInt32`
        Heterozygote count.
    num_hom_var : int or :class:`.Expression` of type :class:`.TInt32`
        Homozygous alternate count.

    Returns
    -------
    :class:`.StructExpression`
        A struct expression with two fields, `rExpectedHetFrequency` (``Float64``) and`pValue` (``Float64``).
    """
    ret_type = tstruct(['rExpectedHetFrequency', 'pHWE'], [tfloat64, tfloat64])
    return _func("hwe", ret_type, num_hom_ref, num_het, num_hom_var)


@typecheck(structs=oneof(expr_array, listof(Struct)),
           identifier=str)
def index(structs, identifier):
    if not isinstance(structs.dtype.element_type, TStruct):
        raise TypeError("'index' expects an array with element type 'Struct', found '{}'"
                        .format(structs.dtype))
    structs = to_expr(structs)
    struct_type = structs._type.element_type
    struct_fields = {fd.name: fd.typ for fd in struct_type.fields}

    if identifier not in struct_fields:
        raise RuntimeError("`structs' does not have a field with identifier `{}'. " \
                           "Struct type is {}.".format(identifier, struct_type))

    key_type = struct_fields[identifier]
    value_type = TStruct.from_fields([f for f in struct_type.fields if f.name != identifier])

    ast = StructOp('index', structs._ast, identifier)
    return construct_expr(ast, tdict(key_type, value_type),
                          structs._indices, structs._aggregations, structs._joins, structs._refs)


@typecheck(contig=expr_str, pos=expr_int32, reference_genome=nullable(GenomeReference))
def locus(contig, pos, reference_genome=None):
    """Construct a locus expression from a chromosome and position.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.locus("1", 10000))
        Locus(contig=1, position=10000, reference_genome=GRCh37)

    Parameters
    ----------
    contig : str or :class:`.StringExpression`
        Chromosome.
    pos : int or :class:`.Expression` of type :class:`.TInt32`
        Base position along the chromosome.
    reference_genome : :class:`.hail.genetics.GenomeReference` (optional)
        Reference genome to use (uses :meth:`~hail.default_reference` if not passed).

    Returns
    -------
    :class:`.LocusExpression`
    """
    contig = to_expr(contig)
    pos = to_expr(pos)
    if reference_genome is None:
        reference_genome = Env.hc().default_reference
    indices, aggregations, joins, refs = unify_all(contig, pos)
    return construct_expr(ApplyMethod('Locus({})'.format(reference_genome.name), contig._ast, pos._ast),
                          tlocus(reference_genome), indices, aggregations, joins, refs)


@typecheck(s=expr_str, reference_genome=nullable(GenomeReference))
def parse_locus(s, reference_genome=None):
    """Construct a locus expression by parsing a string or string expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.parse_locus("1:10000"))
        Locus(contig=1, position=10000, reference_genome=GRCh37)

    Notes
    -----
    This method expects strings of the form ``contig:position``, e.g. ``16:29500000``
    or ``X:123456``.

    Parameters
    ----------
    s : str or :class:`.StringExpression`
        String to parse.
    reference_genome : :class:`.hail.genetics.GenomeReference` (optional)
        Reference genome to use (uses :meth:`.default_reference` if not passed).

    Returns
    -------
    :class:`.LocusExpression`
    """
    s = to_expr(s)
    if reference_genome is None:
        reference_genome = Env.hc().default_reference
    return construct_expr(ApplyMethod('Locus({})'.format(reference_genome.name), s._ast), tlocus(reference_genome),
                          s._indices, s._aggregations, s._joins, s._refs)


@typecheck(s=expr_str, reference_genome=nullable(GenomeReference))
def parse_variant(s, reference_genome=None):
    """Construct a struct with a locus and alleles by parsing a string.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.parse_variant('1:100000:A:T,C'))
        Struct(locus=Locus('1', 100000), alleles=['A', 'T', 'C'])

    Notes
    -----
    This method returns an expression of type :class:`.TStruct` with the
    following fields:

     - `locus` (:class:`.TLocus`)
     - `alleles` (:class:`.TArray` of :class:`.TString`)

    Parameters
    ----------
    s : :class:`.StringExpression`
        String to parse.
    reference_genome: :class:`.GenomeReference`
        Genome reference.

    Returns
    -------
    :class:`.StructExpression`
        Struct with fields `locus` and `alleles`.
    """
    s = to_expr(s)
    if reference_genome is None:
        reference_genome = Env.hc().default_reference
    t = tstruct(['locus', 'alleles'], [tlocus(reference_genome), tarray(tstr)])
    return construct_expr(ApplyMethod('LocusAlleles({})'.format(reference_genome.name), s._ast), t,
                          s._indices, s._aggregations, s._joins, s._refs)


@typecheck(gp=expr_array)
def gp_dosage(gp):
    """
    Return expected genotype dosage from array of genotype probabilities.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.gp_dosage([0.0, 0.5, 0.5]))
        1.5

    Notes
    -----
    This function is only defined for bi-allelic variants. The `gp` argument
    must be length 3. The value is ``gp[1] + 2 * gp[2]``.

    Parameters
    ----------
    gp : :class:`.ArrayFloat64Expression`
        Length 3 array of bi-allelic genotype probabilities

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    if not is_numeric(gp.dtype.element_type):
        raise TypeError("'gp_dosage' expects an expression of type "
                        "'Array[Float64]'. Found '{}'".format(gp.dtype))
    return _func("dosage", tfloat64, gp)


@typecheck(pl=expr_array)
def pl_dosage(pl):
    """
    Return expected genotype dosage from array of Phred-scaled genotype
    likelihoods with uniform prior. Only defined for bi-allelic variants. The
    `pl` argument must be length 3.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.pl_dosage([5, 10, 100]))
        0.24025307377482674

    Parameters
    ----------
    pl : :class:`.ArrayNumericExpression` of type :class:`.TInt32`
        Length 3 array of bi-allelic Phred-scaled genotype likelihoods

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    if not isinstance(pl.dtype.element_type, TInt32):
        raise TypeError("Function 'pl_dosage' expects an expression of type "
                        "'Array[Int32]'. Found {}".format(pl.dtype))
    return _func("plDosage", tfloat64, pl)


@typecheck(start=expr_locus, end=expr_locus)
def interval(start, end):
    """Construct an interval expression from two loci.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.interval(hl.locus("1", 100),
        ...                              hl.locus("1", 1000)))
        Interval(start=Locus(contig=1, position=100, reference_genome=GRCh37),
                 end=Locus(contig=1, position=1000, reference_genome=GRCh37))
    Parameters
    ----------
    start : :class:`.hail.genetics.Locus` or :class:`.LocusExpression`
        Starting locus (inclusive).
    end : :class:`.hail.genetics.Locus` or :class:`.LocusExpression`
        End locus (exclusive).
    reference_genome : :class:`.hail.genetics.GenomeReference` (optional)
        Reference genome to use (uses :meth:`.default_reference` if not passed).

    Returns
    -------
    :class:`.IntervalExpression`
    """
    start = to_expr(start)
    end = to_expr(end)

    indices, aggregations, joins, refs = unify_all(start, end)
    if not start._type._rg == end._type._rg:
        raise TypeError('Reference genome mismatch: {}, {}'.format(start._type._rg, end._type._rg))
    return construct_expr(
        ApplyMethod('Interval', start._ast, end._ast), tinterval(tlocus(start._type._rg)),
        indices, aggregations, joins, refs)


@typecheck(s=expr_str, reference_genome=nullable(GenomeReference))
def parse_interval(s, reference_genome=None):
    """Construct an interval expression by parsing a string or string expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.parse_interval('1:1000-2000'))
        Interval(start=Locus(contig=1, position=1000, reference_genome=GRCh37),
                 end=Locus(contig=1, position=2000, reference_genome=GRCh37))

        >>> hl.eval_expr(hl.parse_interval('1:start-10M'))
        Interval(start=Locus(contig=1, position=0, reference_genome=GRCh37),
                 end=Locus(contig=1, position=10000000, reference_genome=GRCh37))

    Notes
    -----
    This method expects strings of the form ``contig:start-end``, e.g. ``16:29500000-30200000``,
    ``8:start-end``, or ``X:10M-20M``.

    Parameters
    ----------
    s : str or :class:`.StringExpression`
        String to parse.
    reference_genome : :class:`.hail.genetics.GenomeReference` (optional)
        Reference genome to use (uses :meth:`.default_reference` if not passed).

    Returns
    -------
    :class:`.IntervalExpression`
    """
    s = to_expr(s)
    if reference_genome is None:
        reference_genome = Env.hc().default_reference
    return construct_expr(
        ApplyMethod('LocusInterval({})'.format(reference_genome.name), s._ast), tinterval(tlocus(reference_genome)),
        s._indices, s._aggregations, s._joins, s._refs)


@typecheck(phased=expr_bool,
           alleles=expr_int32)
def call(phased, *alleles):  # FIXME: Python 3 allows optional kwarg after varargs
    """Construct a call expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.call(True, 1, 0))
        Call(alleles=[1, 0], phased=True)

    Parameters
    ----------
    phased : :obj:`bool`
        If ``True``, preserve the order of `alleles`.
    alleles : variable-length args of :obj:`int` or :class:`.Expression` of type :class:`.TInt32`
        List of allele indices.

    Returns
    -------
    :class:`.CallExpression`
    """

    indices, aggregations, joins, refs = unify_all(phased, *alleles)
    if std_len(alleles) > 2:
        raise NotImplementedError("`call' supports a maximum of 2 alleles.")
    return construct_expr(ApplyMethod('Call', phased._ast, *[a._ast for a in alleles]), tcall, indices, aggregations,
                          joins, refs)


@typecheck(gt_index=expr_int32)
def unphased_diploid_gt_index_call(gt_index):
    """Construct an unphased, diploid call from a genotype index.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.unphased_diploid_gt_index_call(4))
        Call(alleles=[1, 2], phased=False)

    Parameters
    ----------
    gt_index : :obj:`int` or :class:`.Expression` of type :class:`.TInt32`
        Unphased, diploid genotype index.

    Returns
    -------
    :class:`.CallExpression`
    """
    gt_index = to_expr(gt_index)
    return construct_expr(ApplyMethod('UnphasedDiploidGtIndexCall', gt_index._ast), tcall, gt_index._indices,
                          gt_index._aggregations, gt_index._joins, gt_index._refs)


@typecheck(s=expr_str)
def parse_call(s):
    """Construct a call expression by parsing a string or string expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.parse_call('0|2'))
        Call([0, 2], phased=True)

    Notes
    -----
    This method expects strings in the following format:

    +--------+-------------------------+--------------+
    | ploidy | Phased                  | Unphased     |
    +========+=========================+==============+
    |   0    | &#124;-                 |  -           |
    +--------+-------------------------+--------------+
    |   1    | &#124;i                 |  i           |
    +--------+-------------------------+--------------+
    |   2    | i&#124;j                |  i/j         |
    +--------+-------------------------+--------------+
    |   3    | i&#124;j&#124;k         |  i/j/k       |
    +--------+-------------------------+--------------+
    |   N    | i&#124;j&#124;...&#124;N| i/j/k/.../N  |
    +------+------+-----------------------------------+

    Parameters
    ----------
    s : str or :class:`.StringExpression`
        String to parse.

    Returns
    -------
    :class:`.CallExpression`
    """
    s = to_expr(s)
    return construct_expr(ApplyMethod('Call', s._ast), tcall, s._indices, s._aggregations, s._joins, s._refs)


@typecheck(expression=expr_any)
def is_defined(expression):
    """Returns ``True`` if the argument is not missing.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_defined(5))
        True

        >>> hl.eval_expr(hl.is_defined(hl.null(hl.tstr)))
        False

        >>> hl.eval_expr(hl.is_defined(hl.null(hl.tbool) & True))
        False

    Parameters
    ----------
    expression
        Expression to test.

    Returns
    -------
    :class:`.BooleanExpression`
        ``True`` if `expression` is not missing, ``False`` otherwise.
    """
    return _func("isDefined", tbool, expression)


@typecheck(expression=expr_any)
def is_missing(expression):
    """Returns ``True`` if the argument is missing.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_missing(5))
        False

        >>> hl.eval_expr(hl.is_missing(hl.null(hl.tstr)))
        True

        >>> hl.eval_expr(hl.is_missing(hl.null(hl.tbool) & True))
        True

    Parameters
    ----------
    expression
        Expression to test.

    Returns
    -------
    :class:`.BooleanExpression`
        ``True`` if `expression` is missing, ``False`` otherwise.
    """
    return _func("isMissing", tbool, expression)


@typecheck(x=expr_numeric)
def is_nan(x):
    """Returns ``True`` if the argument is ``NaN`` (not a number).

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_nan(0))
        False

        >>> hl.eval_expr(hl.is_nan(hl.capture(0) / 0))
        True

        >>> hl.eval_expr(hl.is_nan(hl.capture(0) / hl.null(hl.tfloat64)))
        None

    Notes
    -----
    Note that :meth:`.is_missing` will return ``False`` on ``NaN`` since ``NaN``
    is a defined value. Additionally, this method will return missing if `x` is
    missing.

    Parameters
    ----------
    x : float or :class:`.Expression` of type :class:`.TFloat64`
        Expression to test.

    Returns
    -------
    :class:`.BooleanExpression`
        ``True`` if `x` is ``NaN``, ``False`` otherwise.
    """
    return _func("isnan", tbool, x)


@typecheck(x=expr_any)
def json(x):
    """Convert an expression to a JSON string expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.json([1,2,3,4,5]))
        '[1,2,3,4,5]'

        >>> hl.eval_expr(hl.json(hl.Struct(a='Hello', b=0.12345, c=[1,2], d={'hi', 'bye'})))
        '{"a":"Hello","c":[1,2],"b":0.12345,"d":["bye","hi"]}'

    Parameters
    ----------
    x
        Expression to convert.

    Returns
    -------
    :class:`.StringExpression`
        String expression with JSON representation of `x`.
    """
    return _func("json", tstr, x)


@typecheck(x=expr_numeric, base=nullable(expr_numeric))
def log(x, base=None):
    """Take the logarithm of the `x` with base `base`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.log(10))
        2.302585092994046

        >>> hl.eval_expr(hl.log(10, 10))
        1.0

        >>> hl.eval_expr(hl.log(1024, 2))
        10.0

    Notes
    -----
    If the `base` argument is not supplied, then the natural logarithm is used.

    Parameters
    ----------
    x : float or :class:`.Expression` of type :class:`.TFloat64`
    base : float or :class:`.Expression` of type :class:`.TFloat64`

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    x = to_expr(x)
    if base is not None:
        return _func("log", tfloat64, x, to_expr(base))
    else:
        return _func("log", tfloat64, x)


@typecheck(x=expr_numeric)
def log10(x):
    """Take the logarithm of the `x` with base 10.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.log10(1000))
        3.0

        >>> hl.eval_expr(hl.log10(0.0001123))
        -3.949620243738542

    Parameters
    ----------
    x : float or :class:`.Expression` of type :class:`.TFloat64`

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("log10", tfloat64, x)


@typecheck(a=expr_any, b=expr_any)
def or_else(a, b):
    """If `a` is missing, return `b`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.or_else(5, 7))
        5

        >>> hl.eval_expr(hl.or_else(hl.null(hl.tint32), 7))
        7

    Parameters
    ----------
    a
    b

    Returns
    -------
    :class:`.Expression`
    """
    t = unify_types(a._type, b._type)
    if t is None:
        raise TypeError("'or_else' requires the 'a' and 'b' arguments to have the same type\n"
                        "    a: type {}\n"
                        "    b:  type {}".format(a._type, b._type))
    return _func("orElse", t, a, b)


@typecheck(predicate=expr_bool, value=expr_any)
def or_missing(predicate, value):
    """Returns `value` if `predicate` is ``True``, otherwise returns missing.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.or_missing(True, 5))
        5

        >>> hl.eval_expr(hl.or_missing(False, 5))
        None

    Parameters
    ----------
    predicate : bool or :class:`.BooleanExpression`
    value : Value to return if `predicate` is true.

    Returns
    -------
    :class:`.Expression`
        This expression has the same type as `b`.
    """
    predicate = to_expr(predicate)
    return _func("orMissing", value._type, predicate, value)


@typecheck(x=expr_int32, n=expr_int32, p=expr_numeric,
           alternative=enumeration("two.sided", "greater", "less"))
def binom_test(x, n, p, alternative):
    """Performs a binomial test on `p` given `x` successes in `n` trials.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.binom_test(5, 10, 0.5, 'less'))
        0.6230468749999999

    With alternative ``less``, the p-value is the probability of at most `x`
    successes, i.e. the cumulative probability at `x` of the distribution
    Binom(`n`, `p`). With ``greater``, the p-value is the probably of at least
    `x` successes. With ``two.sided``, the p-value is the total probability of
    all outcomes with probability at most that of `x`.

    Returns the p-value from the `exact binomial test
    <https://en.wikipedia.org/wiki/Binomial_test>`__ of the null hypothesis that
    success has probability `p`, given `x` successes in `n` trials.

    Parameters
    ----------
    x : int or :class:`.Expression` of type :class:`.TInt32`
        Number of successes.
    n : int or :class:`.Expression` of type :class:`.TInt32`
        Number of trials.
    p : float or :class:`.Expression` of type :class:`.TFloat64`
        Probability of success, between 0 and 1.
    alternative
        : One of, "two.sided", "greater", "less".

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
        p-value.
    """
    return _func("binomTest", tfloat64, x, n, p, alternative)


@typecheck(x=expr_numeric, df=expr_numeric)
def pchisqtail(x, df):
    """Returns the probability under the right-tail starting at x for a chi-squared
    distribution with df degrees of freedom.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.pchisqtail(5, 1))
        0.025347318677468304

    Parameters
    ----------
    x : float or :class:`.Expression` of type :class:`.TFloat64`
    df : float or :class:`.Expression` of type :class:`.TFloat64`
        Degrees of freedom.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("pchisqtail", tfloat64, x, df)


@typecheck(x=expr_numeric)
def pnorm(x):
    """The cumulative probability function of a standard normal distribution.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.pnorm(0))
        0.5

        >>> hl.eval_expr(hl.pnorm(1))
        0.8413447460685429

        >>> hl.eval_expr(hl.pnorm(2))
        0.9772498680518208

    Notes
    -----
    Returns the left-tail probability `p` = Prob(:math:Z < x) with :math:Z a standard normal random variable.

    Parameters
    ----------
    x : float or :class:`.Expression` of type :class:`.TFloat64`

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("pnorm", tfloat64, x)


@typecheck(x=expr_numeric, lamb=expr_numeric, lower_tail=expr_bool, log_p=expr_bool)
def ppois(x, lamb, lower_tail=True, log_p=False):
    """The cumulative probability function of a Poisson distribution.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.ppois(2, 1))
        0.9196986029286058

    Notes
    -----
    If `lower_tail` is true, returns Prob(:math:`X \leq` `x`) where :math:`X` is a
    Poisson random variable with rate parameter `lamb`. If `lower_tail` is false,
    returns Prob(:math:`X` > `x`).

    Parameters
    ----------
    x : float or :class:`.Expression` of type :class:`.TFloat64`
    lamb : float or :class:`.Expression` of type :class:`.TFloat64`
        Rate parameter of Poisson distribution.
    lower_tail : bool or :class:`.BooleanExpression`
        If ``True``, compute the probability of an outcome at or below `x`,
        otherwise greater than `x`.
    log_p : bool or :class:`.BooleanExpression`
        Return the natural logarithm of the probability.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("ppois", tfloat64, x, lamb, lower_tail, log_p)


@typecheck(p=expr_numeric, df=expr_numeric)
def qchisqtail(p, df):
    """Inverts :meth:`.pchisqtail`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.qchisqtail(0.01, 1))
        6.634896601021213

    Notes
    -----
    Returns right-quantile `x` for which `p` = Prob(:math:`Z^2` > x) with :math:`Z^2` a chi-squared random
     variable with degrees of freedom specified by `df`. `p` must satisfy 0 < `p` <= 1.

    Parameters
    ----------
    p : float or :class:`.Expression` of type :class:`.TFloat64`
        Probability.
    df : float or :class:`.Expression` of type :class:`.TFloat64`
        Degrees of freedom.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("qchisqtail", tfloat64, p, df)


@typecheck(p=expr_numeric)
def qnorm(p):
    """Inverts :meth:`.pnorm`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.qnorm(0.90))
        1.2815515655446008

    Notes
    -----
    Returns left-quantile `x` for which p = Prob(:math:`Z` < x) with :math:`Z` a standard normal random variable.
    `p` must satisfy 0 < `p` < 1.

    Parameters
    ----------
    p : float or :class:`.Expression` of type :class:`.TFloat64`
        Probability.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("qnorm", tfloat64, p)


@typecheck(p=expr_numeric, lamb=expr_numeric, lower_tail=expr_bool, log_p=expr_bool)
def qpois(p, lamb, lower_tail=True, log_p=False):
    """Inverts :meth:`.ppois`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.qpois(0.99, 1))
        4

    Notes
    -----
    Returns the smallest integer :math:`x` such that Prob(:math:`X \leq x`) :math:`\geq` `p` where :math:`X`
    is a Poisson random variable with rate parameter `lambda`.

    Parameters
    ----------
    p : float or :class:`.Expression` of type :class:`.TFloat64`
    lamb : float or :class:`.Expression` of type :class:`.TFloat64`
        Rate parameter of Poisson distribution.
    lower_tail : bool or :class:`.BooleanExpression`
        Corresponds to `lower_tail` parameter in inverse :meth:`.ppois`.
    log_p : bool or :class:`.BooleanExpression`
        Exponentiate `p` before testing.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("qpois", tint32, p, lamb, lower_tail, log_p)


@typecheck(start=expr_int32, stop=expr_int32, step=expr_int32)
def range(start, stop, step=1):
    """Returns an array of integers from `start` to `stop` by `step`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.range(0, 10))
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        >>> hl.eval_expr(hl.range(0, 10, step=3))
        [0, 3, 6, 9]

    Notes
    -----
    The range includes `start`, but excludes `stop`.

    Parameters
    ----------
    start : int or :class:`.Expression` of type :class:`.TInt32`
        Start of range.
    stop : int or :class:`.Expression` of type :class:`.TInt32`
        End of range.
    step : int or :class:`.Expression` of type :class:`.TInt32`
        Step of range.

    Returns
    -------
    :class:`.ArrayInt32Expression`
    """
    return _func("range", tarray(tint32), start, stop, step)


@typecheck(p=expr_numeric)
def rand_bool(p):
    """Returns ``True`` with probability `p` (RNG).

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.rand_bool(0.5))
        True

        >>> hl.eval_expr(hl.rand_bool(0.5))
        False

    Warning
    -------
    This function is non-deterministic, meaning that successive runs of the same pipeline including
    RNG expressions may return different results. This is a known bug.

    Parameters
    ----------
    p : float or :class:`.Expression` of type :class:`.TFloat64`
        Probability between 0 and 1.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("pcoin", tbool, p)


@typecheck(mean=expr_numeric, sd=expr_numeric)
def rand_norm(mean=0, sd=1):
    """Samples from a normal distribution with mean `mean` and standard deviation `sd` (RNG).

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.rand_norm())
        1.5388475315213386

        >>> hl.eval_expr(hl.rand_norm())
        -0.3006188509144124

    Warning
    -------
    This function is non-deterministic, meaning that successive runs of the same
    pipeline including RNG expressions may return different results. This is a known
    bug.

    Parameters
    ----------
    mean : float or :class:`.Expression` of type :class:`.TFloat64`
        Mean of normal distribution.
    sd : float or :class:`.Expression` of type :class:`.TFloat64`
        Standard deviation of normal distribution.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("rnorm", tfloat64, mean, sd)


@typecheck(lamb=expr_numeric)
def rand_pois(lamb):
    """Samples from a Poisson distribution with rate parameter `lamb` (RNG).

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.rand_pois(1))
        2.0

        >>> hl.eval_expr(hl.rand_pois(1))
        3.0

    Warning
    -------
    This function is non-deterministic, meaning that successive runs of the same
    pipeline including RNG expressions may return different results. This is a known
    bug.

    Parameters
    ----------
    lamb : float or :class:`.Expression` of type :class:`.TFloat64`
        Rate parameter for Poisson distribution.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("rpois", tfloat64, lamb)


@typecheck(min=expr_numeric, max=expr_numeric)
def rand_unif(min, max):
    """Returns a random floating-point number uniformly drawn from the interval [`min`, `max`].

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.rand_unif(0, 1))
        0.7983073825816226

        >>> hl.eval_expr(hl.rand_unif(0, 1))
        0.5161799497741769

    Warning
    -------
    This function is non-deterministic, meaning that successive runs of the same
    pipeline including RNG expressions may return different results. This is a known
    bug.

    Parameters
    ----------
    min : float or :class:`.Expression` of type :class:`.TFloat64`
        Left boundary of range.
    max : float or :class:`.Expression` of type :class:`.TFloat64`
        Right boundary of range.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("runif", tfloat64, min, max)


@typecheck(x=expr_numeric)
def sqrt(x):
    """Returns the square root of `x`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.sqrt(3))
        1.7320508075688772

    Notes
    -----
    It is also possible to exponentiate expression with standard Python syntax,
    e.g. ``x ** 0.5``.

    Parameters
    ----------
    x : float or :class:`.Expression` of type :class:`.TFloat64`

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    return _func("sqrt", tfloat64, x)


@typecheck(ref=expr_str, alt=expr_str)
def is_snp(ref, alt):
    """Returns ``True`` if the alleles constitute a single nucleotide polymorphism.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_snp('A', 'T'))
        True

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("is_snp", tbool, ref, alt)


@typecheck(ref=expr_str, alt=expr_str)
def is_mnp(ref, alt):
    """Returns ``True`` if the alleles constitute a multiple nucleotide polymorphism.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_mnp('AA', 'GT'))
        True

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("is_mnp", tbool, ref, alt)


@typecheck(ref=expr_str, alt=expr_str)
def is_transition(ref, alt):
    """Returns ``True`` if the alleles constitute a transition.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_transition('A', 'T'))
        False

        >>> hl.eval_expr(hl.is_transition('A', 'G'))
        True

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("is_transition", tbool, ref, alt)


@typecheck(ref=expr_str, alt=expr_str)
def is_transversion(ref, alt):
    """Returns ``True`` if the alleles constitute a transversion.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_transition('A', 'T'))
        True

        >>> hl.eval_expr(hl.is_transition('A', 'G'))
        False

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("is_transversion", tbool, ref, alt)


@typecheck(ref=expr_str, alt=expr_str)
def is_insertion(ref, alt):
    """Returns ``True`` if the alleles constitute an insertion.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_insertion('A', 'ATT'))
        True

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("is_insertion", tbool, ref, alt)


@typecheck(ref=expr_str, alt=expr_str)
def is_deletion(ref, alt):
    """Returns ``True`` if the alleles constitute a deletion.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_deletion('ATT', 'A'))
        True

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("is_deletion", tbool, ref, alt)


@typecheck(ref=expr_str, alt=expr_str)
def is_indel(ref, alt):
    """Returns ``True`` if the alleles constitute an insertion or deletion.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_indel('ATT', 'A'))
        True

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("is_indel", tbool, ref, alt)


@typecheck(ref=expr_str, alt=expr_str)
def is_star(ref, alt):
    """Returns ``True`` if the alleles constitute an upstream deletion.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_deletion('A', '*'))
        True

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("is_star", tbool, ref, alt)


@typecheck(ref=expr_str, alt=expr_str)
def is_complex(ref, alt):
    """Returns ``True`` if the alleles constitute a complex polymorphism.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_deletion('ATT', 'GCA'))
        True

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return _func("is_complex", tbool, ref, alt)


@typecheck(ref=expr_str, alt=expr_str)
def is_strand_ambiguous(ref, alt):
    """Returns ``True`` if the alleles are strand ambiguous.

    Strand ambiguous allele pairs are as follows:

    - ("A", "T")
    - ("T", "A")
    - ("G", "C")
    - ("C", "G")

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.is_strand_ambiguous('A', 'T'))
        True

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    alleles = hl.capture({("A", "T"), ("T", "A"), ("G", "C"), ("C", "G")})
    return alleles.contains((ref, alt))


@typecheck(ref=expr_str, alt=expr_str)
def allele_type(ref, alt):
    """Returns the type of the polymorphism as a string.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.allele_type('A', 'T'))
        'SNP'

        >>> hl.eval_expr(hl.allele_type('ATT', 'A'))
        'Deletion'

    Notes
    -----
    The possible return values are:
     - ``"SNP"``
     - ``"MNP"``
     - ``"Insertion"``
     - ``"Deletion"``
     - ``"Complex"``
     - ``"Star"``

    Parameters
    ----------
    ref : :class:`.StringExpression`
        Reference allele.
    alt : :class:`.StringExpression`
        Alternate allele.

    Returns
    -------
    :class:`.StringExpression`
    """
    return _func("allele_type", tstr, ref, alt)


@typecheck(s1=expr_str, s2=expr_str)
def hamming(s1, s2):
    """Returns the Hamming distance between the two strings.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.hamming('ATATA', 'ATGCA'))
        2

        >>> hl.eval_expr(hl.hamming('abcdefg', 'zzcdefz'))
        3

    Notes
    -----
    This method will fail if the two strings have different length.

    Parameters
    ----------
    s1 : :class:`.StringExpression`
        First string.
    s2 : :class:`.StringExpression`
        Second string.

    Returns
    -------
    :class:`.Expression` of type :class:`.TInt32`
    """
    return _func("hamming", tint32, s1, s2)


@typecheck(x=expr_any)
def str(x):
    """Returns the string representation of `x`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.str(hl.Struct(a=5, b=7)))
        '{"a":5,"b":7}'

    Parameters
    ----------
    x

    Returns
    -------
    :class:`.StringExpression`
    """
    return _func("str", tstr, x)


@typecheck(c=expr_call, i=expr_int32)
def downcode(c, i):
    """Create a new call by setting all alleles other than i to ref

    Examples
    --------
    Preserve the third allele and downcode all other alleles to reference.

    .. doctest::

        >>> hl.eval_expr(hl.downcode(hl.call([1, 2]), 2))
        Call(alleles=[0, 2], phased=False)

    Parameters
    ----------
    c : :class:`.CallExpression`
        A call.
    i : :class:`.Expression` of type :class:`.TInt32`
        The index of the allele that will be sent to the alternate allele. All
        other alleles will be downcoded to reference.

    Returns
    -------
    :class:`.CallExpression`
    """
    return _func("downcode", tcall, c, i)


@typecheck(pl=expr_array)
def gq_from_pl(pl):
    """Compute genotype quality from Phred-scaled probability likelihoods.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.gq_from_pl([0,69,1035]))
        69

    Parameters
    ----------
    pl : :class:`.ArrayInt32Expression`

    Returns
    -------
    :class:`.Expression` of type :class:`.TInt32`
    """
    if not isinstance(pl.dtype.element_type, TInt32):
        raise TypeError("'gq_from_pl' expects an array with element type 'Int32', found '{}'"
                        .format(pl.dtype
                                ))
    return _func("gqFromPL", tint32, pl)


@typecheck(n=expr_int32)
def triangle(n):
    """Returns the triangle number of `n`.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.triangle(3))
        6

    Notes
    -----
    The calculation is ``n * (n + 1) / 2``.

    Parameters
    ----------
    n : :class:`.Expression` of type :class:`.TInt32`

    Returns
    -------
    :class:`.Expression` of type :class:`.TInt32`
    """
    return _func("triangle", tint32, n)


@typecheck(f=func_spec(1, expr_bool),
           collection=oneof(expr_set, expr_array))
def filter(f, collection):
    """Returns a new collection containing elements where `f` returns ``True``.

    Examples
    --------
    .. doctest::

        >>> a = [1, 2, 3, 4]
        >>> s = {'Alice', 'Bob', 'Charlie'}

        >>> hl.eval_expr(hl.filter(lambda x: x % 2 == 0, a))
        [2, 4]

        >>> hl.eval_expr(hl.filter(lambda x: ~(x[-1] == 'e'), s))
        {'Bob'}

    Notes
    -----
    Returns a same-type expression; evaluated on a :class:`.SetExpression`, returns a
    :class:`.SetExpression`. Evaluated on an :class:`.ArrayExpression`,
    returns an :class:`.ArrayExpression`.

    Parameters
    ----------
    f : function ( (arg) -> :class:`.BooleanExpression`)
        Function to evaluate for each element of the collection. Must return a
        :class:`.BooleanExpression`.
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`.
        Array or set expression to filter.

    Returns
    -------
    :class:`.ArrayExpression` or :class:`.SetExpression`
        Expression of the same type as `collection`.
    """
    return collection._bin_lambda_method("filter", f, collection.dtype.element_type, lambda _: collection.dtype)


@typecheck(f=func_spec(1, expr_bool),
           collection=oneof(expr_set, expr_array))
def any(f, collection):
    """Returns ``True`` if `f` returns ``True`` for any element.

    Examples
    --------
    .. doctest::

        >>> a = ['The', 'quick', 'brown', 'fox']
        >>> s = {1, 3, 5, 6, 7, 9}

        >>> hl.eval_expr(hl.any(lambda x: x[-1] == 'x', a))
        True

        >>> hl.eval_expr(hl.any(lambda x: x % 4 == 0, s))
        False

    Notes
    -----
    This method returns ``False`` for empty collections.

    Parameters
    ----------
    f : function ( (arg) -> :class:`.BooleanExpression`)
        Function to evaluate for each element of the collection. Must return a
        :class:`.BooleanExpression`.
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression.

    Returns
    -------
    :class:`.BooleanExpression`.
        ``True`` if `f` returns ``True`` for any element, ``False`` otherwise.
    """

    return collection._bin_lambda_method("exists", f, collection.dtype.element_type, lambda _: tbool)


@typecheck(f=func_spec(1, expr_bool),
           collection=oneof(expr_set, expr_array))
def all(f, collection):
    """Returns ``True`` if `f` returns ``True`` for every element.

    Examples
    --------
    .. doctest::

        >>> a = ['The', 'quick', 'brown', 'fox']
        >>> s = {1, 3, 5, 6, 7, 9}

        >>> hl.eval_expr(hl.all(lambda x: hl.len(x) > 3, a))
        False

        >>> hl.eval_expr(hl.all(lambda x: x < 10, s))
        True

    Notes
    -----
    This method returns ``True`` if the collection is empty.

    Parameters
    ----------
    f : function ( (arg) -> :class:`.BooleanExpression`)
        Function to evaluate for each element of the collection. Must return a
        :class:`.BooleanExpression`.
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression.

    Returns
    -------
    :class:`.BooleanExpression`.
        ``True`` if `f` returns ``True`` for every element, ``False`` otherwise.
    """

    return collection._bin_lambda_method("forall", f, collection.dtype.element_type, lambda _: tbool)


@typecheck(f=func_spec(1, expr_bool),
           collection=oneof(expr_set, expr_array))
def find(f, collection):
    """Returns the first element where `f` returns ``True``.

    Examples
    --------
    .. doctest::

        >>> a = ['The', 'quick', 'brown', 'fox']
        >>> s = {1, 3, 5, 6, 7, 9}

        >>> hl.eval_expr(hl.find(lambda x: x[-1] == 'x', a))
        'fox'

        >>> hl.eval_expr(hl.find(lambda x: x % 4 == 0, s))
        None

    Notes
    -----
    If `f` returns ``False`` for every element, then the result is missing.

    Sets are unordered. If `collection` is of type :class:`.TSet`, then the
    element returned comes from no guaranteed ordering.

    Parameters
    ----------
    f : function ( (arg) -> :class:`.BooleanExpression`)
        Function to evaluate for each element of the collection. Must return a
        :class:`.BooleanExpression`.
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression.

    Returns
    -------
    :class:`.Expression`
        Expression whose type is the element type of the collection.
    """

    return collection._bin_lambda_method("find", f,
                                         collection.dtype.element_type,
                                         lambda _: collection.dtype.element_type)


@typecheck(f=func_spec(1, expr_any),
           collection=oneof(expr_set, expr_array))
def flatmap(f, collection):
    """Map each element of the collection to a new collection, and flatten the results.

    Examples
    --------
    .. doctest::

        >>> a = [[0, 1], [1, 2], [4, 5, 6, 7]]

        >>> hl.eval_expr(hl.flatmap(lambda x: x[1:], a))
        [1, 2, 5, 6, 7]

    Parameters
    ----------
    f : function ( (arg) -> :class:`.CollectionExpression`)
        Function from the element type of the collection to the type of the
        collection. For instance, `flatmap` on a ``Set[String]`` should take
        a ``String`` and return a ``Set``.
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression.

    Returns
    -------
    :class:`.ArrayExpression` or :class:`.SetExpression`
    """
    expected_type, s = (TArray, 'Array') if isinstance(collection.dtype, TArray) else (TSet, 'Set')

    def unify_ret(t):
        if not isinstance(t, expected_type):
            raise TypeError("'flatmap' expects 'f' to return an expression of type '{}', found '{}'".format(s, t))
        return t

    return collection._bin_lambda_method("flatMap", f, collection.dtype.element_type, unify_ret)


@typecheck(f=func_spec(1, expr_any),
           collection=oneof(expr_set, expr_array))
def group_by(f, collection):
    """Group collection elements into a dict according to a lambda function.

    Examples
    --------
    .. doctest::

        >>> a = ['The', 'quick', 'brown', 'fox']

        >>> hl.eval_expr(hl.group_by(lambda x: hl.len(x), a))
        {5: ['quick', 'brown'], 3: ['The', 'fox']}

    Parameters
    ----------
    f : function ( (arg) -> :class:`.Expression`)
        Function to evaluate for each element of the collection to produce a key for the
        resulting dictionary.
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression.

    Returns
    -------
    :class:`.DictExpression`.
        Dictionary keyed by results of `f`.
    """
    return collection._bin_lambda_method("groupBy", f,
                                         collection.dtype.element_type,
                                         lambda t: tdict(t, collection.dtype))


@typecheck(f=func_spec(1, expr_any),
           collection=oneof(expr_set, expr_array))
def map(f, collection):
    """Transform each element of a collection.

    Examples
    --------
    .. doctest::

        >>> a = ['The', 'quick', 'brown', 'fox']

        >>> hl.eval_expr(hl.map(lambda x: hl.len(x), a))
        [3, 5, 5, 3]

    Parameters
    ----------
    f : function ( (arg) -> :class:`.Expression`)
        Function to transform each element of the collection.
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression.

    Returns
    -------
    :class:`.ArrayExpression` or :class:`SetExpression`.
        Collection where each element has been transformed by `f`.
    """
    return collection._bin_lambda_method("map", f,
                                         collection.dtype.element_type,
                                         lambda t: collection.dtype.__class__(t))


@typecheck(x=oneof(expr_set, expr_array, expr_dict, expr_str))
def len(x):
    """Returns the size of a collection or string.

    Examples
    --------
    .. doctest::

        >>> a = ['The', 'quick', 'brown', 'fox']
        >>> s = {1, 3, 5, 6, 7, 9}

        >>> hl.eval_expr(hl.len(a))
        4

        >>> hl.eval_expr(hl.len(s))
        6

        >>> hl.eval_expr(hl.len("12345"))
        5

    Parameters
    ----------
    x : :class:`.ArrayExpression` or :class:`.SetExpression` or :class:`.DictExpression` or :class:`.StringExpression`
        String or collection expression.

    Returns
    -------
    :class:`.Expression` of type :class:`.TInt32`
    """
    return x._method("size", tint32)


@typecheck(exprs=oneof(expr_numeric, expr_set, expr_array))
def max(*exprs):
    """Returns the maximum element of a collection or of given numeric expressions.

    Examples
    --------
    .. doctest::

        Take the maximum value of an array:

        >>> hl.eval_expr(hl.max([1, 3, 5, 6, 7, 9]))
        9

        Take the maximum value of values:

        >>> hl.eval_expr(hl.max(1, 50, 2))
        50

    Notes
    -----
    Like the Python builtin ``max`` function, this function can either take a
    single iterable expression (an array or set of numeric elements), or
    variable-length arguments of numeric expressions.

    Parameters
    ----------
    exprs : :class:`.ArrayExpression` or class:`.SetExpression` or varargs of :class:`.NumericExpression`
        Single numeric array or set, or multiple numeric values.

    Returns
    -------
    :class:`.NumericExpression`
    """
    if std_len(exprs) < 1:
        raise ValueError("'max' requires at least one argument")
    if std_len(exprs) == 1:
        expr = exprs[0]
        if not ((isinstance(expr.dtype, TSet) or isinstance(expr.dtype, TArray))
                and is_numeric(expr.dtype.element_type)):
            raise TypeError("'max' expects a single numeric array expression or multiple numeric expressions\n"
                            "  Found 1 argument of type '{}'".format(expr.dtype))
        return expr._method('max', expr.dtype.element_type)
    else:
        if not std_all(is_numeric(e.dtype) for e in exprs):
            raise TypeError("'max' expects a single numeric array expression or multiple numeric expressions\n"
                            "  Found {} arguments with types '{}'".format(std_len(exprs), ', '.join(
                "'{}'".format(e.dtype) for e in exprs)))
        ret_t = unify_types(*(e.dtype for e in exprs))
        if std_len(exprs) == 2:
            return exprs[0]._method('max', ret_t, exprs[1])
        else:
            return max([e for e in exprs])


@typecheck(exprs=oneof(expr_numeric, expr_set, expr_array))
def min(*exprs):
    """Returns the minimum of a collection or of given numeric expressions.

    Examples
    --------
    .. doctest::

        Take the minimum value of an array:

        >>> hl.eval_expr(hl.max([2, 3, 5, 6, 7, 9]))
        2

        Take the minimum value:

        >>> hl.eval_expr(hl.max(12, 50, 2))
        2

    Notes
    -----
    Like the Python builtin ``min`` function, this function can either take a
    single iterable expression (an array or set of numeric elements), or
    variable-length arguments of numeric expressions.

    Parameters
    ----------
    exprs : :class:`.ArrayExpression` or class:`.SetExpression` or varargs of :class:`.NumericExpression`
        Single numeric array or set, or multiple numeric values.

    Returns
    -------
    :class:`.NumericExpression`
    """
    if std_len(exprs) < 1:
        raise ValueError("'min' requires at least one argument")
    if std_len(exprs) == 1:
        expr = exprs[0]
        if not ((isinstance(expr.dtype, TSet) or isinstance(expr.dtype, TArray))
                and is_numeric(expr.dtype.element_type)):
            raise TypeError("'min' expects a single numeric array expression or multiple numeric expressions\n"
                            "  Found 1 argument of type '{}'".format(expr.dtype))
        return expr._method('min', expr.dtype.element_type)
    else:
        if not std_all(is_numeric(e.dtype) for e in exprs):
            raise TypeError("'min' expects a single numeric array expression or multiple numeric expressions\n"
                            "  Found {} arguments with types '{}'".format(std_len(exprs), ', '.join(
                "'{}'".format(e.dtype) for e in exprs)))
        ret_t = unify_types(*(e.dtype for e in exprs))
        if std_len(exprs) == 2:
            return exprs[0]._method('min', ret_t, exprs[1])
        else:
            return min([e for e in exprs])


@typecheck(x=oneof(expr_numeric, expr_array))
def abs(x):
    """Take the absolute value of a numeric value or array.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.abs(-5))
        5

        >>> hl.eval_expr(hl.abs([1.0, -2.5, -5.1]))
        [1.0, 2.5, 5.1]

    Parameters
    ----------
    x : :class:`.NumericExpression` or :class:`.ArrayNumericExpression`

    Returns
    -------
    :class:`.NumericExpression` or :class:`.ArrayNumericExpression`.
    """
    if isinstance(x.dtype, TArray):
        if not is_numeric(x.dtype.element_type):
            raise TypeError(
                "'abs' expects a numeric expression or numeric array expression, found '{}'".format(x.dtype))
        return map(abs, x)
    else:
        return x._method('abs', x.dtype)


@typecheck(x=oneof(expr_numeric, expr_array))
def signum(x):
    """Returns the sign (1, 0, or -1) of a numeric value or array.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.signum(-1.23))
        -1

        >>> hl.eval_expr(hl.signum(555))
        1

        >>> hl.eval_expr(hl.signum(0.0))
        0

        >>> hl.eval_expr(hl.signum([1, -5, 0, -125]))
        [1, -1, 0, -1]

    Parameters
    ----------
    x : :class:`.NumericExpression` or :class:`.ArrayNumericExpression`

    Returns
    -------
    :class:`.NumericExpression` or :class:`.ArrayNumericExpression`.
    """
    if isinstance(x.dtype, TArray):
        if not is_numeric(x.dtype.element_type):
            raise TypeError(
                "'signum' expects a numeric expression or numeric array expression, found '{}'".format(x.dtype))
        return map(signum, x)
    else:
        return x._method('signum', tint32)


@typecheck(collection=oneof(expr_set, expr_array))
def mean(collection):
    """Returns the mean of all values in the collection.

    Examples
    --------
    .. doctest::

        >>> a = [1, 3, 5, 6, 7, 9]

        >>> hl.eval_expr(hl.mean(a))
        5.2

    Note
    ----
    Missing elements are ignored.

    Parameters
    ----------
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression with numeric element type.

    Returns
    -------
    :class:`.Expression` of type :class:`.TFloat64`
    """
    if not is_numeric(collection.dtype.element_type):
        raise TypeError("'mean' expects a numeric collection, found '{}'".format(collection.dtype))
    return collection._method("mean", tfloat64)


@typecheck(collection=oneof(expr_set, expr_array))
def median(collection):
    """Returns the median value in the collection.

    Examples
    --------
    .. doctest::

        >>> a = [1, 3, 5, 6, 7, 9]

        >>> hl.eval_expr(hl.median(a))
        5

    Note
    ----
    Missing elements are ignored.

    Parameters
    ----------
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression with numeric element type.

    Returns
    -------
    :class:`.NumericExpression`
    """
    if not is_numeric(collection.dtype.element_type):
        raise TypeError("'median' expects a numeric collection, found '{}'".format(collection.dtype))
    return collection._method("median", collection.dtype.element_type)


@typecheck(collection=oneof(expr_set, expr_array))
def product(collection):
    """Returns the product of values in the collection.

    Examples
    --------
    .. doctest::

        >>> a = [1, 3, 5, 6, 7, 9]

        >>> hl.eval_expr(hl.product(a))
        5670

    Note
    ----
    Missing elements are ignored.

    Parameters
    ----------
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression with numeric element type.

    Returns
    -------
    :class:`.NumericExpression`
    """
    if not is_numeric(collection.dtype.element_type):
        raise TypeError("'product' expects a numeric collection, found '{}'".format(collection.dtype))
    return collection._method("product", collection.dtype.element_type)


@typecheck(collection=oneof(expr_set, expr_array))
def sum(collection):
    """Returns the sum of values in the collection.

    Examples
    --------
    .. doctest::

        >>> a = [1, 3, 5, 6, 7, 9]

        >>> hl.eval_expr(hl.sum(a))
        31

    Note
    ----
    Missing elements are ignored.

    Parameters
    ----------
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection expression with numeric element type.

    Returns
    -------
    :class:`.NumericExpression`
    """
    if not is_numeric(collection.dtype.element_type):
        raise TypeError("'sum' expects a numeric collection, found '{}'".format(collection.dtype))
    return collection._method("sum", collection.dtype.element_type)


@typecheck(collection=oneof(expr_set, expr_array))
def set(collection):
    """Convert a set expression.

    Examples
    --------
    .. doctest::

        >>> a = ['Bob', 'Charlie', 'Alice', 'Bob', 'Bob']

        >>> hl.eval_expr(hl.set(a))
        {'Alice', 'Bob', 'Charlie'}

    Returns
    -------
    :class:`.SetExpression`
        Set of all unique elements.
    """
    if isinstance(collection.dtype, TSet):
        return collection
    return collection._method("toSet", tset(collection.dtype.element_type))


@typecheck(collection=oneof(expr_set, expr_array))
def array(collection):
    """Construct an array expression.

    Examples
    --------
    .. doctest::

        >>> s = {'Bob', 'Charlie', 'Alice'}

        >>> hl.eval_expr(hl.array(s))
        ['Charlie', 'Alice', 'Bob']

    Returns
    -------
    :class:`.ArrayExpression`
        Elements as an array.
    """
    if isinstance(collection.dtype, TArray):
        return collection
    return collection._method("toArray", tarray(collection.dtype.element_type))


@typecheck(collection=oneof(expr_set, expr_array))
def flatten(collection):
    """Flatten a nested collection by concatenating sub-collections.

    Examples
    --------
    .. doctest::

        >>> a = [[1, 2], [2, 3]]

        >>> hl.eval_expr(hl.flatten(a))
        [1, 2, 2, 3]

    Parameters
    ----------
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection with element type :class:`.TArray` or :class:`.TSet`.

    Returns
    -------
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
    """
    if (isinstance(collection.dtype, TSet) and not isinstance(collection.dtype.element_type, TSet)) or (
            isinstance(collection.dtype, TArray) and not isinstance(collection.dtype.element_type, TArray)):
        raise TypeError("'flatten' expects an expression of type 'Array[Array[_]]' or 'Set[Set[_]]', found '{}'"
                        .format(collection.dtype))
    return collection._method("flatten", collection._type.element_type)


@typecheck(collection=oneof(expr_array, expr_set),
           delimiter=expr_str)
def delimit(collection, delimiter=','):
    """Joins elements of `collection` into single string delimited by `delimiter`.

    Examples
    --------
    .. doctest::

        >>> a = ['Bob', 'Charlie', 'Alice', 'Bob', 'Bob']

        >>> hl.eval_expr(hl.delimit(a))
        'Bob,Charlie,Alice,Bob,Bob'

    Notes
    -----
    If the element type of `collection` is not :class:`.TString`, then the
    :func:`str` function will be called on each element before joining with
    the delimiter.

    Parameters
    ----------
    collection : :class:`.ArrayExpression` or :class:`.SetExpression`
        Collection.
    delimiter : str or :class:`.StringExpression`
        Field delimiter.

    Returns
    -------
    :class:`.StringExpression`
        Joined string expression.
    """
    if not isinstance(collection.dtype.element_type, TString):
        collection = map(str, collection)
    return collection._method("mkString", tstr, delimiter)


@typecheck(collection=expr_array,
           key=nullable(func_spec(1, expr_any)),
           reverse=expr_bool)
def sorted(collection, key=None, reverse=False):
    """Returns a sorted array.

    Examples
    --------
    .. doctest::

        >>> a = ['Charlie', 'Alice', 'Bob']

        >>> hl.eval_expr(hl.sorted(a))
        ['Alice', 'Bob', 'Charlie']

        >>> hl.eval_expr(hl.sorted(a, reverse=False))
        ['Charlie', 'Bob', 'Alice']

        >>> hl.eval_expr(hl.sorted(a, key=lambda x: hl.len(x)))
        ['Bob', 'Alice', 'Charlie']

    Notes
    -----
    The ordered types are :class:`.TString` and numeric types.

    Parameters
    ----------
    collection : :class:`.ArrayExpression`
        Array to sort.
    key: function ( (arg) -> :class:`.Expression`), optional
        Function to evaluate for each element to compute sort key.
    reverse : :class:`.BooleanExpression`
        Sort in descending order.

    Returns
    -------
    :class:`.ArrayNumericExpression`
        Sorted array.
    """
    ascending = ~reverse

    def can_sort(t):
        return isinstance(t, TString) or is_numeric(t)

    if key is None:
        if not can_sort(collection.dtype.element_type):
            raise TypeError("'sorted' expects an array with element type 'String' or numeric, found '{}'"
                            .format(collection.dtype))
        return collection._method("sort", collection.dtype, ascending)
    else:
        def check_f(t):
            if not can_sort(t):
                raise TypeError("'sort_by' expects 'key' to return type 'String' or numeric, found '{}'".format(t))
            return collection.dtype

        return collection._bin_lambda_method("sortBy", key, collection.dtype.element_type, check_f, ascending)


@typecheck(collection=expr_array)
def unique_min_index(collection):
    """Return the index of the minimum value in the array.

    Examples
    --------

    .. doctest::

        >>> hl.eval_expr(hl.unique_min_index([10, 0, 100]))
        1

        >>> hl.eval_expr(hl.unique_min_index([0, 0, 100]))
        None

    Notes
    -----
    If the minimum value is not unique, returns missing.

    Parameters
    ----------
    collection : :class:`.ArrayNumericExpression`
        Numeric array.

    Returns
    -------
    :class:`.Expression` of type :class:`.TInt32`
    """
    if not is_numeric(collection.dtype.element_type):
        raise TypeError("'unique_min_index' expects an array with numeric element type, found '{}'"
                        .format(collection.dtype))
    return collection._method("uniqueMinIndex", tint32)


@typecheck(collection=expr_array)
def unique_max_index(collection):
    """Return the index of the maximum value in the array.

    Examples
    --------

    .. doctest::

        >>> hl.eval_expr(hl.unique_max_index([0.2, 0.2, 0.6]))
        2

        >>> hl.eval_expr(hl.unique_max_index([0.4, 0.4, 0.2]))
        None

    Notes
    -----
    If the maximum value is not unique, returns missing.

    Parameters
    ----------
    collection : :class:`.ArrayNumericExpression`
        Numeric array.

    Returns
    -------
    :class:`.Expression` of type :class:`.TInt32`
    """
    if not is_numeric(collection.dtype.element_type):
        raise TypeError("'unique_max_index' expects an array with numeric element type, found '{}'"
                        .format(collection.dtype))
    return collection._method("uniqueMaxIndex", tint32)


@typecheck(expr=oneof(expr_numeric, expr_bool, expr_str))
def float64(expr):
    """Convert to a 64-bit floating point expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.float64('1.1'))
        1.1

        >>> hl.eval_expr(hl.float64(1))
        1.0

        >>> hl.eval_expr(hl.float64(True))
        1.0

    Parameters
    ----------
    expr : :class:`.NumericExpression` or :class:`.BooleanExpression` or :class:`.StringExpression`

    Returns
    -------
    :class:`.NumericExpression` of type :class:`.TFloat64`
    """
    return expr._method("toFloat64", tfloat64)


@typecheck(expr=oneof(expr_numeric, expr_bool, expr_str))
def float32(expr):
    """Convert to a 32-bit floating point expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.float32('1.1'))
        1.1

        >>> hl.eval_expr(hl.float32(1))
        1.0

        >>> hl.eval_expr(hl.float32(True))
        1.0

    Parameters
    ----------
    expr : :class:`.NumericExpression` or :class:`.BooleanExpression` or :class:`.StringExpression`

    Returns
    -------
    :class:`.NumericExpression` of type :class:`.TFloat32`
    """
    return expr._method("toFloat32", tfloat32)


@typecheck(expr=oneof(expr_numeric, expr_bool, expr_str))
def int64(expr):
    """Convert to a 64-bit integer expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.int64('1'))
        1

        >>> hl.eval_expr(hl.int64(1.5))
        1

        >>> hl.eval_expr(hl.int64(True))
        1

    Parameters
    ----------
    expr : :class:`.NumericExpression` or :class:`.BooleanExpression` or :class:`.StringExpression`

    Returns
    -------
    :class:`.NumericExpression` of type :class:`.TInt64`
    """
    return expr._method("toInt64", tint64)


@typecheck(expr=oneof(expr_numeric, expr_bool, expr_str))
def int32(expr):
    """Convert to a 32-bit integer expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.int32('1'))
        1

        >>> hl.eval_expr(hl.int32(1.5))
        1

        >>> hl.eval_expr(hl.int32(True))
        1

    Parameters
    ----------
    expr : :class:`.NumericExpression` or :class:`.BooleanExpression` or :class:`.StringExpression`

    Returns
    -------
    :class:`.NumericExpression` of type :class:`.TInt32`
    """
    return expr._method("toInt32", tint32)

@typecheck(expr=oneof(expr_numeric, expr_bool, expr_str))
def int(expr):
    """Convert to a 32-bit integer expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.int('1'))
        1

        >>> hl.eval_expr(hl.int(1.5))
        1

        >>> hl.eval_expr(hl.int(True))
        1

    Note
    ----
    Alias for :func:`.int32`.

    Parameters
    ----------
    expr : :class:`.NumericExpression` or :class:`.BooleanExpression` or :class:`.StringExpression`

    Returns
    -------
    :class:`.NumericExpression` of type :class:`.TInt32`
    """
    return int32(expr)

@typecheck(expr=oneof(expr_numeric, expr_bool, expr_str))
def float(expr):
    """Convert to a 64-bit floating point expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.float('1.1'))
        1.1

        >>> hl.eval_expr(hl.float(1))
        1.0

        >>> hl.eval_expr(hl.float(True))
        1.0

    Note
    ----
    Alias for :func:`.float64`.

    Parameters
    ----------
    expr : :class:`.NumericExpression` or :class:`.BooleanExpression` or :class:`.StringExpression`

    Returns
    -------
    :class:`.NumericExpression` of type :class:`.TFloat64`
    """
    return float64(expr)


@typecheck(expr=oneof(expr_numeric, expr_bool, expr_str))
def bool(expr):
    """Convert to a Boolean expression.

    Examples
    --------
    .. doctest::

        >>> hl.eval_expr(hl.bool('TRUE'))
        True

        >>> hl.eval_expr(hl.bool(1.5))
        True

    Notes
    -----
    Numeric expressions return ``True`` if they are non-zero, and ``False``
    if they are zero.

    Acceptable string values are: ``'True'``, ``'true'``, ``'TRUE'``,
    ``'False'``, ``'false'``, and ``'FALSE'``.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    if is_numeric(expr.dtype):
        return expr != 0
    else:
        return expr._method("toBoolean", tbool)
