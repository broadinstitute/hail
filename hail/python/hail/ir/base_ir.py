import abc

from typing import List, Set, Optional

from hail.utils.java import Env
from .renderer import Renderer, Renderable, RenderableStr


def _env_bind(env, *bindings):
    if bindings:
        if env:
            res = env.copy()
            res.update(bindings)
            return res
        else:
            return dict(bindings)
    else:
        return env


class BaseIR(Renderable):
    def __init__(self, *children):
        super().__init__()
        self._type = None
        self.children = children

    def __str__(self):
        r = Renderer(stop_at_jir=False)
        return r(self)

    def render_head(self, r):
        head_str = self.head_str()
        if head_str != '':
            head_str = f' {head_str}'
        return f'({self._ir_name()}{head_str}'

    def render_tail(self, r):
        return ')'

    def _ir_name(self):
        return self.__class__.__name__

    def render_children(self, r):
        return self.children

    def head_str(self):
        """String to be added after IR name in serialized representation.

        Returns
        -------
        str
        """
        return ''

    @abc.abstractmethod
    def parse(self, code, ref_map, ir_map):
        return

    @property
    @abc.abstractmethod
    def typ(self):
        return

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.children == other.children and self._eq(other)

    def __ne__(self, other):
        return not self == other

    def _eq(self, other):
        """Compare non-child-BaseIR attributes of the BaseIR.

        Parameters
        ----------
        other
            BaseIR of the same class.

        Returns
        -------
        bool
        """
        return True

    def __hash__(self):
        return 31 + hash(str(self))

    def new_block(self, i: int):
        return self.uses_agg_context(i) or self.uses_scan_context(i)

    def binds(self, i: int):
        return False

    def bindings(self, i: int):
        return []

    def agg_bindings(self, i: int):
        return []

    def scan_bindings(self, i: int):
        return []

    def uses_agg_context(self, i: int):
        return False

    def uses_scan_context(self, i: int):
        return False

    def child_context_without_bindings(self, i: int, parent_context):
        (eval_c, agg_c, scan_c) = parent_context
        if self.uses_agg_context(i):
            return (agg_c, None, None)
        elif self.uses_scan_context(i):
            return (scan_c, None, None)
        else:
            return parent_context

    def child_context(self, i: int, parent_context, default_value=None):
        base = self.child_context_without_bindings(i, parent_context)
        if not self.binds(i):
            return base
        eval_b = self.bindings(i)
        agg_b = self.agg_bindings(i)
        scan_b = self.scan_bindings(i)
        if default_value:
            eval_b = [(var, default_value) for (var, _) in eval_b]
            agg_b  = [(var, default_value) for (var, _) in agg_b]
            scan_b = [(var, default_value) for (var, _) in scan_b]
        (eval_c, agg_c, scan_c) = base
        return _env_bind(eval_c, *eval_b), _env_bind(agg_c, *agg_b), _env_bind(scan_c, *scan_b)


class IR(BaseIR):
    def __init__(self, *children):
        super().__init__(*children)
        self._aggregations = None

    @property
    def aggregations(self):
        if self._aggregations is None:
            self._aggregations = [agg for child in self.children for agg in child.aggregations]
        return self._aggregations

    @property
    def is_nested_field(self):
        return False

    def search(self, criteria):
        others = [node for child in self.children if isinstance(child, IR) for node in child.search(criteria)]
        if criteria(self):
            return others + [self]
        return others

    def copy(self, *args):
        raise NotImplementedError("IR has no copy method defined.")

    def map_ir(self, f):
        new_children = []
        for child in self.children:
            if isinstance(child, IR):
                new_children.append(f(child))
            else:
                new_children.append(child)

        return self.copy(*new_children)

    # FIXME: This is only used to compute the free variables in a subtree,
    # in an incorrect way. Should just define a free variables method directly.
    @property
    def bound_variables(self):
        return {v for child in self.children for v in child.bound_variables}

    @property
    def typ(self):
        if self._type is None:
            self._compute_type({}, None)
            assert self._type is not None, self
        return self._type

    @abc.abstractmethod
    def _compute_type(self, env, agg_env):
        raise NotImplementedError(self)

    def parse(self, code, ref_map={}, ir_map={}):
        return Env.hail().expr.ir.IRParser.parse_value_ir(
            code,
            {k: t._parsable_string() for k, t in ref_map.items()},
            ir_map)


class TableIR(BaseIR):
    def __init__(self, *children):
        super().__init__(*children)

    @abc.abstractmethod
    def _compute_type(self):
        ...

    @property
    def typ(self):
        if self._type is None:
            self._compute_type()
            assert self._type is not None, self
        return self._type

    def parse(self, code, ref_map={}, ir_map={}):
        return Env.hail().expr.ir.IRParser.parse_table_ir(code, ref_map, ir_map)

    global_env = {'global'}
    row_env = {'global', 'row'}


class MatrixIR(BaseIR):
    def __init__(self, *children):
        super().__init__(*children)

    @abc.abstractmethod
    def _compute_type(self):
        ...

    @property
    def typ(self):
        if self._type is None:
            self._compute_type()
            assert self._type is not None, self
        return self._type

    def parse(self, code, ref_map={}, ir_map={}):
        return Env.hail().expr.ir.IRParser.parse_matrix_ir(code, ref_map, ir_map)

    global_env = {'global'}
    row_env = {'global', 'va'}
    col_env = {'global', 'sa'}
    entry_env = {'global', 'sa', 'va', 'g'}


class BlockMatrixIR(BaseIR):
    def __init__(self, *children):
        super().__init__(*children)

    @abc.abstractmethod
    def _compute_type(self):
        ...

    @property
    def typ(self):
        if self._type is None:
            self._compute_type()
            assert self._type is not None, self
        return self._type

    def parse(self, code, ref_map={}, ir_map={}):
        return Env.hail().expr.ir.IRParser.parse_blockmatrix_ir(code, ref_map, ir_map)


class JIRVectorReference(object):
    def __init__(self, jid, length, item_type):
        self.jid = jid
        self.length = length
        self.item_type = item_type

    def __len__(self):
        return self.length

    def __del__(self):
        try:
            Env.hc()._jhc.pyRemoveIrVector(self.jid)
        # there is only so much we can do if the attempt to remove the unused IR fails,
        # especially since this will often get called during interpreter shutdown.
        except Exception:
            pass
