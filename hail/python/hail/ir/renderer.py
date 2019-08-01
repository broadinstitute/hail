from hail import ir
import abc
from typing import Sequence, List, Set, Dict


class Renderable(object):
    @abc.abstractmethod
    def render_head(self, r: 'Renderer') -> str:
        ...

    @abc.abstractmethod
    def render_tail(self, r: 'Renderer') -> str:
        ...

    @abc.abstractmethod
    def render_children(self, r: 'Renderer') -> Sequence['Renderable']:
        ...


class RenderableStr(Renderable):
    def __init__(self, s: str):
        self.s = s

    def render_head(self, r: 'Renderer') -> str:
        return self.s

    @abc.abstractmethod
    def render_tail(self, r: 'Renderer') -> str:
        return ''

    @abc.abstractmethod
    def render_children(self, r: 'Renderer') -> Sequence['Renderable']:
        return []


class ParensRenderer(Renderable):
    def __init__(self, rs: Sequence['Renderable']):
        self.rs = rs

    def render_head(self, r: 'Renderer') -> str:
        return '('

    @abc.abstractmethod
    def render_tail(self, r: 'Renderer') -> str:
        return ')'

    @abc.abstractmethod
    def render_children(self, r: 'Renderer') -> Sequence['Renderable']:
        return self.rs


class RenderableQueue(object):
    def __init__(self, elements: Sequence[Renderable], tail: str):
        self._elements = elements
        self._elements_len = len(elements)
        self.tail = tail
        self._idx = 0

    def exhausted(self):
        return self._elements_len == self._idx

    def pop(self) -> Renderable:
        idx = self._idx
        self._idx += 1
        return self._elements[idx]


class RQStack(object):
    def __init__(self):
        self._list: List[RenderableQueue] = []
        self._idx = -1

    def push(self, x: RenderableQueue):
        self._idx += 1
        self._list.append(x)

    def peek(self) -> RenderableQueue:
        return self._list[self._idx]

    def pop(self) -> RenderableQueue:
        self._idx -= 1
        return self._list.pop()

    def non_empty(self) -> bool:
        return self._idx >= 0

    def is_empty(self) -> bool:
        return self._idx < 0


class Renderer(object):
    def __init__(self, stop_at_jir=False):
        self.stop_at_jir = stop_at_jir
        self.count = 0
        self.jirs = {}

    def add_jir(self, jir):
        jir_id = f'm{self.count}'
        self.count += 1
        self.jirs[jir_id] = jir
        return jir_id

    def __call__(self, x: 'Renderable'):
        stack = RQStack()
        builder = []

        while x is not None or stack.non_empty():
            if x is not None:
                # TODO: it would be nice to put the JavaIR logic in BaseIR somewhere but this isn't trivial
                if self.stop_at_jir and hasattr(x, '_jir'):
                    jir_id = self.add_jir(x._jir)
                    if isinstance(x, ir.MatrixIR):
                        builder.append(f'(JavaMatrix {jir_id})')
                    elif isinstance(x, ir.TableIR):
                        builder.append(f'(JavaTable {jir_id})')
                    elif isinstance(x, ir.BlockMatrixIR):
                        builder.append(f'(JavaBlockMatrix {jir_id})')
                    else:
                        assert isinstance(x, ir.IR)
                        builder.append(f'(JavaIR {jir_id})')
                else:
                    head = x.render_head(self)
                    if head != '':
                        builder.append(x.render_head(self))
                    stack.push(RenderableQueue(x.render_children(self), x.render_tail(self)))
                x = None
            else:
                top = stack.peek()
                if top.exhausted():
                    stack.pop()
                    builder.append(top.tail)
                else:
                    builder.append(' ')
                    x = top.pop()

        return ''.join(builder)


class Scope:
    def __init__(self, depth: int):
        self.depth = depth
        self.visited: Dict[int, 'ir.BaseIR'] = {}
        self.lifted_lets: Dict[int, (str, 'ir.BaseIR')] = {}
        self.let_bodies: List[str] = []

class CSERenderer(Renderer):
    def __init__(self, stop_at_jir=False):
        self.stop_at_jir = stop_at_jir
        self.jir_count = 0
        self.jirs = {}
        self.memo: Dict[int, Sequence[str]] = {}
        self.uid_count = 0
        self.scopes: Dict[int, Scope] = {}

    def uid(self) -> str:
        self.uid_count += 1
        return f'__cse_{self.uid_count}'

    def add_jir(self, x: 'ir.BaseIR'):
        jir_id = f'm{self.jir_count}'
        self.jir_count += 1
        self.jirs[jir_id] = x._jir
        if isinstance(x, ir.MatrixIR):
            return f'(JavaMatrix {jir_id})'
        elif isinstance(x, ir.TableIR):
            return f'(JavaTable {jir_id})'
        elif isinstance(x, ir.BlockMatrixIR):
            return f'(JavaBlockMatrix {jir_id})'
        else:
            assert isinstance(x, ir.IR)
            return f'(JavaIR {jir_id})'

    @staticmethod
    def find_in_scope(x: 'ir.BaseIR', context: List[Scope], outermost_scope: int) -> int:
        for i in range(len(context) - 1, outermost_scope - 1, -1):
            if id(x) in context[i].visited:
                return i
        return -1

    @staticmethod
    def lifted_in_scope(x: 'ir.BaseIR', context: List[Scope]) -> int:
        for i in range(len(context) - 1, -1, -1):
            if id(x) in context[i].lifted_lets:
                return i
        return -1

    Context = (Dict[str, int], Dict[str, int], Dict[str, int])

    # class State:
    #     def __init__(self, scopes, outermost_scope, context):
    #         self.scopes = scopes
    #         self.outermost_scope = outermost_scope
    #         self.context = context
    #
    # def loop(self, state, x, free_vars, i):

    # Pre:
    # * 'context' is a list of 'Scope's, one for each potential let-insertion
    #   site.
    # * 'ref_to_scope' maps each bound variable to the index in 'context' of the
    #   scope of its binding site.
    # Post:
    # * Returns set of free variables in 'x'.
    # * Each subtree of 'x' is flagged as visited in the outermost scope
    #   containing all of its free variables.
    # * Each subtree previously visited (either an earlier subtree of 'x', or
    #   marked visited in 'context') is added to set of lets in its (previously
    #   computed) outermost scope.
    # * 'self.scopes' is updated to map subtrees y of 'x' to scopes containing
    #   any lets to be inserted above y.
    def recur(self, scopes: List[Scope], outermost_scope: int, context: Context, x: 'ir.BaseIR') -> Dict[str, int]:
        free_vars = {}
        depth = len(scopes)
        for i in range(len(x.children)):
            child = x.children[i]
            # FIXME: maintain a union of seen nodes in all scopes
            seen_in_scope = self.find_in_scope(child, scopes, outermost_scope)
            if seen_in_scope >= 0:
                # we've seen 'child' before, no need to traverse
                if isinstance(child, ir.IR):
                    if id(child) not in scopes[seen_in_scope].lifted_lets:
                        # second time we've seen 'child', lift to a let
                        uid = self.uid()
                        scopes[seen_in_scope].lifted_lets[id(child)] = (uid, child)
                    else:
                        (uid, _) = scopes[seen_in_scope].lifted_lets[id(child)]
                    free_vars[uid] = seen_in_scope
            elif self.stop_at_jir and hasattr(child, '_jir'):
                self.memo[id(child)] = self.add_jir(child)
            else:
                def get_vars(bindings):
                    if isinstance(bindings, dict):
                        bindings = bindings.items()
                    return [var for (var, _) in bindings]
                eval_b = get_vars(x.bindings(i))
                agg_b = get_vars(x.agg_bindings(i))
                scan_b = get_vars(x.scan_bindings(i))
                new_scope = Scope(depth)
                scopes.append(new_scope)
                if x.new_block(i):
                    child_outermost_scope = depth
                else:
                    child_outermost_scope = outermost_scope
                child_context = x.child_context_without_bindings(i, context)
                if x.binds(i):
                    (eval_c, agg_c, scan_c) = child_context
                    eval_c = ir.base_ir._env_bind(eval_c, *[(var, depth) for var in eval_b])
                    agg_c = ir.base_ir._env_bind(agg_c, *[(var, depth) for var in agg_b])
                    scan_c = ir.base_ir._env_bind(scan_c, *[(var, depth) for var in scan_b])
                    child_context = (eval_c, agg_c, scan_c)
                if isinstance(child, ir.Ref):
                    if child.name not in child_context[0]:
                        print('...')
                    child_free_vars = {child.name: child_context[0][child.name]}
                else:
                    child_free_vars = self.recur(scopes, child_outermost_scope, child_context, child)
                for var in [*eval_b, *agg_b, *scan_b]:
                    var_depth = child_free_vars.pop(var, depth)
                    assert var_depth == depth
                for (var, _) in new_scope.lifted_lets.values():
                    var_depth = child_free_vars.pop(var, depth)
                    assert var_depth == depth
                free_vars.update(child_free_vars)
                scopes.pop()
                new_scope.visited.clear()
                if new_scope.lifted_lets:
                    self.scopes[id(child)] = new_scope
        if len(free_vars) > 0:
            bind_depth = max(free_vars.values())
            bind_depth = max(bind_depth, outermost_scope)
        else:
            bind_depth = outermost_scope
        scopes[bind_depth].visited[id(x)] = x
        return free_vars

    def print(self, builder: List[str], context: List[Scope], outermost_scope: int, depth: int, x: 'ir.BaseIR'):
        if id(x) in self.memo:
            builder.append(self.memo[id(x)])
            return
        insert_lets = id(x) in self.scopes and len(self.scopes[id(x)].lifted_lets) > 0
        if insert_lets:
            local_builder = []
            context.append(self.scopes[id(x)])
        else:
            local_builder = builder
        head = x.render_head(self)
        if head != '':
            local_builder.append(head)
        children = x.render_children(self)
        ir_child_num = 0
        for i in range(0, len(children)):
            local_builder.append(' ')
            child = children[i]
            lift_to = self.lifted_in_scope(child, context)
            child_outermost_scope = outermost_scope
            if x.new_block(ir_child_num):
                child_outermost_scope = depth
            if lift_to >= 0 and context[lift_to] and context[lift_to].depth >= outermost_scope:
                ir_child_num += 1
                (name, _) = context[lift_to].lifted_lets[id(child)]
                if id(child) not in context[lift_to].visited:
                    context[lift_to].visited[id(child)] = child
                    let_body = [f'(Let {name} ']
                    self.print(let_body, context, child_outermost_scope, depth + 1, child)
                    let_body.append(' ')
                    # let_bodies is built post-order, which guarantees earlier
                    # lets can't refer to later lets
                    context[lift_to].let_bodies.append(let_body)
                local_builder.append(f'(Ref {name})')
            else:
                if isinstance(child, ir.BaseIR):
                    self.print(local_builder, context, child_outermost_scope, depth + 1, child)
                else:
                    ir_child_num = self.print_renderable(local_builder, context, child_outermost_scope, depth + 1, ir_child_num, child)
        local_builder.append(x.render_tail(self))
        if insert_lets:
            context.pop()
            for let_body in self.scopes[id(x)].let_bodies:
                builder.extend(let_body)
            builder.extend(local_builder)
            num_lets = len(self.scopes[id(x)].lifted_lets)
            for i in range(num_lets):
                builder.append(')')

    def print_renderable(self, builder: List[str], context: List[Scope], outermost_scope: int, depth: int, ir_child_num: int, x: Renderable):
        assert not isinstance(x, ir.BaseIR)
        head = x.render_head(self)
        if head != '':
            builder.append(head)
        children = x.render_children(self)
        for i in range(0, len(children)):
            builder.append(' ')
            child = children[i]
            if isinstance(child, ir.BaseIR):
                self.print(builder, context, outermost_scope, depth, child)
                ir_child_num += 1
            else:
                ir_child_num = self.print_renderable(builder, context, outermost_scope, depth, ir_child_num, child)
        builder.append(x.render_tail(self))
        return ir_child_num


    def __call__(self, x: 'BaseIR') -> str:
        x.typ
        root_scope = Scope(0)
        free_vars = self.recur([root_scope], 0, ({}, {}, {}), x)
        root_scope.visited = {}
        if len(free_vars) != 0:
            print('...')
        assert(len(free_vars) == 0)
        self.scopes[id(x)] = root_scope
        builder = []
        self.print(builder, [], 0, 1, x)
        return ''.join(builder)
