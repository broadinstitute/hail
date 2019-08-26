from hail import ir
import abc
from typing import Sequence, List, Set, Dict, Callable, Tuple, Optional


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


Vars = Dict[str, int]
Context = (Vars, Vars, Vars)


class AnalysisStackFrame:
    def __init__(self, min_binding_depth: int, context: Context, x: 'ir.BaseIR'):
        # immutable
        self.min_binding_depth = min_binding_depth
        self.context = context
        self.node = x
        # mutable
        self._free_vars = {}
        self.visited: Dict[int, 'ir.BaseIR'] = {}
        self.lifted_lets: Dict[int, (str, 'ir.BaseIR')] = {}
        self.child_idx = 0
        self._child_bindings = None

    # compute depth at which we might bind this node
    def bind_depth(self) -> int:
        if len(self._free_vars) > 0:
            bind_depth = max(self._free_vars.values())
            bind_depth = max(bind_depth, self.min_binding_depth)
        else:
            bind_depth = self.min_binding_depth
        return bind_depth

    def make_child_frame(self, depth: int) -> 'AnalysisStackFrame':
        x = self.node
        i = self.child_idx - 1
        child = x.children[i]
        if x.new_block(i):
            child_outermost_scope = depth
        else:
            child_outermost_scope = self.min_binding_depth

        # compute vars bound in 'child' by 'node'
        eval_bindings = x.bindings(i, 0).keys()
        agg_bindings = x.agg_bindings(i, 0).keys()
        scan_bindings = x.scan_bindings(i, 0).keys()
        new_bindings = (eval_bindings, agg_bindings, scan_bindings)
        self._child_bindings = new_bindings
        child_context = x.child_context(i, self.context, depth)

        return AnalysisStackFrame(child_outermost_scope, child_context, child,)

    def free_vars(self):
        # subtract vars that will be bound by inserted lets
        for (var, _) in self.lifted_lets.values():
            self._free_vars.pop(var, 0)
        return self._free_vars

    def update_free_vars(self, child_frame: 'AnalysisStackFrame'):
        child_free_vars = child_frame.free_vars()
        # subtract vars bound by parent from free_vars
        (eval_bindings, agg_bindings, scan_bindings) = self._child_bindings
        for var in [*eval_bindings, *agg_bindings, *scan_bindings]:
            child_free_vars.pop(var, 0)
        # update parent's free variables
        self._free_vars.update(child_free_vars)

    def update_parent_free_vars(self, parent_frame: 'AnalysisStackFrame'):
        # subtract vars bound by parent from free_vars
        (eval_bindings, agg_bindings, scan_bindings) = parent_frame._child_bindings
        for var in [*eval_bindings, *agg_bindings, *scan_bindings]:
            self._free_vars.pop(var, 0)
        # subtract vars that will be bound by inserted lets
        for (var, _) in self.lifted_lets.values():
            self._free_vars.pop(var, 0)
        # update parent's free variables
        parent_frame._free_vars.update(self._free_vars)


class BindingSite:
    def __init__(self, lifted_lets: Dict[int, Tuple[str, 'ir.BaseIR']], depth: int, node: 'ir.BaseIR'):
        self.depth = depth
        self.lifted_lets = lifted_lets
        self.node = node


class BindingsStackFrame:
    def __init__(self, binding_site: BindingSite):
        self.depth = binding_site.depth
        self.lifted_lets = binding_site.lifted_lets
        self.visited = {}
        self.let_bodies = []


class PrintStackFrame:
    def __init__(self, node, children, local_builder, outermost_scope, depth, builder, insert_lets, lift_to_frame=None):
        # immutable
        self.node: Renderable = node
        self.children: Sequence[Renderable] = children
        self.min_binding_depth: int = outermost_scope
        self.depth: int = depth
        self.lift_to_frame: Optional[int] = lift_to_frame
        self.insert_lets: bool = insert_lets
        # mutable
        # FIXME: Builder is always parent's local_builder. Shouldn't need to
        # store builder. Maybe also make these plain strings.
        self.builder = builder
        self.local_builder = local_builder
        self.child_idx = 0
        self.ir_child_num = 0

    def add_lets(self, let_bodies):
        for let_body in let_bodies:
            self.builder.extend(let_body)
        self.builder.extend(self.local_builder)
        num_lets = len(let_bodies)
        for _ in range(num_lets):
            self.builder.append(')')

    def make_child_frame(self, renderer, builder, context, outermost_scope, depth, local_builder=None):
        child = self.children[self.i]
        if local_builder is None:
            local_builder = builder
        insert_lets = id(child) in renderer.binding_sites and len(renderer.binding_sites[id(child)].lifted_lets) > 0
        state = PrintStackFrame(child, child.render_children(renderer), local_builder, outermost_scope, depth, builder, insert_lets)
        if not isinstance(child, ir.BaseIR):
            state.ir_child_num = state.ir_child_num
        if insert_lets:
            state.local_builder = []
            context.append(BindingsStackFrame(renderer.binding_sites[id(child)]))
        head = child.render_head(renderer)
        if head != '':
            state.local_builder.append(head)
        return state


class CSERenderer(Renderer):
    def __init__(self, stop_at_jir=False):
        self.stop_at_jir = stop_at_jir
        self.jir_count = 0
        self.jirs = {}
        self.memo: Dict[int, Sequence[str]] = {}
        self.uid_count = 0
        self.binding_sites: Dict[int, BindingSite] = {}

    def uid(self) -> str:
        self.uid_count += 1
        return f'__cse_{self.uid_count}'

    def add_jir(self, jir):
        jir_id = f'm{self.jir_count}'
        self.jir_count += 1
        self.jirs[jir_id] = jir
        return jir_id

    # At top of main loop, we are considering the node 'node' and its
    # 'child_idx'th child, or if 'child_idx' = 'len(node.children)', we are
    # about to do post-processing on 'node' before moving back up to its parent.
    #
    # 'stack' is a stack of `AnalysisStackFrame`s, one for each node on the path
    # from 'root' to 'node. Each stack frame tracks the following immutable
    # information:
    # * 'node': The node corresponding to this stack frame.
    # * 'min_binding_depth': If 'node' is to be bound in a let, the let may not
    #   rise higher than this (its depth must be >= 'min_binding_depth')
    # * 'context': The binding context of 'node'. Maps variables bound above
    #   to the depth at which they were bound (more precisely, if
    #   'context[var] == depth', then 'stack[depth-1].node' binds 'var' in the
    #   subtree rooted at 'stack[depth].node').
    # * 'new_bindings': The variables which were bound by 'node's parent. These
    #   must be subtracted out of 'node's free variables when updating the
    #   parent's free variables.
    #
    # Each stack frame also holds the following mutable state:
    # * 'free_vars': The running union of free variables in the subtree rooted
    #   at 'node'.
    # * 'visited': A set of visited descendants. For each descendant 'x' of
    #   'node', 'id(x)' is added to 'visited'. This allows us to recognize when
    #   we see a node for a second time.
    # * 'lifted_lets': For each descendant 'x' of 'node', if 'x' is to be bound
    #   in a let immediately above 'node', then 'lifted_lets' contains 'id(x)',
    #   along with the unique id to bind 'x' to.
    # * 'child_idx': the child currently being visited (satisfies the invariant
    #   'stack[i].node.children[stack[i].child_idx] is stack[i+1].node`).
    def compute_new_bindings(self, root: 'ir.BaseIR'):
        root_frame = AnalysisStackFrame(0, ({}, {}, {}), root)
        stack = [root_frame]
        binding_sites = {}

        while True:
            frame = stack[-1]
            node = frame.node
            child_idx = frame.child_idx
            frame.child_idx += 1

            if child_idx >= len(node.children):
                if len(stack) <= 1:
                    break

                parent_frame = stack[-2]

                # mark node as visited at potential let insertion site
                if not node.is_effectful():
                    stack[frame.bind_depth()].visited[id(node)] = node

                # if any lets being inserted here, add node to registry of
                # binding sites
                if frame.lifted_lets:
                    binding_sites[id(node)] = \
                        BindingSite(frame.lifted_lets, len(stack), node)

                parent_frame.update_free_vars(frame)

                stack.pop()
                continue

            child = node.children[child_idx]

            if self.stop_at_jir and hasattr(child, '_jir'):
                jir_id = self.add_jir(child._jir)
                if isinstance(child, ir.MatrixIR):
                    jref = f'(JavaMatrix {jir_id})'
                elif isinstance(child, ir.TableIR):
                    jref = f'(JavaTable {jir_id})'
                elif isinstance(child, ir.BlockMatrixIR):
                    jref = f'(JavaBlockMatrix {jir_id})'
                else:
                    assert isinstance(child, ir.IR)
                    jref = f'(JavaIR {jir_id})'

                self.memo[id(child)] = jref
                continue

            seen_in_scope = next((i for i in reversed(range(len(stack))) if id(child) in stack[i].visited), None)

            if seen_in_scope is not None and isinstance(child, ir.IR):
                # we've seen 'child' before, should not traverse (or we will
                # find too many lifts)
                if id(child) in stack[seen_in_scope].lifted_lets:
                    (uid, _) = stack[seen_in_scope].lifted_lets[id(child)]
                else:
                    # second time we've seen 'child', lift to a let
                    uid = self.uid()
                    stack[seen_in_scope].lifted_lets[id(child)] = (uid, child)
                # Since we are not traversing 'child', we don't know its free
                # variables. To prevent a parent from being lifted too high,
                # we must register 'child' as having the free variable 'uid',
                # which will be true when 'child' is replaced by "Ref uid".
                frame._free_vars[uid] = seen_in_scope
                continue

            # first time visiting 'child'

            if isinstance(child, ir.Ref):
                if child.name not in node.bindings(child_idx, default_value=0).keys():
                    (eval_c, _, _) = node.child_context_without_bindings(
                        child_idx, frame.context)
                    frame._free_vars[child.name] = eval_c[child.name]
                continue

            stack.append(frame.make_child_frame(len(stack)))
            continue

        root_free_vars = root_frame.free_vars()
        assert(len(root_free_vars) == 0)

        binding_sites[id(root)] = BindingSite(frame.lifted_lets, 0, root)
        return binding_sites

    def make_post_children(self, node, builder, context, outermost_scope, depth, local_builder=None):
        if local_builder is None:
            local_builder = builder
        insert_lets = id(node) in self.binding_sites and len(self.binding_sites[id(node)].lifted_lets) > 0
        state = PrintStackFrame(node, node.render_children(self), local_builder, outermost_scope, depth, builder, insert_lets)
        if not isinstance(node, ir.BaseIR):
            state.ir_child_num = state.ir_child_num
        if insert_lets:
            state.local_builder = []
            context.append(BindingsStackFrame(self.binding_sites[id(node)]))
        head = node.render_head(self)
        if head != '':
            state.local_builder.append(head)
        return state

    # At top of main loop, we are considering the 'Renderable' 'node' and its
    # 'child_idx'th child, or if 'child_idx' = 'len(node.children)', we are
    # about to do post-processing on 'node' before moving back up to its parent.
    #
    # 'stack' is a stack of `PrintStackFrame`s, one for each node on the path
    # from 'root' to 'node. Each stack frame tracks the following immutable
    # information:
    # * 'node': The 'Renderable' node corresponding to this stack frame.
    # * 'children': The list of 'Renderable' children.
    # * 'min_binding_depth': If 'node' is to be bound in a let, the let may not
    #   rise higher than this (its depth must be >= 'min_binding_depth')
    # * 'depth': The depth of 'node' in the original tree, i.e. the number of
    #   BaseIR in 'stack', not counting other 'Renderable's.
    # * 'lift_to_frame': The outermost frame in which 'node' was marked to be
    #   lifted in the analysis pass, if any, otherwise None.
    # * 'insert_lets': True if any lets need to be inserted above 'node'. No
    #   node has both 'lift_to_frame' not None and 'insert_lets' True.
    #
    # Each stack frame also holds the following mutable state:
    # * 'builder' and 'local_builder': If 'insert_lets', then 'builder' is the
    #   buffer building the parent's rendered IR, and 'local_builder' is the
    #   buffer building 'node's rendered IR. After traversing the subtree rooted
    #   at 'node', all lets will be added to 'builder' before copying
    #   'local_builder' to 'builder'.
    #   If
    # * 'child_idx':
    # * 'ir_child_num':

    def build_string(self, root):
        root_builder = []
        context = []

        if id(root) in self.memo:
            return ''.join(self.memo[id(root)])

        stack = [self.make_post_children(root, root_builder, context, 0, 1)]

        while True:
            frame = stack[-1]
            node = frame.node
            # child_idx = frame.child_idx
            # frame.child_idx += 1

            if frame.child_idx >= len(frame.children):
                if frame.lift_to_frame is not None:
                    assert(not frame.insert_lets)
                    if id(node) in self.memo:
                        frame.local_builder.extend(self.memo[id(node)])
                    else:
                        frame.local_builder.append(node.render_tail(self))
                    frame.local_builder.append(' ')
                    # let_bodies is built post-order, which guarantees earlier
                    # lets can't refer to later lets
                    frame.lift_to_frame.let_bodies.append(frame.local_builder)
                    (name, _) = frame.lift_to_frame.lifted_lets[id(node)]
                    # FIXME: can this be added on first visit?
                    frame.builder.append(f'(Ref {name})')
                    stack.pop()
                    stack[-1].child_idx += 1
                    continue
                else:
                    frame.local_builder.append(node.render_tail(self))
                    if frame.insert_lets:
                        frame.add_lets(context[-1].let_bodies)
                        context.pop()
                    stack.pop()
                    if not stack:
                        return ''.join(frame.builder)
                    frame = stack[-1]
                    if not isinstance(node, ir.BaseIR):
                        frame.ir_child_num += 1
                    frame.child_idx += 1
                    continue

            frame.local_builder.append(' ')
            child = frame.children[frame.child_idx]

            child_outermost_scope = frame.min_binding_depth
            child_depth = frame.depth

            if isinstance(frame.node, ir.BaseIR) and frame.node.new_block(frame.ir_child_num):
                child_outermost_scope = frame.depth
            lift_to_frame = None
            if isinstance(child, ir.BaseIR):
                child_depth += 1
                lift_to_frame = next((frame for frame in context if id(child) in frame.lifted_lets), None)

            if lift_to_frame and lift_to_frame.depth >= frame.min_binding_depth:
                insert_lets = id(child) in self.binding_sites and len(self.binding_sites[id(child)].lifted_lets) > 0
                assert not insert_lets
                frame.ir_child_num += 1
                (name, _) = lift_to_frame.lifted_lets[id(child)]

                if id(child) in lift_to_frame.visited:
                    frame.local_builder.append(f'(Ref {name})')
                    frame.child_idx += 1
                    continue

                lift_to_frame.visited[id(child)] = child

                child_builder = [f'(Let {name} ']
                if id(child) in self.memo:
                    new_state = PrintStackFrame(child, [], child_builder, child_outermost_scope, child_depth, frame.local_builder, insert_lets, lift_to_frame)
                else:
                    new_state = self.make_post_children(child, frame.local_builder, context, child_outermost_scope, child_depth, child_builder)
                    new_state.lift_to_frame = lift_to_frame
                stack.append(new_state)
                continue

            if id(child) in self.memo:
                frame.local_builder.extend(self.memo[id(child)])
                frame.child_idx += 1
                continue

            new_state = self.make_post_children(child, frame.local_builder, context, child_outermost_scope, child_depth)
            stack.append(new_state)
            continue

    def __call__(self, root: 'ir.BaseIR') -> str:
        self.binding_sites = self.compute_new_bindings(root)

        return self.build_string(root)
