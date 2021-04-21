#include "hail/tunion.hpp"
#include "hail/query/ir.hpp"
#include "hail/query/ir_type.hpp"

namespace hail {

const Type *
IRType::infer(Block *x) {
  if (x->get_function_parent()) {
    std::vector<const Type *> input_types = x->get_function_parent()->parameter_types;
    std::vector<const Type *> output_types;
    for (auto c : x->get_children())
      output_types.push_back(infer(c));
    return tc.tblock(std::move(input_types), std::move(output_types));
  }

  IR *parent = x->get_parent();
  switch (parent->tag) {
  default:
    abort();
  }
}

const Type *
IRType::infer(Input *x) {
  auto parent_input_types = cast<TBlock>(infer(x->get_parent()))->input_types;
  assert(x->index < parent_input_types.size());
  return parent_input_types[x->index];
}

const Type *
IRType::infer(Literal *x) {
  return x->value.vtype->type;
}

const Type *
IRType::infer(NA *x) {
  return x->type;
}

const Type *
IRType::infer(IsNA *x) {
  infer(x->get_child(0));
  return tc.tbool;
}

const Type *
IRType::infer(MakeTuple *x) {
  std::vector<const Type *> element_types;
  for (auto c : x->get_children())
    element_types.push_back(infer(c));
  return tc.ttuple(element_types);
}

const Type *
IRType::infer(GetTupleElement *x) {
  return cast<TTuple>(infer(x->get_child(0)))->element_types[x->index];
}

const Type *
IRType::infer(MakeArray *x) {
  auto children = x->get_children();
  assert(children.size() > 0);
  auto element_type = infer(children[0]);
  for (auto c : children)
    assert(infer(c) == element_type);
  return tc.tarray(element_type);
}

const Type *
IRType::infer(ArrayLen *x) {
  infer(x->get_child(0));
  return tc.tint64;
}

const Type *
IRType::infer(ArrayRef *x) {
  auto child_type = infer(x->get_child(0));
  auto index_type = infer(x->get_child(1));
  return cast<TArray>(child_type)->element_type;
}

const Type *
IRType::infer(IR *x) {
  auto p = ir_type.insert({x, nullptr});
  if (!p.second)
    return p.first->second;

  const Type *t = x->dispatch([this](auto x) {
				return infer(x);
			      });
  p.first->second = t;
  return t;
}

IRType::IRType(TypeContext &tc, Function *f)
  : tc(tc) {
  infer(f->get_body());
}

}
