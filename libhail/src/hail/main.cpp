#include <cstdio>
#include <cstring>

#include <memory>

#include <hail/allocators.hpp>
#include <hail/format.hpp>
#include <hail/query/backend/jit.hpp>
#include <hail/query/ir.hpp>
#include <hail/tunion.hpp>
#include <hail/type.hpp>
#include <hail/value.hpp>

using namespace hail;

int
main() {
  HeapAllocator heap;
  ArenaAllocator arena(heap);
  TypeContext tc(heap);
  auto t = tc.ttuple({tc.tint32, tc.tstr});

  print("this: ", 5, " is a number and this is a type: ", t);

  auto region = std::make_shared<ArenaAllocator>(heap);

  auto p = cast<VTuple>(tc.get_vtype(t));
  auto pint32 = cast<VInt32>(p->element_vtypes[0]);
  auto pstr = cast<VStr>(p->element_vtypes[1]);

  Value i(pint32, 5);
  auto s = Value::make_str(pstr, region, 5);
  assert(s.get_size() == 5);
  memcpy(s.get_data(), "fooba", 5);
  auto v = Value::make_tuple(p, region);
  v.set_element_present(0, true);
  v.set_element(0, i);
  v.set_element_present(1, true);
  v.set_element(1, s);

  print("v = ", v);

  IRContext xc(heap);

  Module *m = xc.make_module();
  Function *f = xc.make_function(m, "main", {tc.tbool, tc.tint32}, tc.tint32);

  auto body = f->get_body();

  auto tb = body->make_block({body->inputs[1]});

  auto fb = body->make_block(1, 0);
  fb->set_child(0, fb->make_literal(i));

  body->set_child(0, body->make_mux(body->inputs[0], tb, fb));

  m->pretty_self(outs);

  JIT jit;
  auto fp  = reinterpret_cast<int (*)()>(jit.compile(m));
  print("result ", fp());

  return 0;
}
