/*  Copyright (C) 2013-2014  Povilas Kanapickas <povilas@radix.lt>

    Distributed under the Boost Software License, Version 1.0.
        (See accompanying file LICENSE_1_0.txt or copy at
            http://www.boost.org/LICENSE_1_0.txt)
*/

#ifndef LIBSIMDPP_SIMDPP_CORE_CMP_GE_H
#define LIBSIMDPP_SIMDPP_CORE_CMP_GE_H

#ifndef LIBSIMDPP_SIMD_H
    #error "This file must be included through simd.h"
#endif

#include <simdpp/types.h>
#include <simdpp/detail/insn/cmp_ge.h>
#include <simdpp/core/detail/scalar_arg_impl.h>

namespace simdpp {
namespace SIMDPP_ARCH_NAMESPACE {


/** Compares the values of two float32x4 vectors for greater-than or equal

    @code
    r0 = (a0 >= b0) ? 0xffffffff : 0x0
    ...
    rN = (aN >= bN) ? 0xffffffff : 0x0
    @endcode

    @par 256-bit version:
    @icost{SSE2-SSE4.1, NEON, ALTIVEC, 2}
*/
template<unsigned N, class E1, class E2> SIMDPP_INL
mask_float32<N,expr_empty> cmp_ge(const float32<N,E1>& a,
                                  const float32<N,E2>& b)
{
    return detail::insn::i_cmp_ge(a.eval(), b.eval());
}

SIMDPP_SCALAR_ARG_IMPL_VEC(cmp_ge, mask_float32, float32)

/** Compares the values of two float64x2 vectors for greater-than

    @code
    r0 = (a0 >= b0) ? 0xffffffffffffffff : 0x0
    ...
    rN = (aN >= bN) ? 0xffffffffffffffff : 0x0
    @endcode

    @par 128-bit version:
    @novec{NEON, ALTIVEC}

    @par 256-bit version:
    @novec{NEON, ALTIVEC}
    @icost{SSE2-SSE4.1, 2}
*/
template<unsigned N, class E1, class E2> SIMDPP_INL
mask_float64<N,expr_empty> cmp_ge(const float64<N,E1>& a,
                                  const float64<N,E2>& b)
{
    return detail::insn::i_cmp_ge(a.eval(), b.eval());
}

SIMDPP_SCALAR_ARG_IMPL_VEC(cmp_ge, mask_float64, float64)

} // namespace SIMDPP_ARCH_NAMESPACE
} // namespace simdpp

#endif

