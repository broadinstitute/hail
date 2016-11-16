/*  Copyright (C) 2013-2014  Povilas Kanapickas <povilas@radix.lt>

    Distributed under the Boost Software License, Version 1.0.
        (See accompanying file LICENSE_1_0.txt or copy at
            http://www.boost.org/LICENSE_1_0.txt)
*/

#ifndef LIBSIMDPP_SIMDPP_CORE_BIT_NOT_H
#define LIBSIMDPP_SIMDPP_CORE_BIT_NOT_H

#ifndef LIBSIMDPP_SIMD_H
    #error "This file must be included through simd.h"
#endif

#include <simdpp/types.h>
#include <simdpp/detail/insn/bit_not.h>
#include <simdpp/detail/expr/bit_not.h>
#include <simdpp/detail/get_expr.h>

namespace simdpp {
namespace SIMDPP_ARCH_NAMESPACE {

/// @{
/** Computes bitwise NOT of an integer or floating-point vector

    @code
    r = ~a
    @endcode

    @todo icost
*/
template<unsigned N, class V> SIMDPP_INL
typename detail::get_expr<V, expr_bit_not<V>>::empty
    bit_not(const any_vec<N,V>& a)
{
    typename detail::get_expr_nosign<V>::type ra;
    ra = a.wrapped().eval();
    return detail::insn::i_bit_not(ra);
}

/* FIXME
template<unsigned N, class E> SIMDPP_INL
mask_int32<N, expr_bit_not<mask_int32<N,E>>> bit_not(mask_int32<N,E> a)
{
    return { { a } };
}
template<unsigned N, class E> SIMDPP_INL
mask_int64<N, expr_bit_not<mask_int64<N,E>>> bit_not(mask_int64<N,E> a)
{
    return { { a } };
}
/// @}


template<unsigned N, class E> SIMDPP_INL
mask_float32<N, expr_bit_not<mask_float32<N,E>>> bit_not(mask_float32<N,E> a)
{
    return { { a } };
}
template<unsigned N, class E> SIMDPP_INL
mask_float64<N, expr_bit_not<mask_float64<N,E>>> bit_not(mask_float64<N,E> a)
{
    return { { a } };
}
/// @}*/

} // namespace SIMDPP_ARCH_NAMESPACE
} // namespace simdpp

#endif

