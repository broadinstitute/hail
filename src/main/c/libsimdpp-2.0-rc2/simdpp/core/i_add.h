/*  Copyright (C) 2013-2014  Povilas Kanapickas <povilas@radix.lt>

    Distributed under the Boost Software License, Version 1.0.
        (See accompanying file LICENSE_1_0.txt or copy at
            http://www.boost.org/LICENSE_1_0.txt)
*/

#ifndef LIBSIMDPP_SIMDPP_CORE_I_ADD_H
#define LIBSIMDPP_SIMDPP_CORE_I_ADD_H

#ifndef LIBSIMDPP_SIMD_H
    #error "This file must be included through simd.h"
#endif

#include <simdpp/types.h>
#include <simdpp/detail/expr/i_add.h>
#include <simdpp/detail/cast_expr.h>
#include <simdpp/core/detail/get_expr_uint.h>
#include <simdpp/core/detail/scalar_arg_impl.h>
#include <simdpp/core/detail/get_expr_uint.h>

namespace simdpp {
namespace SIMDPP_ARCH_NAMESPACE {


/** Adds 8-bit integer values.

    @code
    r0 = a0 + b0
    ...
    rN = aN + bN
    @endcode

    @par 256-bit version:
    @icost{SSE2-AVX, NEON, ALTIVEC, 2}
*/
template<unsigned N, class V1, class V2> SIMDPP_INL
typename detail::get_expr_uint<expr_add, V1, V2>::type
        add(const any_int8<N,V1>& a,
            const any_int8<N,V2>& b)
{
    using expr = typename detail::get_expr_uint<expr_add, V1, V2>;
    return { { detail::cast_expr<typename expr::v1_type>(a.wrapped()),
               detail::cast_expr<typename expr::v2_type>(b.wrapped()) } };
}

SIMDPP_SCALAR_ARG_IMPL_INT_UNSIGNED(add, expr_add, any_int8, uint8)

/** Adds 16-bit integer values.

    @code
    r0 = a0 + b0
    ...
    rN = aN + bN
    @endcode

    @par 256-bit version:
    @icost{SSE2-AVX, NEON, ALTIVEC, 2}
*/
template<unsigned N, class V1, class V2> SIMDPP_INL
typename detail::get_expr_uint<expr_add, V1, V2>::type
        add(const any_int16<N,V1>& a,
            const any_int16<N,V2>& b)
{
    using expr = typename detail::get_expr_uint<expr_add, V1, V2>;
    return { { detail::cast_expr<typename expr::v1_type>(a.wrapped()),
               detail::cast_expr<typename expr::v2_type>(b.wrapped()) } };
}

SIMDPP_SCALAR_ARG_IMPL_INT_UNSIGNED(add, expr_add, any_int16, uint16)

/** Adds 32-bit integer values.

    @code
    r0 = a0 + b0
    ...
    rN = aN + bN
    @endcode

    @par 256-bit version:
    @icost{SSE2-AVX, NEON, ALTIVEC, 2}
*/
template<unsigned N, class V1, class V2> SIMDPP_INL
typename detail::get_expr_uint<expr_add, V1, V2>::type
        add(const any_int32<N,V1>& a,
            const any_int32<N,V2>& b)
{
    using expr = typename detail::get_expr_uint<expr_add, V1, V2>;
    return { { detail::cast_expr<typename expr::v1_type>(a.wrapped()),
               detail::cast_expr<typename expr::v2_type>(b.wrapped()) } };
}

SIMDPP_SCALAR_ARG_IMPL_INT_UNSIGNED(add, expr_add, any_int32, uint32)

/** Adds 64-bit integer values.

    @code
    r0 = a0 + b0
    ...
    rN = aN + bN
    @endcode

    @par 128-bit version:
    @icost{ALTIVEC, 5-6}

    @par 256-bit version:
    @icost{SSE2-AVX, NEON, 2}
    @icost{ALTIVEC, 10-11}
*/
template<unsigned N, class V1, class V2> SIMDPP_INL
typename detail::get_expr_uint<expr_add, V1, V2>::type
        add(const any_int64<N,V1>& a,
            const any_int64<N,V2>& b)
{
    using expr = typename detail::get_expr_uint<expr_add, V1, V2>;
    return { { detail::cast_expr<typename expr::v1_type>(a.wrapped()),
               detail::cast_expr<typename expr::v2_type>(b.wrapped()) } };
}

SIMDPP_SCALAR_ARG_IMPL_INT_UNSIGNED(add, expr_add, any_int64, uint64)

} // namespace SIMDPP_ARCH_NAMESPACE
} // namespace simdpp

#endif

