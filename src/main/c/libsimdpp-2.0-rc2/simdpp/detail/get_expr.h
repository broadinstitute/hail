/*  Copyright (C) 2014  Povilas Kanapickas <povilas@radix.lt>

    Distributed under the Boost Software License, Version 1.0.
        (See accompanying file LICENSE_1_0.txt or copy at
            http://www.boost.org/LICENSE_1_0.txt)
*/

#ifndef LIBSIMDPP_SIMDPP_DETAIL_GET_EXPR_H
#define LIBSIMDPP_SIMDPP_DETAIL_GET_EXPR_H

#ifndef LIBSIMDPP_SIMD_H
    #error "This file must be included through simd.h"
#endif

#include <simdpp/setup_arch.h>
#include <simdpp/expr.h>
#include <simdpp/types/fwd.h>
#include <simdpp/types/any.h>
#include <simdpp/types/tag.h>
#include <simdpp/expr.h>

namespace simdpp {
namespace SIMDPP_ARCH_NAMESPACE {
namespace detail {

// Same as V::expr_type, except that if that type is void, returns V as type.
// In other words, if V is a vector (V<N,void>), then type is V<N,void>.
// otherwise type is V.
template<class V>
struct wrap_vector_expr { using type = typename V::expr_type; };

template<template<unsigned, class> class V, unsigned N>
struct wrap_vector_expr< V<N,void> > { using type = V<N,void>; };


/*  The tag values are selected specifically to make this class template
    simpler.

    See simdpp/types/tag.h

    Note that B refers to the number of elements in the resulting vector. Since
    get_expr2 always selects elements with smaller number of (wider) elements,
    one only needs to ensure that B refers to the function argument with widest
    elements among the function arguments.
*/

template<unsigned Tag, unsigned B, class E> struct type_of_tag;
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_INT + SIMDPP_TAG_SIZE8, B, E>     { using type = int8<B,E>;     using empty = int8<B,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_INT + SIMDPP_TAG_SIZE16, B, E>    { using type = int16<B/2,E>;    using empty = int16<B/2,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_INT + SIMDPP_TAG_SIZE32, B, E>    { using type = int32<B/4,E>;    using empty = int32<B/4,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_INT + SIMDPP_TAG_SIZE64, B, E>    { using type = int64<B/8,E>;    using empty = int64<B/8,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_UINT + SIMDPP_TAG_SIZE8, B, E>    { using type = uint8<B,E>;    using empty = uint8<B,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_UINT + SIMDPP_TAG_SIZE16, B, E>   { using type = uint16<B/2,E>;   using empty = uint16<B/2,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_UINT + SIMDPP_TAG_SIZE32, B, E>   { using type = uint32<B/4,E>;   using empty = uint32<B/4,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_UINT + SIMDPP_TAG_SIZE64, B, E>   { using type = uint64<B/8,E>;   using empty = uint64<B/8,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_MASK_INT + SIMDPP_TAG_SIZE8, B, E>    { using type = mask_int8<B,E>;    using empty = mask_int8<B,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_MASK_INT + SIMDPP_TAG_SIZE16, B, E>   { using type = mask_int16<B/2,E>; using empty = mask_int16<B/2,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_MASK_INT + SIMDPP_TAG_SIZE32, B, E>   { using type = mask_int32<B/4,E>; using empty = mask_int32<B/4,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_MASK_INT + SIMDPP_TAG_SIZE64, B, E>   { using type = mask_int64<B/8,E>; using empty = mask_int64<B/8,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_FLOAT + SIMDPP_TAG_SIZE32, B, E>      { using type = float32<B/4,E>;    using empty = float32<B/4,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_FLOAT + SIMDPP_TAG_SIZE64, B, E>      { using type = float64<B/8,E>;    using empty = float64<B/8,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_MASK_FLOAT + SIMDPP_TAG_SIZE32, B, E> { using type = mask_float32<B/4,E>; using empty = mask_float32<B/4,expr_empty>; };
template<unsigned B, class E> struct type_of_tag<SIMDPP_TAG_MASK_FLOAT + SIMDPP_TAG_SIZE64, B, E> { using type = mask_float64<B/8,E>; using empty = mask_float64<B/8,expr_empty>; };

template<class V, class E = void>
class get_expr {
public:
    using type = typename type_of_tag<V::type_tag + V::size_tag, V::length_bytes, E>::type;
    using empty = typename type_of_tag<V::type_tag + V::size_tag, V::length_bytes, E>::empty;
};

template<class V, class E = void>
class get_expr_nomask {
    static const unsigned type_tag = (V::type_tag == SIMDPP_TAG_MASK_FLOAT) ? SIMDPP_TAG_FLOAT :
                                     (V::type_tag == SIMDPP_TAG_MASK_INT) ? SIMDPP_TAG_UINT : V::type_tag;
public:
    using type = typename type_of_tag<type_tag + V::size_tag, V::length_bytes, E>::type;
    using empty = typename type_of_tag<type_tag + V::size_tag, V::length_bytes, E>::empty;
};

template<class V, class E = void>
class get_expr_nosign {
    static const unsigned type_tag = (V::type_tag == SIMDPP_TAG_INT) ? SIMDPP_TAG_UINT : V::type_tag;
public:
    using type = typename type_of_tag<type_tag + V::size_tag, V::length_bytes, E>::type;
    using empty = typename type_of_tag<type_tag + V::size_tag, V::length_bytes, E>::empty;
};

template<class V, class E = void>
class get_expr_nomask_nosign {
    static const unsigned type_tag = (V::type_tag == SIMDPP_TAG_MASK_FLOAT) ? SIMDPP_TAG_FLOAT :
                                     (V::type_tag == SIMDPP_TAG_INT) ? SIMDPP_TAG_UINT :
                                     (V::type_tag == SIMDPP_TAG_MASK_INT) ? SIMDPP_TAG_UINT : V::type_tag;
public:
    using type = typename type_of_tag<type_tag + V::size_tag, V::length_bytes, E>::type;
    using empty = typename type_of_tag<type_tag + V::size_tag, V::length_bytes, E>::empty;
};

template<class V1, class V2, class E = void>
class get_expr2 {
    static const bool is_mask_op = (V1::type_tag == SIMDPP_TAG_MASK_FLOAT || V1::type_tag == SIMDPP_TAG_MASK_INT) &&
                                   (V2::type_tag == SIMDPP_TAG_MASK_FLOAT || V2::type_tag == SIMDPP_TAG_MASK_INT);
    static const unsigned size_tag = V1::size_tag > V2::size_tag ? V1::size_tag : V2::size_tag;
    static const unsigned type_tag = (is_mask_op && V1::size_tag != V2::size_tag) ? SIMDPP_TAG_UINT :
                                                  (V1::type_tag > V2::type_tag ? V1::type_tag : V2::type_tag);

public:
    using type = typename type_of_tag<type_tag + size_tag, V1::length_bytes, E>::type;
    using empty = typename type_of_tag<type_tag + size_tag, V1::length_bytes, E>::empty;
};

template<class V1, class V2, class E = void>
class get_expr2_nomask {
    static const unsigned type_tag_t1 = V1::type_tag > V2::type_tag ? V1::type_tag : V2::type_tag;

    static const unsigned size_tag = V1::size_tag > V2::size_tag ? V1::size_tag : V2::size_tag;
    static const unsigned type_tag = (type_tag_t1 == SIMDPP_TAG_MASK_FLOAT) ? SIMDPP_TAG_FLOAT :
                                        (type_tag_t1 == SIMDPP_TAG_MASK_INT) ? SIMDPP_TAG_UINT : type_tag_t1;

public:
    using type = typename type_of_tag<type_tag + size_tag, V1::length_bytes, E>::type;
    using empty = typename type_of_tag<type_tag + size_tag, V1::length_bytes, E>::empty;
};

template<class V1, class V2, class E = void>
class get_expr2_nosign {
    static const unsigned type_tag_t1 = V1::type_tag > V2::type_tag ? V1::type_tag : V2::type_tag;

public:
    static const unsigned size_tag = V1::size_tag > V2::size_tag ? V1::size_tag : V2::size_tag;
    static const unsigned type_tag = (type_tag_t1 == SIMDPP_TAG_INT) ? SIMDPP_TAG_UINT : type_tag_t1;

    using type = typename type_of_tag<type_tag + size_tag, V1::length_bytes, E>::type;
    using empty = typename type_of_tag<type_tag + size_tag, V1::length_bytes, E>::empty;
};


template<class V1, class V2, class E = void>
class get_expr2_nomask_nosign {
    static const unsigned type_tag_t1 = V1::type_tag > V2::type_tag ? V1::type_tag : V2::type_tag;
    static const bool is_mask_op = type_tag_t1 == SIMDPP_TAG_MASK_FLOAT || type_tag_t1 == SIMDPP_TAG_MASK_INT;

    static const unsigned size_tag = V1::size_tag > V2::size_tag ? V1::size_tag : V2::size_tag;
    static const unsigned type_tag = (is_mask_op && V1::size_tag != V2::size_tag) ? SIMDPP_TAG_UINT :
                                     (type_tag_t1 == SIMDPP_TAG_INT) ? SIMDPP_TAG_UINT : type_tag_t1;

public:
    using type = typename type_of_tag<type_tag + size_tag, V1::length_bytes, E>::type;
    using empty = typename type_of_tag<type_tag + size_tag, V1::length_bytes, E>::empty;
};


} // namespace detail
} // namespace SIMDPP_ARCH_NAMESPACE
} // namespace simdpp

#endif
