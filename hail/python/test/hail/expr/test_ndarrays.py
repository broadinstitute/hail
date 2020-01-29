import hail as hl
from hail.utils.java import FatalError
import numpy as np
from ..helpers import *
import tempfile
import pytest

from hail.utils.java import FatalError


def assert_ndarrays(asserter, exprs_and_expecteds):
    exprs, expecteds = zip(*exprs_and_expecteds)

    expr_tuple = hl.tuple(exprs)
    evaled_exprs = hl.eval(expr_tuple)

    for (evaled, expected) in zip(evaled_exprs, expecteds):
        assert asserter(evaled, expected)


def assert_ndarrays_eq(*expr_and_expected):
    assert_ndarrays(np.array_equal, expr_and_expected)


def assert_ndarrays_almost_eq(*expr_and_expected):
    assert_ndarrays(np.allclose, expr_and_expected)

@skip_unless_spark_backend()
def test_ndarray_ref():

    scalar = 5.0
    np_scalar = np.array(scalar)
    h_scalar = hl.nd.array(scalar)
    h_np_scalar = hl.nd.array(np_scalar)

    assert_evals_to(h_scalar[()], 5.0)
    assert_evals_to(h_np_scalar[()], 5.0)

    cube = [[[0, 1],
             [2, 3]],
            [[4, 5],
             [6, 7]]]
    h_cube = hl.nd.array(cube)
    h_np_cube = hl.nd.array(np.array(cube))
    missing = hl.nd.array(hl.null(hl.tarray(hl.tint32)))

    assert_all_eval_to(
        (h_cube[0, 0, 1], 1),
        (h_cube[1, 1, 0], 6),
        (h_np_cube[0, 0, 1], 1),
        (h_np_cube[1, 1, 0], 6),
        (hl.nd.array([[[[1]]]])[0, 0, 0, 0], 1),
        (hl.nd.array([[[1, 2]], [[3, 4]]])[1, 0, 0], 3),
        (missing[1], None),
        (hl.nd.array([1, 2, 3])[hl.null(hl.tint32)], None),
        (h_cube[0, 0, hl.null(hl.tint32)], None)
    )

    with pytest.raises(FatalError) as exc:
        hl.eval(hl.nd.array([1, 2, 3])[4])
    assert "Index out of bounds" in str(exc)

@skip_unless_spark_backend()
def test_ndarray_slice():
    np_rect_prism = np.arange(24).reshape((2, 3, 4))
    rect_prism = hl.nd.array(np_rect_prism)
    np_mat = np.arange(8).reshape((2, 4))
    mat = hl.nd.array(np_mat)
    np_flat = np.arange(20)
    flat = hl.nd.array(np_flat)

    assert_ndarrays_eq(
        (rect_prism[:, :, :], np_rect_prism[:, :, :]),
        (rect_prism[:, :, 1], np_rect_prism[:, :, 1]),
        (rect_prism[0:1, 1:3, 0:2], np_rect_prism[0:1, 1:3, 0:2]),
        (rect_prism[:, :, 1:4:2], np_rect_prism[:, :, 1:4:2]),
        (rect_prism[:, 2, 1:4:2], np_rect_prism[:, 2, 1:4:2]),
        (rect_prism[0, 2, 1:4:2], np_rect_prism[0, 2, 1:4:2]),
        (rect_prism[0, :, 1:4:2] + rect_prism[:, :1, 1:4:2], np_rect_prism[0, :, 1:4:2] + np_rect_prism[:, :1, 1:4:2]),
        (rect_prism[0:, :, 1:4:2] + rect_prism[:, :1, 1:4:2], np_rect_prism[0:, :, 1:4:2] + np_rect_prism[:, :1, 1:4:2]),
        (mat[0, 1:4:2] + mat[:, 1:4:2], np_mat[0, 1:4:2] + np_mat[:, 1:4:2]),
        (rect_prism[0, 0, -3:-1], np_rect_prism[0, 0, -3:-1]),
        (flat[15:5:-1], np_flat[15:5:-1]),
        (flat[::-1], np_flat[::-1]),
        (flat[::22], np_flat[::22]),
        (flat[::-22], np_flat[::-22]),
        (flat[15:5], np_flat[15:5]),
        (flat[3:12:-1], np_flat[3:12:-1]),
        (flat[12:3:1], np_flat[12:3:1]),
        (mat[::-1, :], np_mat[::-1, :]),
        (flat[4:1:-2], np_flat[4:1:-2]),
        (flat[0:0:1], np_flat[0:0:1]),
        (flat[-4:-1:2], np_flat[-4:-1:2])
    )

    assert hl.eval(flat[hl.null(hl.tint32):4:1]) is None
    assert hl.eval(flat[4:hl.null(hl.tint32)]) is None
    assert hl.eval(flat[4:10:hl.null(hl.tint32)]) is None
    assert hl.eval(rect_prism[:, :, 0:hl.null(hl.tint32):1]) is None
    assert hl.eval(rect_prism[hl.null(hl.tint32), :, :]) is None

    with pytest.raises(FatalError) as exc:
        hl.eval(flat[::0])
    assert "Slice step cannot be zero" in str(exc)


@skip_unless_spark_backend()
def test_ndarray_eval():
    data_list = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    mishapen_data_list1 = [[4], [1, 2, 3]]
    mishapen_data_list2 = [[[1], [2, 3]]]
    mishapen_data_list3 = [[4], [1, 2, 3], 5]

    nd_expr = hl.nd.array(data_list)
    evaled = hl.eval(nd_expr)
    np_equiv = np.array(data_list, dtype=np.int32)
    np_equiv_fortran_style = np.asfortranarray(np_equiv)
    np_equiv_extra_dimension = np_equiv.reshape((3, 1, 3))
    assert(np.array_equal(evaled, np_equiv))
    assert(evaled.strides == np_equiv.strides)

    assert hl.eval(hl.nd.array([[], []])).strides == (8, 8)
    assert np.array_equal(hl.eval(hl.nd.array([])), np.array([]))

    zero_array = np.zeros((10, 10), dtype=np.int64)
    evaled_zero_array = hl.eval(hl.literal(zero_array))

    assert np.array_equal(evaled_zero_array, zero_array)
    assert zero_array.dtype == evaled_zero_array.dtype

    # Testing correct interpretation of numpy strides
    assert np.array_equal(hl.eval(hl.literal(np_equiv_fortran_style)), np_equiv_fortran_style)
    assert np.array_equal(hl.eval(hl.literal(np_equiv_extra_dimension)), np_equiv_extra_dimension)

    # Testing from hail arrays
    assert np.array_equal(hl.eval(hl.nd.array(hl.range(6))), np.arange(6))
    assert np.array_equal(hl.eval(hl.nd.array(hl.int64(4))), np.array(4))

    # Testing from nested hail arrays
    assert np.array_equal(hl.eval(hl.nd.array(hl.array([hl.array(x) for x in data_list]))), np.arange(9).reshape((3, 3)) + 1)

    # Testing missing data
    assert hl.eval(hl.nd.array(hl.null(hl.tarray(hl.tint32)))) is None

    with pytest.raises(ValueError) as exc:
        hl.nd.array(mishapen_data_list1)
    assert "inner dimensions do not match" in str(exc.value)

    with pytest.raises(FatalError) as exc:
        hl.eval(hl.nd.array(hl.array(mishapen_data_list1)))
    assert "inner dimensions do not match" in str(exc.value)

    with pytest.raises(FatalError) as exc:
        hl.eval(hl.nd.array(hl.array(mishapen_data_list2)))
    assert "inner dimensions do not match" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        hl.nd.array(mishapen_data_list3)
    assert "inner dimensions do not match" in str(exc.value)


@skip_unless_spark_backend()
def test_ndarray_shape():
    np_e = np.array(3)
    np_row = np.array([1, 2, 3])
    np_col = np.array([[1], [2], [3]])
    np_m = np.array([[1, 2], [3, 4]])
    np_nd = np.arange(30).reshape((2, 5, 3))

    e = hl.nd.array(np_e)
    row = hl.nd.array(np_row)
    col = hl.nd.array(np_col)
    m = hl.nd.array(np_m)
    nd = hl.nd.array(np_nd)
    missing = hl.nd.array(hl.null(hl.tarray(hl.tint32)))

    assert_all_eval_to(
        (e.shape, np_e.shape),
        (row.shape, np_row.shape),
        (col.shape, np_col.shape),
        (m.shape, np_m.shape),
        (nd.shape, np_nd.shape),
        ((row + nd).shape, (np_row + np_nd).shape),
        ((row + col).shape, (np_row + np_col).shape),
        (m.transpose().shape, np_m.transpose().shape),
        (missing.shape, None)
    )

@skip_unless_spark_backend()
def test_ndarray_reshape():
    np_single = np.array([8])
    single = hl.nd.array([8])

    np_zero_dim = np.array(4)
    zero_dim = hl.nd.array(4)

    np_a = np.array([1, 2, 3, 4, 5, 6])
    a = hl.nd.array(np_a)

    np_cube = np.array([0, 1, 2, 3, 4, 5, 6, 7]).reshape((2, 2, 2))
    cube = hl.nd.array([0, 1, 2, 3, 4, 5, 6, 7]).reshape((2, 2, 2))
    cube_to_rect = cube.reshape((2, 4))
    np_cube_to_rect = np_cube.reshape((2, 4))
    cube_t_to_rect = cube.transpose((1, 0, 2)).reshape((2, 4))
    np_cube_t_to_rect = np_cube.transpose((1, 0, 2)).reshape((2, 4))

    np_hypercube = np.arange(3 * 5 * 7 * 9).reshape((3, 5, 7, 9))
    hypercube = hl.nd.array(np_hypercube)

    assert_ndarrays_eq(
        (single.reshape(()), np_single.reshape(())),
        (zero_dim.reshape(()), np_zero_dim.reshape(())),
        (zero_dim.reshape((1,)), np_zero_dim.reshape((1,))),
        (a.reshape((6,)), np_a.reshape((6,))),
        (a.reshape((2, 3)), np_a.reshape((2, 3))),
        (a.reshape((3, 2)), np_a.reshape((3, 2))),
        (a.reshape((3, -1)), np_a.reshape((3, -1))),
        (a.reshape((-1, 2)), np_a.reshape((-1, 2))),
        (cube_to_rect, np_cube_to_rect),
        (cube_t_to_rect, np_cube_t_to_rect),
        (hypercube.reshape((5, 7, 9, 3)).reshape((7, 9, 3, 5)), np_hypercube.reshape((7, 9, 3, 5))),
        (hypercube.reshape(hl.tuple([5, 7, 9, 3])), np_hypercube.reshape((5, 7, 9, 3)))
    )

    assert hl.eval(hl.null(hl.tndarray(hl.tfloat, 2)).reshape((4, 5))) is None
    assert hl.eval(hl.nd.array(hl.range(20)).reshape(hl.null(hl.ttuple(hl.tint64, hl.tint64)))) is None

    with pytest.raises(FatalError) as exc:
        hl.eval(hl.literal(np_cube).reshape((-1, -1)))
    assert "more than one -1" in str(exc)

    with pytest.raises(FatalError) as exc:
        hl.eval(hl.literal(np_cube).reshape((20,)))
    assert "requested shape is incompatible with number of elements" in str(exc)

    with pytest.raises(FatalError) as exc:
        hl.eval(a.reshape((3,)))
    assert "requested shape is incompatible with number of elements" in str(exc)

    with pytest.raises(FatalError) as exc:
        hl.eval(a.reshape(()))
    assert "requested shape is incompatible with number of elements" in str(exc)

    with pytest.raises(FatalError) as exc:
        hl.eval(hl.literal(np_cube).reshape((0, 2, 2)))
    assert "must contain only positive numbers or -1" in str(exc)

    with pytest.raises(FatalError) as exc:
        hl.eval(hl.literal(np_cube).reshape((2, 2, -2)))
    assert "must contain only positive numbers or -1" in str(exc)


@skip_unless_spark_backend()
def test_ndarray_map():
    a = hl.nd.array([[2, 3, 4], [5, 6, 7]])
    b = hl.map(lambda x: -x, a)
    c = hl.map(lambda x: True, a)

    assert_ndarrays_eq(
        (b, [[-2, -3, -4], [-5, -6, -7]]),
        (c, [[True, True, True],
             [True, True, True]]))

    assert hl.eval(hl.null(hl.tndarray(hl.tfloat, 1)).map(lambda x: x * 2)) is None

@skip_unless_spark_backend()
def test_ndarray_map2():

    a = 2.0
    b = 3.0
    x = np.array([a, b])
    y = np.array([b, a])
    row_vec = np.array([[1, 2]])
    cube1 = np.array([[[1, 2],
                       [3, 4]],
                      [[5, 6],
                       [7, 8]]])
    cube2 = np.array([[[9, 10],
                       [11, 12]],
                      [[13, 14],
                       [15, 16]]])

    na = hl.nd.array(a)
    nx = hl.nd.array(x)
    ny = hl.nd.array(y)
    nrow_vec = hl.nd.array(row_vec)
    ncube1 = hl.nd.array(cube1)
    ncube2 = hl.nd.array(cube2)

    assert_ndarrays_eq(
        # with lists/numerics
        (na + b, np.array(a + b)),
        (b + na, np.array(a + b)),
        (nx + y, x + y),
        (ncube1 + cube2, cube1 + cube2),

        # Addition
        (na + na, np.array(a + a)),
        (nx + ny, x + y),
        (ncube1 + ncube2, cube1 + cube2),
        # Broadcasting
        (ncube1 + na, cube1 + a),
        (na + ncube1, a + cube1),
        (ncube1 + ny, cube1 + y),
        (ny + ncube1, y + cube1),
        (nrow_vec + ncube1, row_vec + cube1),
        (ncube1 + nrow_vec, cube1 + row_vec),

        # Subtraction
        (na - na, np.array(a - a)),
        (nx - nx, x - x),
        (ncube1 - ncube2, cube1 - cube2),
        # Broadcasting
        (ncube1 - na, cube1 - a),
        (na - ncube1, a - cube1),
        (ncube1 - ny, cube1 - y),
        (ny - ncube1, y - cube1),
        (ncube1 - nrow_vec, cube1 - row_vec),
        (nrow_vec - ncube1, row_vec - cube1),

        # Multiplication
        (na * na, np.array(a * a)),
        (nx * nx, x * x),
        (nx * na, x * a),
        (na * nx, a * x),
        (ncube1 * ncube2, cube1 * cube2),
        # Broadcasting
        (ncube1 * na, cube1 * a),
        (na * ncube1, a * cube1),
        (ncube1 * ny, cube1 * y),
        (ny * ncube1, y * cube1),
        (ncube1 * nrow_vec, cube1 * row_vec),
        (nrow_vec * ncube1, row_vec * cube1),

        # Floor div
        (na // na, np.array(a // a)),
        (nx // nx, x // x),
        (nx // na, x // a),
        (na // nx, a // x),
        (ncube1 // ncube2, cube1 // cube2),
        # Broadcasting
        (ncube1 // na, cube1 // a),
        (na // ncube1, a // cube1),
        (ncube1 // ny, cube1 // y),
        (ny // ncube1, y // cube1),
        (ncube1 // nrow_vec, cube1 // row_vec),
        (nrow_vec // ncube1, row_vec // cube1))

    # Division
    assert_ndarrays_almost_eq(
        (na / na, np.array(a / a)),
        (nx / nx, x / x),
        (nx / na, x / a),
        (na / nx, a / x),
        (ncube1 / ncube2, cube1 / cube2),
        # Broadcasting
        (ncube1 / na, cube1 / a),
        (na / ncube1, a / cube1),
        (ncube1 / ny, cube1 / y),
        (ny / ncube1, y / cube1),
        (ncube1 / nrow_vec, cube1 / row_vec),
        (nrow_vec / ncube1, row_vec / cube1))

    # Missingness tests
    missing = hl.null(hl.tndarray(hl.tfloat64, 2))
    present = hl.nd.array(np.arange(10).reshape(5, 2))

    assert hl.eval(missing + missing) is None
    assert hl.eval(missing + present) is None
    assert hl.eval(present + missing) is None

@skip_unless_spark_backend()
@run_with_cxx_compile()
def test_ndarray_to_numpy():
    nd = np.array([[1, 2, 3], [4, 5, 6]])
    np.array_equal(hl.nd.array(nd).to_numpy(), nd)

@skip_unless_spark_backend()
@run_with_cxx_compile()
def test_ndarray_save():
    arrs = [
        np.array([[[1, 2, 3], [4, 5, 6]],
                  [[7, 8, 9], [10, 11, 12]]], dtype=np.int32),
        np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int64),
        np.array(3.0, dtype=np.float32),
        np.array([3.0], dtype=np.float64),
        np.array([True, False, True, True])
    ]

    for expected in arrs:
        with tempfile.NamedTemporaryFile(suffix='.npy') as f:
            hl.nd.array(expected).save(f.name)
            actual = np.load(f.name)

            assert expected.dtype == actual.dtype, f'expected: {expected.dtype}, actual: {actual.dtype}'
            assert np.array_equal(expected, actual)

@skip_unless_spark_backend()
@run_with_cxx_compile()
def test_ndarray_sum():
    np_m = np.array([[1, 2], [3, 4]])
    m = hl.nd.array(np_m)

    assert_all_eval_to(
        (m.sum(axis=0), np_m.sum(axis=0)),
        (m.sum(axis=1), np_m.sum(axis=1)),
        (m.sum(), np_m.sum()))

@skip_unless_spark_backend()
def test_ndarray_transpose():
    np_v = np.array([1, 2, 3])
    np_m = np.array([[1, 2, 3], [4, 5, 6]])
    np_cube = np.array([[[1, 2],
                         [3, 4]],
                        [[5, 6],
                         [7, 8]]])
    v = hl.nd.array(np_v)
    m = hl.nd.array(np_m)
    cube = hl.nd.array(np_cube)

    assert_ndarrays_eq(
        (v.T, np_v.T),
        (v.T, np_v),
        (m.T, np_m.T),
        (cube.transpose((0, 2, 1)), np_cube.transpose((0, 2, 1))),
        (cube.T, np_cube.T))

    assert hl.eval(hl.null(hl.tndarray(hl.tfloat, 1)).T) is None

    with pytest.raises(ValueError) as exc:
        v.transpose((1,))
    assert "Invalid axis: 1" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        cube.transpose((1, 1))
    assert "Expected 3 axes, got 2" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        cube.transpose((1, 1, 1))
    assert "Axes cannot contain duplicates" in str(exc.value)

@skip_unless_spark_backend()
def test_ndarray_matmul():
    np_v = np.array([1, 2])
    np_y = np.array([1, 1, 1])
    np_m = np.array([[1, 2], [3, 4]])
    np_m_f32 = np_m.astype(np.float32)
    np_m_f64 = np_m.astype(np.float64)
    np_r = np.array([[1, 2, 3], [4, 5, 6]])
    np_r_f32 = np_r.astype(np.float32)
    np_r_f64 = np_r.astype(np.float64)
    np_cube = np.arange(8).reshape((2, 2, 2))
    np_rect_prism = np.arange(12).reshape((3, 2, 2))
    np_broadcasted_mat = np.arange(4).reshape((1, 2, 2))
    np_six_dim_tensor = np.arange(3 * 7 * 1 * 9 * 4 * 5).reshape((3, 7, 1, 9, 4, 5))
    np_five_dim_tensor = np.arange(7 * 5 * 1 * 5 * 3).reshape((7, 5, 1, 5, 3))
    np_ones_int32 = np.ones((4, 4), dtype=np.int32)
    np_ones_float64 = np.ones((4, 4), dtype=np.float64)

    v = hl.nd.array(np_v)
    y = hl.nd.array(np_y)
    m = hl.nd.array(np_m)
    m_f32 = hl.nd.array(np_m_f32)
    m_f64 = hl.nd.array(np_m_f64)
    r = hl.nd.array(np_r)
    r_f32 = hl.nd.array(np_r_f32)
    r_f64 = hl.nd.array(np_r_f64)
    cube = hl.nd.array(np_cube)
    rect_prism = hl.nd.array(np_rect_prism)
    broadcasted_mat = hl.nd.array(np_broadcasted_mat)
    six_dim_tensor = hl.nd.array(np_six_dim_tensor)
    five_dim_tensor = hl.nd.array(np_five_dim_tensor)
    ones_int32 = hl.nd.array(np_ones_int32)
    ones_float64 = hl.nd.array(np_ones_float64)

    assert_ndarrays_eq(
        (v @ v, np_v @ np_v),
        (m @ m, np_m @ np_m),
        (m_f32 @ m_f32, np_m_f32 @ np_m_f32),
        (m_f64 @ m_f64, np_m_f64 @ np_m_f64),
        (m @ m.T, np_m @ np_m.T),
        (m_f64 @ m_f64.T, np_m_f64 @ np_m_f64.T),
        (r @ r.T, np_r @ np_r.T),
        (r_f32 @ r_f32.T, np_r_f32 @ np_r_f32.T),
        (r_f64 @ r_f64.T, np_r_f64 @ np_r_f64.T),
        (v @ m, np_v @ np_m),
        (m @ v, np_m @ np_v),
        (v @ r, np_v @ np_r),
        (r @ y, np_r @ np_y),
        (cube @ cube, np_cube @ np_cube),
        (cube @ v, np_cube @ np_v),
        (v @ cube, np_v @ np_cube),
        (cube @ m, np_cube @ np_m),
        (m @ cube, np_m @ np_cube),
        (rect_prism @ m, np_rect_prism @ np_m),
        (m @ rect_prism, np_m @ np_rect_prism),
        (m @ rect_prism.T, np_m @ np_rect_prism.T),
        (broadcasted_mat @ rect_prism, np_broadcasted_mat @ np_rect_prism),
        (six_dim_tensor @ five_dim_tensor, np_six_dim_tensor @ np_five_dim_tensor)
    )

    assert hl.eval(hl.null(hl.tndarray(hl.tfloat64, 2)) @ hl.null(hl.tndarray(hl.tfloat64, 2))) is None
    assert hl.eval(hl.null(hl.tndarray(hl.tint64, 2)) @ hl.nd.array(np.arange(10).reshape(5, 2))) is None
    assert hl.eval(hl.nd.array(np.arange(10).reshape(5, 2)) @ hl.null(hl.tndarray(hl.tint64, 2))) is None

    assert np.array_equal(hl.eval(ones_int32 @ ones_float64), np_ones_int32 @ np_ones_float64)

    with pytest.raises(ValueError):
        m @ 5

    with pytest.raises(ValueError):
        m @ hl.nd.array(5)

    with pytest.raises(ValueError):
        cube @ hl.nd.array(5)

    with pytest.raises(FatalError) as exc:
        hl.eval(r @ r)
    assert "Matrix dimensions incompatible: 3 2" in str(exc)

    with pytest.raises(FatalError) as exc:
        hl.eval(hl.nd.array([1, 2]) @ hl.nd.array([1, 2, 3]))
    assert "Matrix dimensions incompatible" in str(exc)

@skip_unless_spark_backend()
def test_ndarray_big():
    assert hl.eval(hl.nd.array(hl.range(100_000))).size == 100_000

@skip_unless_spark_backend()
def test_ndarray_full():
    assert_ndarrays_eq(
        (hl.nd.zeros(4), np.zeros(4)),
        (hl.nd.zeros((3, 4, 5)), np.zeros((3, 4, 5))),
        (hl.nd.ones(6), np.ones(6)),
        (hl.nd.ones((6, 6, 6)), np.ones((6, 6, 6))),
        (hl.nd.full(7, 9), np.full(7, 9)),
        (hl.nd.full((3, 4, 5), 9), np.full((3, 4, 5), 9))
    )

    assert hl.eval(hl.nd.zeros((5, 5), dtype=hl.tfloat32)).dtype, np.float32
    assert hl.eval(hl.nd.ones(3, dtype=hl.tint64)).dtype, np.int64
    assert hl.eval(hl.nd.full((5, 6, 7), hl.int32(3), dtype=hl.tfloat64)).dtype, np.float64

@skip_unless_spark_backend()
def test_ndarray_arange():
    assert_ndarrays_eq(
        (hl.nd.arange(40), np.arange(40)),
        (hl.nd.arange(5, 50), np.arange(5, 50)),
        (hl.nd.arange(2, 47, 13), np.arange(2, 47, 13))
    )

    with pytest.raises(FatalError) as exc:
        hl.eval(hl.nd.arange(5, 20, 0))
    assert "Array range cannot have step size 0" in str(exc)

@skip_unless_spark_backend()
def test_ndarray_mixed():
    assert hl.eval(hl.null(hl.tndarray(hl.tint64, 2)).map(lambda x: x * x).reshape((4, 5)).T) is None
    assert hl.eval(
        (hl.nd.zeros((5, 10)).map(lambda x: x - 2) +
         hl.nd.ones((5, 10)).map(lambda x: x + 5)).reshape(hl.null(hl.ttuple(hl.tint64, hl.tint64))).T.reshape((10, 5))) is None
    assert hl.eval(hl.or_missing(False, hl.nd.array(np.arange(10)).reshape((5,2)).map(lambda x: x * 2)).map(lambda y: y * 2)) is None

@skip_unless_spark_backend()
def test_ndarray_show():
    hl.nd.array(3).show()
    hl.nd.arange(6).show()
    hl.nd.arange(6).reshape((2, 3)).show()
    hl.nd.arange(8).reshape((2, 2, 2)).show()

@skip_unless_spark_backend()
def test_ndarray_qr():
    def assert_raw_equivalence(hl_ndarray, np_ndarray):
        ndarray_h, ndarray_tau = hl.eval(hl.nd.qr(hl_ndarray, mode="raw"))
        np_ndarray_h, np_ndarray_tau = np.linalg.qr(np_ndarray, mode="raw")

        rank = np.linalg.matrix_rank(np_ndarray)

        assert np.allclose(ndarray_h[:, :rank], np_ndarray_h[:, :rank])
        assert np.allclose(ndarray_tau[:rank], np_ndarray_tau[:rank])

    def assert_r_equivalence(hl_ndarray, np_ndarray):
        assert np.allclose(hl.eval(hl.nd.qr(hl_ndarray, mode="r")), np.linalg.qr(np_ndarray, mode="r"))

    def assert_reduced_equivalence(hl_ndarray, np_ndarray):
        q, r = hl.eval(hl.nd.qr(hl_ndarray, mode="reduced"))
        nq, nr = np.linalg.qr(np_ndarray, mode="reduced")

        rank = np.linalg.matrix_rank(np_ndarray)

        assert np.allclose(q[:, :rank], nq[:, :rank])
        assert np.allclose(r, nr)
        assert np.allclose(q @ r, np_ndarray)

    def assert_complete_equivalence(hl_ndarray, np_ndarray):
        q, r = hl.eval(hl.nd.qr(hl_ndarray, mode="complete"))
        nq, nr = np.linalg.qr(np_ndarray, mode="complete")

        rank = np.linalg.matrix_rank(np_ndarray)

        assert np.allclose(q[:, :rank], nq[:, :rank])
        assert np.allclose(r, nr)
        assert np.allclose(q @ r, np_ndarray)

    def assert_same_qr(hl_ndarray, np_ndarray):
        assert_raw_equivalence(hl_ndarray, np_ndarray)
        assert_r_equivalence(hl_ndarray, np_ndarray)
        assert_reduced_equivalence(hl_ndarray, np_ndarray)
        assert_complete_equivalence(hl_ndarray, np_ndarray)

    np_identity4 = np.identity(4)
    identity4 = hl.nd.array(np_identity4)

    assert_same_qr(identity4, np_identity4)

    np_all3 = np.full((3, 3), 3)
    all3 = hl.nd.full((3, 3), 3)

    assert_same_qr(all3, np_all3)

    np_nine_square = np.arange(9).reshape((3, 3))
    nine_square = hl.nd.arange(9).reshape((3, 3))

    assert_same_qr(nine_square, np_nine_square)

    np_wiki_example = np.array([[12, -51, 4],
                                [6, 167, -68],
                                [-4, 24, -41]])
    wiki_example = hl.nd.array(np_wiki_example)

    assert_same_qr(wiki_example, np_wiki_example)

    np_wide_rect = np.arange(12).reshape((3, 4))
    wide_rect = hl.nd.arange(12).reshape((3, 4))

    assert_same_qr(wide_rect, np_wide_rect)

    np_tall_rect = np.arange(12).reshape((4, 3))
    tall_rect = hl.nd.arange(12).reshape((4, 3))

    assert_same_qr(tall_rect, np_tall_rect)

    np_single_element = np.array([1]).reshape((1, 1))
    single_element = hl.nd.array([1]).reshape((1, 1))

    assert_same_qr(single_element, np_single_element)

    with pytest.raises(ValueError) as exc:
        hl.nd.qr(wiki_example, mode="invalid")
    assert "Unrecognized mode" in str(exc)

    with pytest.raises(AssertionError) as exc:
        hl.nd.qr(hl.nd.arange(6))
    assert "requires 2 dimensional" in str(exc)
