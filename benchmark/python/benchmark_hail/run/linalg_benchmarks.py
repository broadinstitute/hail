import hail as hl

from .utils import benchmark


@benchmark()
def block_matrix_nested_multiply():
    bm = hl.linalg.BlockMatrix.random(8 * 1024, 8 * 1024).checkpoint(hl.utils.new_temp_file(suffix='bm'))
    path = hl.utils.new_temp_file(suffix='bm')
    ((bm @ bm) @ bm @ bm @ (bm @ bm)).write(path, overwrite=True)


@benchmark()
def make_ndarray_bench():
    ht = hl.utils.range_table(200_000)
    ht = ht.annotate(x=hl._nd.array(hl.range(200_000)))
    ht._force_count()

@benchmark()
def ndarray_matmul_benchmark():
    arr = hl._nd.arange(4096 * 4096).reshape((4096, 4096))
    hl.eval(arr @ arr)