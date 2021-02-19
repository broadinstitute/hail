import math

import pytest
import numpy as np

import hail as hl
from ..helpers import resource, startTestHailContext, stopTestHailContext, fails_local_backend, fails_service_backend

setUpModule = startTestHailContext
tearDownModule = stopTestHailContext


@fails_service_backend()
@fails_local_backend()
def test_hwe_normalized_pca():
    mt = hl.balding_nichols_model(3, 100, 50)
    eigenvalues, scores, loadings = hl.hwe_normalized_pca(mt.GT, k=2, compute_loadings=True)

    assert len(eigenvalues) == 2
    assert isinstance(scores, hl.Table)
    scores.count() == 100
    assert isinstance(loadings, hl.Table)

    _, _, loadings = hl.hwe_normalized_pca(mt.GT, k=2, compute_loadings=False)
    assert loadings is None


@fails_service_backend()
@fails_local_backend()
def test_pca_against_numpy():
    mt = hl.import_vcf(resource('tiny_m.vcf'))
    mt = mt.filter_rows(hl.len(mt.alleles) == 2)
    mt = mt.annotate_rows(AC=hl.agg.sum(mt.GT.n_alt_alleles()),
                          n_called=hl.agg.count_where(hl.is_defined(mt.GT)))
    mt = mt.filter_rows((mt.AC > 0) & (mt.AC < 2 * mt.n_called)).persist()
    n_rows = mt.count_rows()

    def make_expr(mean):
        return hl.if_else(hl.is_defined(mt.GT),
                          (mt.GT.n_alt_alleles() - mean) / hl.sqrt(mean * (2 - mean) * n_rows / 2),
                          0)

    eigen, scores, loadings = hl.pca(hl.bind(make_expr, mt.AC / mt.n_called), k=3, compute_loadings=True)
    hail_scores = scores.explode('scores').scores.collect()
    hail_loadings = loadings.explode('loadings').loadings.collect()

    assert len(eigen) == 3
    assert scores.count() == mt.count_cols()
    assert loadings.count() == n_rows

    assert len(scores.globals) == 0
    assert len(loadings.globals) == 0

    # compute PCA with numpy
    def normalize(a):
        ms = np.mean(a, axis=0, keepdims=True)
        return np.divide(np.subtract(a, ms), np.sqrt(2.0 * np.multiply(ms / 2.0, 1 - ms / 2.0) * a.shape[1]))

    g = np.pad(np.diag([1.0, 1, 2]), ((0, 1), (0, 0)), mode='constant')
    g[1, 0] = 1.0 / 3
    n = normalize(g)
    U, s, V = np.linalg.svd(n, full_matrices=0)
    np_scores = U.dot(np.diag(s)).flatten()
    np_loadings = V.transpose().flatten()
    np_eigenvalues = np.multiply(s, s).flatten()

    np.testing.assert_allclose(eigen, np_eigenvalues, rtol=1e-5)
    np.testing.assert_allclose(hail_scores, np_scores, rtol=1e-5)
    np.testing.assert_allclose(hail_loadings, np_loadings, rtol=1e-5)

@fails_service_backend()
@fails_local_backend()
def test_blanczos_against_numpy():

    def concatToNumpy(field, horizontal=True):
        blocks = field.collect()
        if horizontal:
            return np.concatenate(blocks, axis=0)
        else:
            return np.concatenate(blocks, axis=1)

    mt = hl.import_vcf(resource('tiny_m.vcf'))
    mt = mt.filter_rows(hl.len(mt.alleles) == 2)
    mt = mt.annotate_rows(AC=hl.agg.sum(mt.GT.n_alt_alleles()),
                          n_called=hl.agg.count_where(hl.is_defined(mt.GT)))
    mt = mt.filter_rows((mt.AC > 0) & (mt.AC < 2 * mt.n_called)).persist()
    n_rows = mt.count_rows()

    def make_expr(mean):
        return hl.if_else(hl.is_defined(mt.GT),
                          (mt.GT.n_alt_alleles() - mean) / hl.sqrt(mean * (2 - mean) * n_rows / 2),
                          0)

    k = 3

    float_expr = make_expr(mt.AC / mt.n_called)

    eigens, scores_t, loadings_t = hl._blanczos_pca(float_expr, k=k, q_iterations=7, compute_loadings=True)
    A = np.array(float_expr.collect()).reshape((3, 4)).T
    scores = concatToNumpy(scores_t.scores)
    loadings = concatToNumpy(loadings_t.loadings)
    scores = np.reshape(scores, (len(scores) // k, k))
    loadings = np.reshape(loadings, (len(loadings) // k, k))

    assert len(eigens) == 3
    assert scores_t.count() == mt.count_cols()
    assert loadings_t.count() == n_rows
    np.testing.assert_almost_equal(A @ loadings, scores)

    assert len(scores_t.globals) == 0
    assert len(loadings_t.globals) == 0

    # compute PCA with numpy
    def normalize(a):
        ms = np.mean(a, axis=0, keepdims=True)
        return np.divide(np.subtract(a, ms), np.sqrt(2.0 * np.multiply(ms / 2.0, 1 - ms / 2.0) * a.shape[1]))

    g = np.pad(np.diag([1.0, 1, 2]), ((0, 1), (0, 0)), mode='constant')
    g[1, 0] = 1.0 / 3
    n = normalize(g)
    U, s, V = np.linalg.svd(n, full_matrices=0)
    np_loadings = V.transpose()
    np_eigenvalues = np.multiply(s, s)

    def bound(vs, us):  # equation 12 from https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4827102/pdf/main.pdf
        return 1/k * sum([np.linalg.norm(us.T @ vs[:,i]) for i in range(k)])

    np.testing.assert_allclose(eigens, np_eigenvalues, rtol=0.05)
    assert bound(np_loadings, loadings) > 0.9


@fails_service_backend()
@fails_local_backend()
def test_blanczos_against_hail():
    k = 10

    def concatToNumpy(field, horizontal=True):
        blocks = field.collect()
        if horizontal:
            return np.concatenate(blocks, axis=0)
        else:
            return np.concatenate(blocks, axis=1)

    hl.utils.get_1kg('data/')
    hl.import_vcf('data/1kg.vcf.bgz').write('data/1kg.mt', overwrite=True)
    dataset = hl.read_matrix_table('data/1kg.mt')

    b_eigens, b_scores, b_loadings = hl._blanczos_pca(hl.int(hl.is_defined(dataset.GT)), k=k, q_iterations=3, compute_loadings=True)
    b_scores = concatToNumpy(b_scores.scores)
    b_loadings = concatToNumpy(b_loadings.loadings)
    b_scores = np.reshape(b_scores, (len(b_scores) // k, k))
    b_loadings = np.reshape(b_loadings, (len(b_loadings) // k, k))

    h_eigens, h_scores, h_loadings = hl.pca(hl.int(hl.is_defined(dataset.GT)), k=k, compute_loadings=True)
    h_scores = np.reshape(concatToNumpy(h_scores.scores), b_scores.shape)
    h_loadings = np.reshape(concatToNumpy(h_loadings.loadings), b_loadings.shape)

    # equation 12 from https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4827102/pdf/main.pdf
    def bound(vs, us):
        return 1/k * sum([np.linalg.norm(us.T @ vs[:,i]) for i in range(k)])

    MEV = bound(h_loadings, b_loadings)

    np.testing.assert_allclose(b_eigens, h_eigens, rtol=0.05)
    assert MEV > 0.9


@fails_service_backend()
@fails_local_backend()
def test_spectra():
    def make_spectral_matrix(index_func, k, m, n):
        sigma_dim = min(m, n)
        answer = np.zeros((m, n))
        for j in range(sigma_dim):
            answer[j, j] = index_func(j + 1, k)
        return answer

    def matrix_table_from_numpy(np_mat):
        rows, cols = np_mat.shape
        mt = hl.utils.range_matrix_table(rows, cols)
        mt = mt.annotate_globals(entries_global = np_mat)
        mt = mt.annotate_entries(ent = mt.entries_global[mt.row_idx, mt.col_idx])
        return mt

    # Defined for j >= 1
    def spec1(j, k):
        return 1/j

    def spec2(j, k):
        if j == 1:
            return 1
        if j <= k:
            return 2 * 10**-5
        else:
            return (10**-5) * (k + 1)/j

    def spec3(j, k):
        if j <= k:
            return 10**(-5*(j-1)/(k-1))
        else:
            return (10**-5)*(k+1)/j

    def spec4(j, k):
        if j <= k:
            return 10**(-5*(j-1)/(k-1))
        elif j == (k + 1):
            return 10**-5
        else:
            return 0

    def spec5(j, k):
        if j <= k:
            return 10**-5 + (1 - 10**-5)*(k - j)/(k - 1)
        else:
            return 10**-5 * math.sqrt((k + 1)/j)

    spectral_functions = [spec1, spec2, spec3, spec4, spec5]

    # k, m, n
    dim_triplets = [(10, 1000, 1000), (20, 1000, 1000), (10, 100, 200)]

    for triplet in dim_triplets:
        k, m, n = triplet
        for idx, spec_func in enumerate(spectral_functions):
            sigma = make_spectral_matrix(spec_func, k, m, n)
            seed = 1025
            np.random.seed(seed)
            U = np.linalg.qr(np.random.normal(0, 1, (m, m)))[0]
            V = np.linalg.qr(np.random.normal(0, 1, (n, n)))[0]
            A = U @ sigma @ V
            mt_A = matrix_table_from_numpy(A)

            eigenvalues, scores, loadings = hl._blanczos_pca(mt_A.ent, k=k, oversampling_param=k, compute_loadings=True, q_iterations=4)
            singulars = np.sqrt(eigenvalues)
            hail_V = (np.array(scores.scores.collect()) / singulars).T
            hail_U = np.array(loadings.loadings.collect())
            approx_A = hail_U @ np.diag(singulars) @ hail_V
            norm_of_diff = np.linalg.norm(A - approx_A, 2)
            np.testing.assert_allclose(norm_of_diff, spec_func(k + 1, k), rtol=1e-02, err_msg=f"Norm test failed on triplet {triplet} on spec{idx + 1}")
            np.testing.assert_allclose(singulars, np.diag(sigma)[:k], rtol=1e-01, err_msg=f"Failed on triplet {triplet} on spec{idx + 1}")

