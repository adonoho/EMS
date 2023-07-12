#!/usr/bin/env python3

import cvxpy as cp
import numpy as np
from numpy.random import Generator
from cvxpy.atoms import normNuc, multiply, norm
from pandas import DataFrame
from scipy import stats as st
from sklearn.utils.extmath import randomized_svd

from EMS.manager import active_remote_engine, do_on_cluster, unroll_experiment
from dask.distributed import Client, LocalCluster
import logging

logging.basicConfig(level=logging.INFO)


def seed(m: int, n: int, snr: float, p: float, mc: int) -> int:
    return round(1 + m * 1000 + n * 1000 + round(snr * 1000) + round(p * 1000) + mc * 100000)


def _df(c: list, l: list) -> DataFrame:
    d = dict(zip(c, l))
    return DataFrame(data=d, index=[0])


def df_experiment(m: int, n: int, snr: float, p: float, mc: int, t: float, cos_l: float, cos_r: float, sv0: float, sv1: float) -> DataFrame:
    c = ['m', 'n', 'snr', 'p', 'mc', 't', 'cosL', 'cosR', 'sv0', 'sv1']
    d = [m, n, snr, p, mc, t, cos_l, cos_r, sv0, sv1]
    return _df(c, d)


def suggested_t(observed, n):
    return np.sqrt(np.sum(observed) / n)


def make_data(m: int, n: int, p: float, rng: Generator) -> tuple:
    u = rng.normal(size=m)
    v = rng.normal(size=n)
    u /= np.linalg.norm(u)
    v /= np.linalg.norm(v)
    M = np.outer(u, v)
    noise = rng.normal(0, 1 / np.sqrt(m), (m, n))
    observes = st.bernoulli.rvs(p, size=(m, n), random_state=rng)

    return u, v, M, noise, observes


# problem setup
def nuc_norm_problem(Y, observed, t) -> tuple:
    X = cp.Variable(Y.shape)
    objective = cp.Minimize(normNuc(X))
    Z = multiply(X - Y, observed)
    constraints = [Z == 0] if t == 0. else [norm(Z, "fro") <= t]

    prob = cp.Problem(objective, constraints)

    prob.solve()

    return X, prob


# measurements
def vec_cos(v: np.array, vhat: np.array):
    return np.abs(np.inner(v, vhat))


def take_measurements(Mhat, u, v):
    uhatm, svv, vhatmh = np.linalg.svd(Mhat, full_matrices=False)
    cosL = vec_cos(u, uhatm[:, 0])
    cosR = vec_cos(v, vhatmh[0, :])

    return cosL, cosR, svv[0], svv[1]


def do_matrix_completion(*, m: int, n: int, snr: float, p: float, mc: int, tmethod='0') -> DataFrame:
    rng = np.random.default_rng(seed=seed(m, n, snr, p, mc))

    u, v, M, noise, obs = make_data(m, n, p, rng)
    t = 0. if tmethod == '0' else suggested_t(observed=obs, n=n)
    Y = snr * M + noise
    X, _ = nuc_norm_problem(Y=Y, observed=obs, t=t)
    Mhat = X.value

    cos_l, cos_r, sv0, sv1 = take_measurements(Mhat, u, v)

    return df_experiment(m, n, snr, p, mc, t, cos_l, cos_r, sv0, sv1)


def test_experiment() -> dict:
    # exp = dict(table_name='test',
    #            base_index=0,
    #            db_url='sqlite:///data/MatrixCompletion.db3',
    #            multi_res=[{
    #                'n': [10],
    #                'snr': [1.0],
    #                'p': [0.0],
    #                'mc': [0]
    #            }])
    # exp = dict(table_name='test',
    #            base_index=0,
    #            db_url='sqlite:///data/MatrixCompletion.db3',
    #            multi_res=[{
    #                'n': [round(p) for p in np.linspace(10, 100, 10)],
    #                'snr': [round(p, 0) for p in np.linspace(1, 10, 10)],
    #                'p': [round(p, 1) for p in np.linspace(0, 1, 11)],
    #                'mc': list(range(5))
    #            }])
    # exp = dict(table_name='mc:0001',
    #            base_index=0,
    #            db_url='sqlite:///data/MatrixCompletion.db3',
    #            multi_res=[{
    #                'n': [round(p) for p in np.linspace(10, 1000, 41)],
    #                'snr': [round(p, 3) for p in np.linspace(1, 20, 39)],
    #                'p': [round(p, 3) for p in np.linspace(0., 1., 41)],
    #                'mc': list(range(20))
    #            }])
    # exp = dict(table_name='mc-0002',
    #            base_index=0,
    #            db_url='sqlite:///data/MatrixCompletion.db3',
    #            multi_res=[{
    #                'n': [round(p) for p in np.linspace(10, 500, 21)],
    #                'snr': [round(p, 3) for p in np.linspace(1, 20, 39)],
    #                'p': [round(p, 3) for p in np.linspace(0., 1., 41)],
    #                'mc': list(range(20))
    #            }])
    # exp = dict(table_name='mc-0003',
    #            base_index=0,
    #            db_url='sqlite:///data/MatrixCompletion.db3',
    #            multi_res=[{
    #                # 'n': [round(p) for p in np.linspace(10, 500, 21)],
    #                'n': [500],
    #                'snr': [round(p, 3) for p in np.linspace(1, 20, 20)],
    #                'p': [round(p, 3) for p in np.linspace(0.05, 1., 20)],
    #                'mc': [20]
    #            }])
    # exp = dict(table_name='mc-0003',
    #            base_index=400,
    #            db_url='sqlite:///data/MatrixCompletion.db3',
    #            multi_res=[{
    #                'n': [500],
    #                'snr': [round(p, 3) for p in np.linspace(1, 20, 20)],
    #                'p': [1.],
    #                'mc': [20]
    #            },{
    #                'n': [500],
    #                'snr': [round(p, 3) for p in np.linspace(1, 20, 20)],
    #                'p': [2./3.],
    #                'mc': [20]
    #            }])
    # mr = exp['multi_res']
    # for snr in np.linspace(3., 6., 31):
    #     for x in np.linspace(1.5, 4.0, 26):
    #         p = (x / snr) ** 2
    #         if p <= 1.0:
    #             d = {
    #                 'n': [500],
    #                 'snr': [snr],
    #                 'p': [p],
    #                 'mc': [20]
    #             }
    #             mr.append(d)
    exp = dict(table_name='mc-0004',
               base_index=0,
               db_url='sqlite:///data/MatrixCompletion.db3',
               multi_res=[{
                   'm': [500],
                   'n': [500],
                   'snr': [round(p, 3) for p in np.linspace(1, 20, 20)],
                   'p': [round(p, 3) for p in np.linspace(0.05, 1., 20)],
                   'mc': [20]
               }])
    return exp


def do_local_experiment():
    exp = test_experiment()
    with LocalCluster(dashboard_address='localhost:8787') as cluster:
        with Client(cluster) as client:
            do_on_cluster(exp, do_matrix_completion, client)


def do_test():
    exp = test_experiment()
    print(exp)
    # params = unroll_experiment(exp)
    # for p in params:
    #     df = do_matrix_completion(**p)
    #     print(df)
    pass
    df = do_matrix_completion(m=12, n=8, snr=20., p=2./3., mc=20)
    print(df)


if __name__ == "__main__":
    do_local_experiment()
    # do_test()
