#!/usr/bin/env python3

from pathlib import Path
import json
from math import floor
import sys

from EMS.manager import get_gbq_credentials, read_json, dedup_experiment_from_db


def experiment() -> dict:
    N = 800
    n_idx = floor(N/2)
    k_idx = floor(N/2)
    mc_base = 1
    mc_range = 10
    exp = dict(
        table_name='SepEmpirical_donoho_Gaussian_800',
        params=[{
            'SUID': ['adonoho'],
            'ensemble': ['Gaussian'],
            'n_idx': list(range(1, n_idx+1)),
            'N': [800],
            'k_idx': list(range(1, k_idx+1)),
            'NMonte': list(range(mc_base, mc_base+mc_range))
        }]
    )
    return exp


if __name__ == "__main__":
    exp = experiment()
    pass
    params = dedup_experiment_from_db(exp, credentials=get_gbq_credentials())
    pass
    json.dump(params, sys.stdout, indent=4)
