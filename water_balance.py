import numpy as np
from numba import njit

@njit
def do_wb(taw, pr_ts, et_ts):
    num_steps = et_ts.size

    d_ro = taw
    #d_ro = 0
    last_r = d_ro

    dr_ts = np.zeros(num_steps)
    dp_ts = np.zeros(num_steps)
    phantom_ts = np.zeros(num_steps)

    for i in range(num_steps):
        pr = pr_ts[i]
        et = et_ts[i]

        dp = max(pr - et - last_r, 0)
        dr = min(max(last_r - pr + et + dp, 0), taw)
        if last_r - pr + et > taw:
            phantom = et-(dr-last_r+pr)
        else:
            phantom = 0
        pr_min_dp = pr - dp 
        added_to_rz = max(last_r-dr, 0)
        last_r = dr

        dr_ts[i] = dr
        dp_ts[i] = dp
        phantom_ts[i] = phantom

    return dr_ts, dp_ts, phantom_ts
