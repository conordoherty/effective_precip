import numpy as np
from numba import njit

# threshold ~ 0.67 inches
runoff_threshold = 17
CN = 75
S = (25400-254*CN)/CN

@njit
def do_wb(taw_full, pr_ts, et_ts, irrig_ts=None):
    if irrig_ts is None:
        irrig_ts = np.zeros(et_ts.size).astype('int')

    num_steps = et_ts.size

    d_ro = taw_full
    #d_ro = 0
    last_dr = d_ro
    taw = taw_full

    dr_ts = np.zeros(num_steps)
    dp_ts = np.zeros(num_steps)
    et_of_aw_ts = np.zeros(num_steps)

    irrig_on = False
    for i in range(num_steps):
        pr = pr_ts[i]
        et = et_ts[i]
        
        if pr >= runoff_threshold:
            ro = (pr-0.2*S)**2/(pr+0.8*S)
        else:
            ro = 0


        dp = max(pr - ro - et - last_dr, 0)
        dr = min(max(last_dr - pr + ro + et + dp, 0), taw)
        if last_dr + et - pr + ro > taw:
            et_of_aw = et-(dr-last_dr+pr-ro)
        else:
            et_of_aw = 0

        if et_of_aw > 0 and irrig_ts[i] == 1 and not irrig_on:
            # redo day with less TAW
            irrig_on = True
            #taw = taw_full/2
            #last_dr = last_dr - taw_full/2
            #i -= 1
            #continue
        elif irrig_ts[i] == 0:
            irrig_on = False
            #taw = taw_full

        if irrig_on:
            dr = min(dr, taw/2)

        pr_min_dp = pr - dp 
        last_dr = dr

        dr_ts[i] = dr
        dp_ts[i] = dp
        et_of_aw_ts[i] = et_of_aw

    return dr_ts, dp_ts, et_of_aw_ts
