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
        excess_et = et-(dr-last_dr+pr-ro)
        if excess_et > taw:
            et_of_aw = excess_et
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

@njit
def etof_interp(et_ts, eto_ts, nodata=-9999.):
    etof_ts = np.zeros_like(eto_ts)

    # fill leading empty values with first EToF
    first_et_ind = np.argmax(et_ts != nodata)
    first_et_val = et_ts[first_et_ind]
    first_etof = first_et_val / eto_ts[first_et_ind]
    etof_ts[:first_et_ind+1] = first_etof

    start_ind = first_et_ind
    for i in range(first_et_ind+1, et_ts.size):
        # find next non-missing et value
        if et_ts[i] != nodata:
            start_etof = et_ts[start_ind] / eto_ts[start_ind]
            end_etof = et_ts[i] / eto_ts[i]

            # linear interpolate from start to end
            etof_ts[start_ind:i] = np.linspace(start_etof, end_etof, i-start_ind)

            # set current index as start_ind
            start_ind = i
        # reach the end of et_ts and last value is missing
        elif i == et_ts.size-1 and et_ts[i] == nodata:
            etof_ts[start_ind:] = et_ts[start_ind] / eto_ts[start_ind]
        elif et_ts[i] == nodata:
            continue
        else:
            # something is wrong !!!
            #raise Excpetion('should not be here!!!')
            print('should not be here!!!')

    # return interpolated ET
    return etof_ts * eto_ts
