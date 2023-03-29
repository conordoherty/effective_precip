import pickle
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d

nodata = -9999

et_arr = pickle.load(open('et_arr.p', 'rb'))
et = np.ma.masked_where(et_arr==nodata, et_arr)

eto_arr = pickle.load(open('eto_arr.p', 'rb'))
etof = et/eto_arr

def interp(ts, kind='linear'):
    steps = np.arange(ts.size)
    x = steps[~ts.mask]
    y = ts[~ts.mask]

    func = interp1d(x, y, kind=kind, fill_value='extrapolate')

    return func(steps)

etof_interp = np.zeros_like(eto_arr)
for i in range(et.shape[0]):
    if i % 10 == 0:
        print(i)
    for j in range(et.shape[1]):
        etof_interp[i, j] = interp(etof[i, j])

pickle.dump(etof_interp, open('etof_interp.p', 'wb'))

et_interp = etof_interp*eto_arr
pickle.dump(et_interp, open('et_interp.p', 'wb'))

#ts = et[100, 100]

#plt.figure(figsize=(12, 4))
#plt.plot(ts, '-o')
#plt.plot(interp(ts), '--')
#plt.plot(interp(ts, kind='slinear'), '--')
#plt.tight_layout()
#plt.show()
