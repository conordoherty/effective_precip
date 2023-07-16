import pickle
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

nodata = -9999
data_dir = 'data_irrig'
dt_range = pd.date_range('2018-01-01', '2021-12-31')

et = pickle.load(open(f'data/et_interp.p', 'rb'))
dr = pickle.load(open(f'{data_dir}/dr_arr.p', 'rb'))
ph = pickle.load(open(f'{data_dir}/phantom_arr.p', 'rb'))

fig = plt.figure(figsize=(11, 4))

x, y = 229, 191
#plt.plot(dt_range, et[y, x, :])
plt.plot(dt_range, dr[y, x, :], label='Depletion')
plt.ylabel('Depletion (mm)')

ax2 = plt.gca().twinx()
ax2.plot(dt_range, ph[y, x, :], '.', color='red', label='Phantom ET', markersize=5)
ax2.set_ylabel('Phantom ET (mm)')

plt.tight_layout()
#fig.legend(fontsize=8, loc='upper center')
plt.savefig('pres/dep_phantom_irrig.png')
plt.show()
