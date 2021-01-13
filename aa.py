# coding: utf-8
import pandas as pd
import numpy as np
dt = pd.DataFrame({'c1': [1, 0, 1.1, np.nan,56, 88], 'c2': ['a', 'b', np.nan, np.nan\
, 'd', 'e'], 'c3': [np.nan, 62, 26, np.nan, np.nan,71], 'd4': \
[np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]})

dt1 = dt[dt['c1']>=1]

print(dt1)