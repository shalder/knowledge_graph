import pandas as pd
import numpy as np
temp = pd.read_csv('skill_vertex.csv')
temp = set(temp[temp.iloc[:,0] == 'skill'].iloc[:,1])

cv_s = []
for i in range(20000):
    cv_s.append(np.random.choice(list(temp), 5 + int(np.abs(np.random.normal(10,10)))))

pd.DataFrame(cv_s).to_csv('jd_s.csv', index=False, header=None)
