import math
import numpy as np
from numpy import array
from matplotlib import pyplot as plt
import pandas as pd
from pandas import Series, DataFrame


emp = { 'Name': ['Justin', 'Davies', 'Clark', 'Tris', 'Manual'], 'Location': ['BLR', 'HYD', 'MUM', 'PUNE', 'CHN'], 'Salary': [25000, 35000, 45000, 55000, 65000]
}

df = pd.DataFrame.from_dict(emp)  #creating pandas dataframe from dict


df["Domain"] = ''  #adding empty column Domain
df["Team"] = 'good'  #adding column Team with "good" value

df.loc[2, 'Team'] = 'testing' # updating Clark as "testing" Team

#adding Domain information as Development or Executives based on Salary condition
df.loc[df['Salary'] >= 45000, 'Domain'] = 'Executives'
df.loc[df['Salary'] < 45000, 'Domain'] = 'Development'
df