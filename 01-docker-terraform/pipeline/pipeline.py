# Build a simple pipeline that process the data in the month given as argument and print the result. The pipeline should be able to run in a docker container.
import sys

import pandas as pd


print('arguments', sys.argv)
month = int(sys.argv[1])

df = pd.DataFrame({
    'day': [1, 2, 3, 4, 5, 6],
    'num_passengers': [10, 20, 30, 40, 50, 60]
})  
df['month'] = month
print(df.head())

df.to_parquet(f'output_{month}.parquet')
print (f"Processing data for month: {month}")