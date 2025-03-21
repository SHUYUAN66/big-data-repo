#!/usr/bin/env python3
import time, datetime, sys
import pandas as pd
import os
import time
dir_path = os.path.realpath(__file__)

# Extracted from: https://www.nature.com/articles/s41599-020-0523-3#Sec7 [Supplementary information]
dj30_df = pd.read_csv(os.path.dirname(dir_path) + '/data/dj30.csv')
dj30_df.dropna(inplace=True)
dj30_df = dj30_df.loc[:,['Long Date', 'Close']]
#print(dj30_df)
try:
    for _, row in dj30_df.iterrows():
        print(row['Long Date'], row['Close'], flush = True)
        time.sleep(1.0)
except (BrokenPipeError, IOError):
    print ('BrokenPipeError caught', file = sys.stderr)

