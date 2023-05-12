# -*- coding: utf-8 -*-
"""
Created on Mon May  1 06:18:04 2023

@author: bat_j
"""
def moving_mean_unit_test():
    
    import pandas as pd
    import random
    from statistics import mean
    
    #saved_location = r'data/saved_files/Stocks_and_ETF_smallfile_Modified.parquet'
    saved_location = r'data/saved_files/Stocks_and_ETF_Modified.parquet'
    
    # read parquet file
    Stocks_and_ETF_Modified = pd.read_parquet(saved_location, engine='fastparquet')
    
    random_index = random.randint(0,len(Stocks_and_ETF_Modified))
    
    random_moving_avg_volume = Stocks_and_ETF_Modified.loc[random_index,'vol_moving_avg']
    
    previous_30_days_volume = Stocks_and_ETF_Modified.loc[random_index-29:random_index,'Volume']
    
    print("Unit Test Moving Average")
    if float(random_moving_avg_volume)==float(mean(previous_30_days_volume)):
        return True
    else:
        return False
    

def moving_median_unit_test():
    
    import pandas as pd
    import random
    from statistics import mean
    
    #saved_location = r'data/saved_files/Stocks_and_ETF_smallfile_Modified.parquet'
    saved_location = r'data/saved_files/Stocks_and_ETF_Modified.parquet'
    
    # read parquet file
    Stocks_and_ETF_Modified = pd.read_parquet(saved_location, engine='fastparquet')
    
    random_index = random.randint(0,len(Stocks_and_ETF_Modified))
    
    random_moving_median_volume = Stocks_and_ETF_Modified.loc[random_index,'adj_close_rolling_med']
    
    previous_30_days_volume = Stocks_and_ETF_Modified.loc[random_index-29:random_index,'adj_close_rolling_med']
    
    print("Unit Test Moving Median")
    if float(random_moving_median_volume)==float(mean(previous_30_days_volume)):
        return True
    else:
        return False