# -*- coding: utf-8 -*-
"""
Created on Sun Apr 30 15:12:37 2023

@author: bat_j
"""

def Feature_engineering():

    import pandas as pd
    
    #save_location = r'data/saved_files/Stocks_and_ETF_smallfile.parquet'
    #modifed_saved_location = r'data/saved_files/Stocks_and_ETF_smallfile_Modified.parquet'
    
    save_location = r'data/saved_files/Stocks_and_ETF.parquet'
    modifed_saved_location = r'data/saved_files/Stocks_and_ETF_Modified.parquet'
    
    window_size = 30
    
    # read parquet file
    Stocks_and_ETF = pd.read_parquet(save_location, engine='fastparquet')
    Stocks_and_ETF.sort_values(by=["Symbol","Date"],inplace=True)
    
    # Empty list to store Moving average volume & Moving median volume and add it to dataframe
    MovingAverageVolume = []
    MovingMedianVolume = []
    
    # group by records by Symbol
    group_by_records = Stocks_and_ETF.groupby(by="Symbol")
    
    # Get all Symbols from grouped data frame
    All_Symbol = list(group_by_records.groups.keys())
    
    '''
    the following FOR loop
    - get each individual company dataframe from grouped by dataframe
    - calculate moving average and median and stores result in empty list declared above
    '''
    
    for company in All_Symbol:
        
        individual_company = group_by_records.get_group(company)    
        MovingAverageVolume.extend(individual_company['Volume'].rolling(window = window_size,min_periods = 30).mean().to_list())
        MovingMedianVolume.extend(individual_company['Volume'].rolling(window = window_size,min_periods = 30).median().to_list())
        
        ''' 
        The following code also works but as per my testing the above code works 
        with less memory and generates exact result 
        
        individual_company.loc[:,'Moving_Average_Trading_Volume'] = individual_company['Volume'].rolling(window = 30).mean()
        Stocks_and_ETF_Modified = pd.concat([Stocks_and_ETF_Modified,individual_company]) 
        '''
    
    # Add the newly generated column in existing dataframe
    Stocks_and_ETF['vol_moving_avg'] = MovingAverageVolume
    Stocks_and_ETF['adj_close_rolling_med'] = MovingMedianVolume
    
    # Save file
    Stocks_and_ETF.to_parquet(modifed_saved_location, engine='fastparquet')
    print('New columns added')