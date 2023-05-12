# -*- coding: utf-8 -*-
"""
Created on Sat Apr 29 03:44:23 2023

@author: Om Patel
"""

def Raw_Data_Processing():
    
    import pandas as pd
    #from local_config import meta_data_file, stock_data_file, etf_data_file, final_location
    import glob
    import os
    
    print('raw')
    
    meta_data_file = r'data/stock_data/symbols_valid_meta.csv'

    stock_data_file = r'data/stock_data/stocks/'

    etf_data_file = r'data/stock_data/etfs/'

    #save_location = r'data/saved_files/Stocks_and_ETF_smallfile.parquet'
    save_location = r'data/saved_files/Stocks_and_ETF.parquet'

    ''' Read metadata file '''
    meta_data_file = pd.read_csv(meta_data_file)
    
    # categorize the metadata file into stocks and ETF to reduce travesing speed
    # filter and keep only 'Symbol' & 'Security Name' columns to reduce size of dataframe
    
    stocks_metadata = meta_data_file[meta_data_file["ETF"] == "N"][['Symbol', 'Security Name']]
    
    ETF_metadata = meta_data_file[meta_data_file["ETF"] == "Y"][['Symbol', 'Security Name']]
    
    ''' Read all stocks and ETF file '''
    
    # get names of all individual stock csv files from folder
    all_stock_files = glob.glob(os.path.join(stock_data_file , "*.csv"))
    
    # get names of all individual etf csv files from folder
    all_etf_files = glob.glob(os.path.join(etf_data_file , "*.csv"))
    
    # empty list to store individual data frame
    every_file_as_dataframe = []
    
    '''
    the following FOR loop
    - trims the file name to get the symbol of stock/etf which is found in file name
    - reads individual CSV stock/etf file in sequence and converts it into dataframe
    - append symbol column to individual stock/etf dataframe
    - find the security name from stocks_metadata dataframe and append it as "Security Name" column in individual stock/etf dataframe
    - append the individual stock/etf dataframe into empty list declared above
    '''
    
    for filename in all_stock_files:
        try:
            symbol = filename.split('/')[-1].split(".csv")[0]
            individual_stock_dataframe = pd.read_csv(filename, index_col=None, header=0)
            individual_stock_dataframe["Symbol"] = symbol
            individual_stock_dataframe["Security Name"] = stocks_metadata[stocks_metadata["Symbol"] == symbol]["Security Name"].values[0]
            every_file_as_dataframe.append(individual_stock_dataframe)
        except:
            print(filename)
            return 0
    
    for filename in all_etf_files:
        symbol = filename.split("/")[-1].split(".csv")[0]
        individual_etf_dataframe = pd.read_csv(filename, index_col=None, header=0)
        individual_etf_dataframe["Symbol"] = symbol
        individual_etf_dataframe["Security Name"] = ETF_metadata[ETF_metadata["Symbol"] == symbol]["Security Name"].values[0]
        every_file_as_dataframe.append(individual_etf_dataframe)
    
    # Concate all individual stock dataframe into single dataframe
    Stocks_and_ETF = pd.concat(every_file_as_dataframe, axis=0, ignore_index=True)
    
    # Convert Volume column to int from float, but first convert all nan and null values to 0
    Stocks_and_ETF['Volume'] = Stocks_and_ETF['Volume'].fillna(0)
    
    
    #Stocks_and_ETF.iloc[0:100000].to_parquet(save_location, engine='fastparquet')
    # Store the final data frame as parquet file
    Stocks_and_ETF.to_parquet(save_location, engine='fastparquet')
    print('Raw data processing done!')
    
    return 0