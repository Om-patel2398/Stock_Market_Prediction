# -*- coding: utf-8 -*-
"""
Created on Tue May  2 14:46:49 2023

@author: bat_j
"""

def ML_model():
    
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    
    #saved_location = r'data/saved_files/Stocks_and_ETF_smallfile_Modified.parquet'
    saved_location = r'data/saved_files/Stocks_and_ETF_Modified.parquet'
    
    data = pd.read_parquet(saved_location, engine='fastparquet')
    
    # Assume `data` is loaded as a Pandas DataFrame
    data['Date'] = pd.to_datetime(data['Date'])
    data.set_index('Date', inplace=True)
    
    # Remove rows with NaN values
    data.dropna(inplace=True)
    
    # Select features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'
    
    X = data[features]
    y = data[target]
    
    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Create a RandomForestRegressor model
    model = LinearRegression()
    
    # Train the model
    model.fit(X_train, y_train)
    
    # Make predictions on test data
    y_pred = model.predict(X_test)
    
    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    
    print("Mean Absolute error: "+str(mae))
    print("Mean Squared error: "+str(mse))