# -*- coding: utf-8 -*-
"""
Created on Tue May  2 02:09:45 2023

@author: bat_j
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from python_scripts.Raw_Data_Processing import Raw_Data_Processing
from python_scripts.Feature_engineering import Feature_engineering
from python_scripts.unit_test import moving_mean_unit_test, moving_median_unit_test
from python_scripts.Applying_ML import ML_model

default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023,5,11)
    }


with DAG(
        default_args = default_args,
        dag_id = 'Data_Pipline_V1',
        description = 'DAG for the assessment',
        schedule_interval = '@daily'
    ) as dag:
    
    Data_Processing = PythonOperator(
        task_id = 'data_processing',
        python_callable = Raw_Data_Processing  
        )
    
    Feature_Engineering = PythonOperator(
        task_id = 'feature_engineering',
        python_callable = Feature_engineering
        )
    
    Unit_test_1 = PythonOperator(
        task_id = 'unit_test_mean',
        python_callable = moving_mean_unit_test
        )
    
    Unit_test_2 = PythonOperator(
        task_id = 'unit_test_median',
        python_callable = moving_median_unit_test  
        )
    
    Machine_learning_model = PythonOperator(
        task_id = 'Machine_learning_model',
        python_callable = ML_model  
        )
    
    Data_Processing >> Feature_Engineering
    Feature_Engineering >> Unit_test_1
    Feature_Engineering >> Unit_test_2
    Unit_test_1 >> Machine_learning_model
    Unit_test_2 >> Machine_learning_model