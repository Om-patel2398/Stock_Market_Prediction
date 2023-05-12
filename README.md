# Stock_Market_Prediction

The Stock_Market_Prediction repo is a data pipline project. The Open source stock data is consumed, transformed and then utilized to train an ML Model. The data pipline is designed using Direct acyclic graphs (DAG) and implemented using AIRFLOW with help on docker

The following Repo Consist of following files & Folder
1. dags: Airflow required folder that contains all the DAG scripts
2. data: Additional folder mounted to airflow instance to read datasets/data file and store the results
3. logs: Airflow required folder that stores logs of each run
4. Dockerfile: docker build file, optional to use as everything is included in docker-compose
5. docker-compose: docker-compose YAML file. Run command docker-compose up airflow init followed by docker-compose up -d to start initialize and start airflow on docker
6. requirements: tagged to dockerfile to track and ensure all dependency librarires are included

THINGS TO NOTE:

- Due to size restriction of github, I havent uploaded actual raw data files. Once the repo is cloned, ensure to download the data into data/stock_data/ folder from https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset
- Due to memory restriction I ran the entire DAG using just a small chunk of data to test and confirm DAG is running successfully, as reflected in my LOG files.
- The performace of ML model might change depending on size of data
