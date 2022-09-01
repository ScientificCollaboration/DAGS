from sklearn.linear_model import Ridge
from sklearn.linear_model import Lasso
import json
import numpy as np
import pandas as pd
from tqdm import tqdm
tqdm().pandas()
import sys, getopt
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputRegressor
from numpy import asarray
from pandas import read_csv
from pandas import DataFrame
from pandas import concat
from sklearn.metrics import mean_absolute_error
from xgboost import XGBRegressor
import pickle
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputRegressor
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error
from skforecast.ForecasterAutoreg import ForecasterAutoreg
from skforecast.ForecasterAutoregMultiOutput import ForecasterAutoregMultiOutput
from skforecast.model_selection import grid_search_forecaster
from skforecast.model_selection import backtesting_forecaster
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine
import utils.ml_pipeline_config as config
import requests
from array import *
import schedule
import time
import psycopg2 as pg
import pandas.io.sql as psql
from h3 import h3
import sched, time
import joblib
from joblib import dump, load
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import json
from airflow.models import Variable
from dotenv import load_dotenv

load_dotenv()


class Predict:
    
    def __init__(self):
      
        self.model_fname_ = 'model.pkl'
        
    def read_data(self):
       
        global datasetttt
       
        
        #connection = pg.connect("host=os.environ.get('PG_HOST'), os.environ.get('PG_PORT'), dbname=os.environ.get('PG_DATABASE'),  user=os.environ.get('PG_USER'), password=os.environ.get('PG_PASSWORD'), sslmode=require")
        
        connection = pg.connect("host=gw-sand-toyou.net.amhub.org dbname=amdelivery_sandbox user= password= sslmode=require")
        
        dataframedelivery = psql.read_sql('SELECT * FROM delivery_order LIMIT 20000', connection) #ORDER BY creation_date DESC
       
        #connection2 = pg.connect("host=os.environ.get('PG_HOST'), os.environ.get('PG_PORT'), dbname=os.environ.get('PG_DATABASE2'),  user=os.environ.get('PG_USER'), password=os.environ.get('PG_PASSWORD'), sslmode=require") 
            
        connection2 = pg.connect("host=gw-sand-toyou.net.amhub.org dbname=amdelivery_sandbox user= password= sslmode=require")
        dataframelocation = psql.read_sql('SELECT * FROM location  LIMIT 1000000', connection2)

        merged_2 = dataframedelivery.merge(dataframelocation, how='inner', left_on=["pick_up_location"], right_on=["id"])
        cols = ['lat','lon','creation_date']
        data = merged_2[cols]
        df2 = data
        df2['creation_date'] = data['creation_date'].astype('datetime64[h]')
        df2['orders'] = 1
        df2= df2.groupby(["lon","lat", "creation_date"], as_index=False)["orders"].count()
        df2 = df2.sort_values(by = ['creation_date'], ascending = [False])
        h3_level = 9
 
        def lat_lng_to_h3(row):
            return h3.geo_to_h3(
                row.lat, row.lon, h3_level)
 
        orders = df2.apply(lat_lng_to_h3, axis=1)
        df2['fid'] = orders
        df2 = df2.rename(columns={"creation_date": "time"})
        table = pd.pivot_table(df2, values='orders', index=['time'],columns=['fid'])
        table = table.fillna(0)
        table = table.sort_values(by = ['time'], ascending = [False])
         
        datasetttt = table[0:-1100]
        df = datasetttt.resample('H').sum().fillna(0)
        datasetttt = df.fillna(0)
        datasetttt = datasetttt[-1100:] 
        global hex_number
        hex_number = 100 # datasetttt[:]
        datasetttt = datasetttt.iloc[:,:hex_number]
        datasetttt.to_csv('datasetttt.csv',sep = ',')
        return datasetttt
    
    def fitdata(self):
        datasetttt = pd.read_csv('datasetttt.csv',sep = ',')
        datasetttt = datasetttt.set_index('time')
        global k
        global datasetlen
        datasetlen = 1000
        
        end_validation = 951
        end_train = 800
        global l
        global forecaster
        
        for index in range(datasetttt.shape[1]):
            columnSeriesObj = datasetttt.iloc[:, index]
            columnSeriesObj = pd.Series(list(columnSeriesObj))
            forecaster = ForecasterAutoregMultiOutput(
                RandomForestRegressor(max_depth=14),lags = 20,steps = 24)
            columnSeriesObj1 = columnSeriesObj[48:datasetlen + 48]
            columnSeriesObj2 = columnSeriesObj[24:datasetlen + 24]
            columnSeriesObj4 = columnSeriesObj[0:datasetlen]
            i = columnSeriesObj1.index
            columnSeriesObj2.index = i
    
            param_grid = {'n_estimators': [100, 500],'max_depth': [4, 6]}

            lags_grid = [[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24], [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]]
            results_grid = grid_search_forecaster(
                        forecaster  = forecaster,
                        y           = pd.Series(list(columnSeriesObj2)),  
                        exog        = pd.Series(list(columnSeriesObj4)),
                        param_grid  = param_grid,
                        lags_grid   = lags_grid,
                        steps       = 24,
                        metric      = 'mean_absolute_error',
                        refit       = False,
                        initial_train_size = 900,
                        return_best = True,
                        verbose     = False
                  )
            dump(forecaster, filename='forecastery.py')          
            return forecaster
        
    def predictions(self):
        global forecaster
        datasetttt = pd.read_csv('datasetttt.csv',sep = ',')
        datasetttt = datasetttt.set_index('time')
        end_train = 800
        end_validation = 951
        datasetlen = 1000
        ar = []
        #forecaster = load('forecastery.py')
        for index in range(datasetttt.shape[1]):
            columnSeriesObj = datasetttt.iloc[:, index]
            columnSeriesObj = pd.Series(list(columnSeriesObj))
            columnSeriesObj1 = columnSeriesObj[48:datasetlen + 48]
            columnSeriesObj2 = columnSeriesObj[24:datasetlen + 24]
            columnSeriesObj4 = columnSeriesObj[0:datasetlen]
            i = columnSeriesObj1.index
            columnSeriesObj2.index = i
            forecaster = ForecasterAutoregMultiOutput(
                RandomForestRegressor(max_depth=14),
                lags = 20,
                steps = 24
                )
            metric, predictions = backtesting_forecaster(
                            forecaster = forecaster,
                            y          = pd.Series(list(columnSeriesObj1)),
                            exog       = pd.Series(list(columnSeriesObj2)),
                            initial_train_size = len(columnSeriesObj[:end_validation]),
                            steps      = 24,
                            metric     = 'mean_absolute_error',
                            refit      = False,
                            verbose    = False)            
            ar.append(predictions.copy())
            print(ar)
        k = datasetlen - end_validation
        l = datasetlen - end_train    
        hex_number = 100    
        arr = np.array(ar)
        arr = arr.reshape(k,hex_number)
        arr = pd.DataFrame(arr,index=datasetttt[0:k].index,columns=datasetttt[0:hex_number].columns ) 
        arr.to_csv('arr.csv',sep = ',')
        
        print(arr)
                       
            
    def save_data(self):        
        datasetttt = pd.read_csv('datasetttt.csv',sep = ',')
        engine = create_engine('postgresql://alex:alex@localhost:5432/postgres') # to .env
        with engine.begin() as connection:
            datasetttt.to_sql('table10', con=connection, if_exists='replace')
            rs = connection.execute('SELECT * FROM table10')  
        datasetttt = pd.read_csv('arr.csv',sep = ',')
        engine = create_engine('postgresql://alex:alex@localhost:5432/postgres') # to .env
        with engine.begin() as connection:
            datasetttt.to_sql('table11', con=connection, if_exists='replace')
            rs2 = connection.execute('SELECT * FROM table11')
        
    
    def main(argv):
  
        if __name__ == "__main__":
            
            main(sys.argv[1:])

#Last version            

