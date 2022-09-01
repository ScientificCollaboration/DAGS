
from __future__ import absolute_import, unicode_literals
import os
from airflow.example_dags.plugins.workday import AfterWorkdayTimetable
from airflow.models import DAG
from datetime import timedelta
from typing import Optional
from pendulum import Date, DateTime, Time, timezone
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from RF91DB import Predict
from RF91DB import *
import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pendulum
import schedule
from datetime import datetime
# -*- coding: utf-8 -*-
from airflow.models import DAG
from datetime import datetime, timedelta
import sched, time

def sleep_long():
    """Sleep for 20 week"""
    time.sleep(10)

def sleep_short():
    """Sleep for 20 week"""
    time.sleep(4) 
    
default_args= {
    'owner': 'AlexZ',
    'email_on_failure': False,
    'email': ['alex@mail.com'],
    'retries': 2,
    'retry_delay': timedelta(seconds=3600),
    'start_date': datetime(2021, 12, 1)
}


p = Predict()

with DAG(
    "RF91DB",
    description='ML pipeline example 2',
    schedule_interval=timedelta(minutes=10),
    default_args=default_args, 
    catchup=False) as dag:
    

    readdata = PythonOperator(
            task_id='readdata',
            python_callable= p.read_data
        )

        # task: 2.1
    traindata = PythonOperator(
            task_id='traindata',
            python_callable= p.fitdata
        )
        # task: 2.1
    traindatax = PythonOperator(
            task_id='traindatax',
            python_callable= p.fitdata
        )
        # =======
        # task: 3.1        
    pred = PythonOperator(
            task_id='pred',
            python_callable= p.predictions
        )
        
        # =======
        # task: 4.1        
    saving_results = PythonOperator(
            task_id='saving_results',
            python_callable= p.save_data
        )
   
        # task: 1.1
    sleep_longid = PythonOperator(
            task_id='sleep_longid',
            python_callable= sleep_long
        )
     
        # task: 1.1
    sleep_shortid = PythonOperator(
            task_id='sleep_shortid',
            python_callable= sleep_short
        ) 
        
    def choose_branch():
        """
        Run an extra branch on the first day of the week
        """
        #weekday = run_after.weekday()
        weekday = datetime.today().weekday()
        if weekday == 7: #+Time.hour == 1 
            return 'sleep_longid' 
        else:
            return 'sleep_longid'  
            
        # task: 1.1
    branch_task = BranchPythonOperator(
            task_id='branch_task',
            python_callable= choose_branch
        )  
     
        # task: 1.1
    join_task = DummyOperator(
            task_id='join_task',
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        ) 
             
        
    readdata >> traindatax >> branch_task     
              
    branch_task >> sleep_longid >> traindata >> join_task 
    
    branch_task >> sleep_shortid >> join_task
    
    join_task >> pred >> saving_results
  
   
    
    
    
    
    
    
 

