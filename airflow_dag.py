#!/usr/bin/env python
# coding: utf-8

# In[10]:


from datetime import datetime as dt
from datetime import timedelta 
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import pandas as pd
import os


# In[23]:


root_folder_path = '/mnt/d/WSL/Ubuntu-22.04/yongsin91/airflow_basic'
os.chdir(root_folder_path)
exec(open('airflow_code.py').read())


# In[24]:


def_args = {
    "owner":"airflow",
    "retries":0,
    "retry_delay":timedelta(minutes=1),
    "start_date":dt(2024,7,16) }
    # 'schedule_interval':'@monthly'}


# In[25]:


with DAG("ex_etl_dag", default_args = def_args, schedule = None, catchup=False, max_active_runs = 5) as dag:
    start = EmptyOperator ( task_id = 'start' )
    # etl = PythonOperator( 
    #     task_id = 'extract_transform_load', 
    #     python_callable = etl,
    #     op_args = ['variable N1','variable_B1','variable_b2'] )
    end = EmptyOperator(task_id = 'end')

    # start >> etl >> end
    with TaskGroup('testing_taskgroup', tooltip='taskgroup for all remaining') as tg:
        task_a = PythonOperator( task_id = 'extract_transform_load', 
                                 python_callable = etl,
                                 op_args = ['variable N1','variable_B1','variable_b2'] )
        task_b = PythonOperator( task_id = 'taskgroup_task_b', 
                                 python_callable = join_task,
                                 op_args = ['join_task_1','join_task_2'] )
        task_c = PythonOperator( task_id = 'taskgroup_test_c', 
                                 python_callable = test_a )
        task_c >> task_b

    ind_e = PythonOperator(
            task_id = 'test_e_last',
            python_callable = test_e ,
            op_args=['item_e'])           
    ind_f = PythonOperator(
            task_id = 'test_f_last',
            python_callable = test_f )           

    start >> tg >> ind_e >> ind_f >> end


# In[26]:


with DAG("addition_dag_a", default_args = def_args, schedule = None, catchup=False, max_active_runs = 5) as dag:
    start1 = EmptyOperator ( task_id = 'start' )
    # etl = PythonOperator( 
    #     task_id = 'extract_transform_load', 
    #     python_callable = etl,
    #     op_args = ['variable N1','variable_B1','variable_b2'] )
    end1 = EmptyOperator(task_id = 'end')

    ind_e1 = PythonOperator(
            task_id = 'test_e_last',
            python_callable = test_e ,
            op_args=['item_e'])           
    ind_f1 = PythonOperator(
            task_id = 'test_f_last',
            python_callable = test_f )           

    start1 >> ind_e1 >> ind_f1 >> end1


# In[27]:


with DAG("addition_dag_b", default_args = def_args, schedule = None, catchup=False, max_active_runs = 5) as dag:
    start2 = EmptyOperator ( task_id = 'start' )
    # etl = PythonOperator( 
    #     task_id = 'extract_transform_load', 
    #     python_callable = etl,
    #     op_args = ['variable N1','variable_B1','variable_b2'] )
    end2 = EmptyOperator(task_id = 'end')         
    ind_f2 = PythonOperator(
            task_id = 'test_f_last',
            python_callable = test_f )           

    start2 >> ind_f2 >> end2


# In[28]:


with DAG("addition_dag_c", default_args = def_args, schedule = None, catchup=False, max_active_runs = 5) as dag:
    start3 = EmptyOperator ( task_id = 'start' )
    # etl = PythonOperator( 
    #     task_id = 'extract_transform_load', 
    #     python_callable = etl,
    #     op_args = ['variable N1','variable_B1','variable_b2'] )
    end3 = EmptyOperator(task_id = 'end')
          
    ind_f3 = PythonOperator(
            task_id = 'test_f_last',
            python_callable = test_f )           

    start3 >> ind_f3 >> end3


# In[29]:


with DAG("addition_dag_d", default_args = def_args, schedule = None, catchup=False) as dag:
    start4 = EmptyOperator ( task_id = 'start' )
    # etl = PythonOperator( 
    #     task_id = 'extract_transform_load', 
    #     python_callable = etl,
    #     op_args = ['variable N1','variable_B1','variable_b2'] )
    end4 = EmptyOperator(task_id = 'end')
     
    ind_f4 = PythonOperator(
            task_id = 'test_f_last',
            python_callable = test_f )           

    start4 >> ind_f4 >> end4


# In[ ]:




