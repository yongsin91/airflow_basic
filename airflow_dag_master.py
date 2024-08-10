#!/usr/bin/env python
# coding: utf-8

# In[4]:


from datetime import datetime as dt
from datetime import timedelta 
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pandas as pd
import os


# In[10]:


def_args = {
    "start_date":dt(2024,6,15) }


# In[15]:


with DAG(
    "etl_master_dag", 
    default_args = def_args, 
    schedule = None ,
    catchup=False,
    tags =["self"],
    max_active_runs = 5) as dag:
    
    start = EmptyOperator (task_id = 'start' )
    end = EmptyOperator(task_id = 'end')

    base = TriggerDagRunOperator ( 
        task_id = 'base_dag',
        trigger_dag_id = 'ex_etl_dag',
        logical_date = '{{ds}}',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval = 3)

    with TaskGroup('taskgroup_triggerdag', tooltip='taskgroup for triggerdag') as taskgrouptriggerdag:
        a_dag = TriggerDagRunOperator ( 
            task_id = 'dag_a',
            trigger_dag_id = 'addition_dag_a',
            logical_date = '{{ds}}',
            reset_dag_run = True,
            wait_for_completion = True,
            poke_interval = 3)

        b_dag = TriggerDagRunOperator ( 
            task_id = 'dag_b',
            trigger_dag_id = 'addition_dag_b',
            logical_date = '{{ds}}',
            reset_dag_run = True,
            wait_for_completion = True,
            poke_interval = 3)
        a_dag >> b_dag

    c_dag = TriggerDagRunOperator ( 
        task_id = 'dag_c',
        trigger_dag_id = 'addition_dag_c',
        logical_date = '{{ds}}',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval = 3)

    d_dag = TriggerDagRunOperator ( 
        task_id = 'dag_d',
        trigger_dag_id = 'addition_dag_d',
        logical_date = '{{ds}}',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval = 3)
        
    start >> [base,taskgrouptriggerdag]  >> c_dag >> d_dag >> end
    # start >> base  >> c_dag >> d_dag >> end


# In[ ]:




