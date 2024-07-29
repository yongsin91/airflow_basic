#!/usr/bin/env python
# coding: utf-8

# In[3]:


from datetime import datetime as dt
from datetime import timedelta 
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# In[4]:


def extract_fn():
    print("logic to extract data")

def transform_fn(n1):
    print("the value of a1 is ",n1)
    print("logic to transform data")

def load_fn(b1,b2):
    print("The value of b1 is {}".format(b1))
    print("The value of b2 is {}".format(b2))
    print("logic to load data")


# In[5]:


def etl_fn(n1,b1,b2):
    print("logic to extract data")
    print("the value of a1 is ",n1)
    print("logic to transform data")
    print("The value of b1 is {}".format(b1))
    print("The value of b2 is {}".format(b2))
    print("logic to load data")


# In[7]:


def_args = {
    "owner":"airflow",
    "retries":0,
    "retry_delay":timedelta(minutes=1),
    "start_date":dt(2024,6,15) }


# In[ ]:


with DAG("ex_py_operator",
         default_args = def_args,
         catchup=False) as dag:
    start = EmptyOperator(task_id = 'start')
    e = PythonOperator(
        task_id = 'extract',
        python_callable = extract_fn )
    t = PythonOperator(
        task_id = 'transform',
        python_callable = transform_fn ,
        op_args=['learning data engineering with airflow'])           
    l = PythonOperator(
        task_id = 'load',
        python_callable = load_fn,
        op_args = ['1st_var','2nd_var'])   
    end = EmptyOperator(task_id = 'end')

    start >> e >> t >> l >> end

