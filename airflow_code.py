#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd 


# In[12]:


# extra function to link all 3 objects, in order to pass the output dataset from extract to transform, then from transform to load
# using xcom to pass data among stage has its limitation. Complex items, e.g. DataFrame are unable to be passed over, as it is unable to be serialize
def etl(n1,b1,b2,ti):
    e_rtn_obj = extract_fn()
    t_rtn_obj = transform_fn(n1, e_rtn_obj)
    l_rtn_obj = load_fn(b1,b2,e_rtn_obj)


# In[13]:


def extract_fn():
    print("logic to extract data")
    
    details = {
        'cust_id': [1,2,3,4],
        'name': ['james', 'justin','jess','joyce'] }
    df = pd.DataFrame(details)
    return df

def transform_fn(n1, e_rtn_obj):
    extract = e_rtn_obj
    print(f"the value of variables are {n1} and {extract}")
    print("logic to transform data")

def load_fn(b1,b2, e_rtn_obj):
    extract = e_rtn_obj
    print(f"The value of b1 is {b1} and {extract}")
    print("The value of b2 is {}".format(b2))
    print("logic to load data")


# In[18]:


def join_task(a,b,ti):
    item_b = test_b(a)
    item_c = test_c(item_b) 

def test_a():
    A = 'item_A in test_a'
    return A

def test_b(a):
    print(f'received {a}')
    print("test_b end")
    return 'test_b'

def test_c(b):
    print(f'received {b}')
    print("test_c end")


# In[17]:


def test_e(d):
    print(f"external dag with item {d}") 
def test_f():
    print(f'last item')

