#!/usr/bin/env python
# coding: utf-8

# # We Will make the imports 

# In[29]:


'''You need to have elasticsearch connector jars downloaded first , use google and search for elasticsearc spark connector , download the jar'''


# In[30]:


import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession,SQLContext
from elasticsearch import Elasticsearch 


# We will now `create` object of `Elasticsearch`

# In[2]:


es = Elasticsearch()


# we will create a sparksession , sqlcontext and set spark sonf 

# In[6]:


sp = SparkSession.builder.config('spark.jars', 'C:\JARS\jars\elasticsearch-hadoop-7.7.0.jar').getOrCreate()


# In[14]:


sqc = SQLContext(sparkContext=sp.sparkContext,sparkSession=sp)


# In[26]:


conf = SparkConf()


# `this prop will auto create a index in elastic search otherwise it will throw an error`

# In[27]:


conf.set("es.index.auto.create","true")


# In[17]:


#reading data
data = sqc.read.csv('ab.csv',header=True,inferSchema=True)


# In[19]:


data.show(10)


# # Writing Data to `elasticsearch`

# In[21]:


#now writing data into elastic search , in .save("#/#") replace # with your indec name and format (they both are index only format dosent matter)
data.write.format("org.elasticsearch.spark.sql").save("fighter/csv")


# # Reading Data from `elasticsearch`

# In[23]:


els = sqc.read.format("org.elasticsearch.spark.sql").load("fighter/csv")


# In[24]:


els.show()


# Done 

# In[ ]:




