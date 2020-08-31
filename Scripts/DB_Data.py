# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 14:34:39 2020

@author: Ayush
"""
from sqlalchemy import create_engine

# TODO: make below configurable from file.
 
def get_db_engine(db_name=None,user_name=None,password=None,host=None,port=None):
    # follows django database settings format, replace with your own settings
    """
    configured to use 'tableau_user' credentias by default
    """    
    if (db_name==None and 
       user_name==None and 
       password==None and 
       host==None and 
       port==None) :
            db_name= 'gpbidb'
            user_name= 'tableau_user'
            password= 'Th1717AL0ngAndC0mplexPa77w0rd!'
            host= 'gpbirepo.cxj2szm6sxyo.us-east-2.rds.amazonaws.com'
            port= 5432
    DATABASES = {
        'production':{
            'NAME': db_name,
            'USER': user_name,
            'PASSWORD': password,
            'HOST': host,
            'PORT': port,
        }
    }
    
    # choose the database to use
    db = DATABASES['production']
    
    # construct an engine connection string
    engine_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
        user = db['USER'],
        password = db['PASSWORD'],
        host = db['HOST'],
        port = db['PORT'],
        database = db['NAME'],
    )
    
    # create sqlalchemy engine
    engine = create_engine(engine_string)
    
    return engine
    # read a table from database into pandas dataframe, replace "tablename" with your table name

def getQuery(limit=-1):
    """
    Sample Query
    """
    query= "select * from repo.v_gp_sfdc_lead_current "+(("limit "+str(limit) + ";") if limit != -1 else ";")

    return query