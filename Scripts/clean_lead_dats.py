# -*- coding: utf-8 -*-
"""
Created on Fri Sep  4 14:12:17 2020

@author: Ayush Modi
"""

import pandas as pd
from os import path
import sys
sys.path.append(path.abspath('D://gp_common'))

from gp_common.code import all_functions as fun
# from gp_common.code import DB_Data as db

def clean_and_dice(data,keepCols=None,dateCols=None,empty_percent=70):
    
    if keepCols!=None:
        cleanDataMQL = data[keepCols]
    else:
        NullVals= fun.getNulls(data)
        colsToKeep = NullVals[NullVals["Percent_Nulls"]<= empty_percent]
        cleanDataMQL = data[list(colsToKeep.index)]

    if dateCols!=None:
        for dCols in dateCols:
            cleanDataMQL.loc[:, dCols]=pd.to_datetime(cleanDataMQL.loc[:, dCols])
    
    # Converting all columns to uppercase for uniformity
    for cols in list(cleanDataMQL.loc[:, cleanDataMQL.dtypes == 'object'].columns):
        cleanDataMQL.loc[:, cols] = cleanDataMQL.loc[:, cols].str.upper() 
    
    # Cleaning data for job level
    cleanDataMQL=fun.Cleancity(cleanDataMQL,'city','state','country')
    cleanDataMQL=fun.fillJobLevel(cleanDataMQL,'job__level__c','title')
    cleanDataMQL=fun.fillJobFunction(cleanDataMQL,'job__function__c','title')
    cleanDataMQL=fun.cleanCampaignSource(cleanDataMQL,'campaign__source__c')
    cleanDataMQL=fun.cleanLeadSource(cleanDataMQL,'lead_source')
    cleanDataMQL=fun.cleanindustry(cleanDataMQL,'industry')
    cleanDataMQL.drop(['title','postal_code'],axis=1,inplace=True)
    
    return cleanDataMQL


# Load Data
# mdata = pd.read_sql_query(db.getQuery(),db.get_db_engine())
# mdata.to_csv("../raw_data/MQL/DB_Data_MQL.csv", index = None, header=True)
mdata = pd.read_csv("../raw_data/MQL/DB_Data_MQL.csv",low_memory=False)

# MQL conversion non-Partner Data
# mql_data_not_partner=mql_data_not_partner[mql_data_not_partner['is_converted']==True]

empty_percent=70
NullVals= fun.getNulls(mdata)
colsToRemove = NullVals[NullVals["Percent_Nulls"]> empty_percent]
colsToKeep = NullVals[NullVals["Percent_Nulls"]<= empty_percent]
list(colsToRemove.index)
list(colsToKeep.index)

keepCols =['postal_code',
 'industry',
 'city',
 'job__level__c',
 'title',
 'job__function__c',
 'state',
 'hubspot_number_of_unique_form_submission__c',
 'campaign__source__c',
 'custom__hub_spot__score__c',
 'country',
 'lead_source',
 'partner_notified__c',
 'status',
 'self_generated__c',
 'autoconvert__c',
 'created_date',
 'is_unread_by_owner',
 'is_converted',
 'converted_date',
 'has_opted_out_of_email']

dateCols = ['created_date','converted_date']

cleaned_madata=clean_and_dice(mdata,keepCols,dateCols)
cleaned_madata.to_csv("../raw_data/MQL/Cleaned_DB_Data_MQL.csv", index = None, header=True)
