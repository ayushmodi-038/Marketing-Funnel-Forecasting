# -*- coding: utf-8 -*-
"""
Created on Thu Sep  3 13:27:13 2020

@author: Ayush Modi
"""
import pandas as pd
import clean_lead_data as cld

cleaned_madata = cld.get_lead_data()

conv_data=cleaned_madata[cleaned_madata['is_converted']==True]
not_conv_data=cleaned_madata[cleaned_madata['is_converted']!=True]

conv_data.loc[:,'converted_date']=pd.to_datetime(conv_data.loc[:,'converted_date'])
conv_data.loc[:,'created_date']=pd.to_datetime(conv_data.loc[:,'created_date'])

conv_data.loc[:,'no_months_to_convert']= ((conv_data.loc[:,'converted_date']) - conv_data.loc[:,'created_date']).astype('timedelta64[M]').astype(int)

conv_data.to_csv("../processed_data/time_to_convert/Converted_MQL.csv", index = None, header=True)
not_conv_data.to_csv("../processed_data/time_to_convert/Not_Converted_MQL.csv", index = None, header=True)
