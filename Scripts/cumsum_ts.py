# -*- coding: utf-8 -*-
"""
Created on Fri Sep 11 12:40:46 2020

@author: Ayush Modi
"""

import pandas as pd 
import numpy as np

freq="M"

period_seq=12

def get_derived_feature(freq,data_df,date_col):

    if freq.startswith("W"):
        freq_="W"
        data_df['Weeks_Left_to_Year']=((data_df[date_col] + pd.tseries.offsets.YearEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
        data_df['Weeks_Left_to_Quarter']=((data_df[date_col] + pd.tseries.offsets.QuarterEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
        data_df['Weeks_Left_to_Month']=((data_df[date_col] + pd.tseries.offsets.MonthEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
    
    else:
        freq_=freq
        data_df['Months_Left_to_Year']=12-data_df[date_col].dt.month
        data_df['Months_Left_to_Quarter']=((data_df[date_col] + pd.tseries.offsets.QuarterEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
        
    return data_df

def make_future_df(data,date_col,freq,target_col):
    max_date=max(data[date_col])
    new_df=pd.DataFrame(pd.date_range(start=max_date, periods=period_seq+1, freq=freq),columns=['date']).iloc[1:]
    new_df[target_col]=""
    new_df= get_derived_feature(freq,new_df,'date')
    new_df.rename(columns={'date': date_col}, inplace=True)
    predict_new_df=data.append(new_df)
    return predict_new_df

def get_cumsum(date_col=None,stage=None,final_col=None,freq="M",period_seq=12,partner=False,lead_source_col=None):
    mdata=pd.read_csv("v_marketing_dashboard.csv",low_memory=False)
    # MQL conversion non-Partner Data
    is_partner=""
    data=mdata[mdata['pipeline_stage']==stage]
    if partner==True:
        data_not_partner=data[data[lead_source_col]=='Partner']
        is_partner="partner"
    else:
        data_not_partner=data[data[lead_source_col]!='Partner']
        is_partner="non_partner"
        
    fname = "cumsum_"+stage.lower()
    
    if stage=='Qualified Opportunity':
        fname = "cumsum_closed_won"
        data_not_partner=data_not_partner[data_not_partner['is_won']==True]
    
    if stage=='SAL':
        data_not_partner=data_not_partner[data_not_partner['s_a_l__c']==True]
    
    data_not_partner.loc[:,date_col]=pd.to_datetime(data_not_partner.loc[:,date_col])
    
    data_not_partner_ts=data_not_partner.groupby(data_not_partner[date_col].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
    data_not_partner_ts.rename(columns={'lead_or_opp_id':final_col},inplace=True)
    
    data_not_partner_ts.index=pd.DatetimeIndex(data_not_partner_ts.index.to_timestamp())
    data_not_partner_ts=data_not_partner_ts.resample(freq).sum()
    data_not_partner_ts=data_not_partner_ts.reset_index()
    data_not_partner_ts= get_derived_feature(freq,data_not_partner_ts,date_col)
    data_not_partner_ts=data_not_partner_ts[data_not_partner_ts[date_col]>'2017-01-01']
    
    data_not_partner_ts.loc[:,final_col]=data_not_partner_ts.loc[:,final_col].cumsum()
    data_not_partner_ts.loc[:,final_col].plot()
    data_not_partner_ts.to_csv("../processed_data/cumsum/"+fname+"_data_created_"+is_partner+"_ts.csv",index=None,header=True)
    data_not_partner_ts_predict=make_future_df(data_not_partner_ts,date_col,freq,final_col)
    data_not_partner_ts_predict.to_csv("../processed_data/cumsum/"+fname+"_data_created_"+is_partner+"_ts_predict.csv",index=None,header=True)

get_cumsum(date_col='created_date',stage='MQL',final_col='created',freq="M",period_seq=12,partner=False,lead_source_col='lead_source')
get_cumsum(date_col='created_date',stage='MQL',final_col='created',freq="M",period_seq=12,partner=True,lead_source_col='lead_source')

get_cumsum(date_col='created_date',stage='SQL',final_col='created',freq="M",period_seq=12,partner=False,lead_source_col='opportunity_lead_source')
get_cumsum(date_col='created_date',stage='SQL',final_col='created',freq="M",period_seq=12,partner=True,lead_source_col='opportunity_lead_source')

get_cumsum(date_col='sales__accepted__date__c',stage='SAL',final_col='sales_accepted',freq="W-SUN",period_seq=12,partner=False,lead_source_col='opportunity_lead_source')
get_cumsum(date_col='sales__accepted__date__c',stage='SAL',final_col='sales_accepted',freq="W-SUN",period_seq=12,partner=True,lead_source_col='opportunity_lead_source')

get_cumsum(date_col='close_date',stage='Qualified Opportunity',final_col='closed',freq="M",period_seq=12,partner=False,lead_source_col='opportunity_lead_source')
get_cumsum(date_col='close_date',stage='Qualified Opportunity',final_col='closed',freq="M",period_seq=12,partner=True,lead_source_col='opportunity_lead_source')
