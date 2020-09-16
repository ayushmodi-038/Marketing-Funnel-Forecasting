# -*- coding: utf-8 -*-
"""
Created on Fri Sep 11 12:40:46 2020

@author: Ayush Modi
"""
import os
import pandas as pd 

def get_derived_feature(freq,data_df,date_col):

    if freq.startswith("W"):
        freq_="W"
        data_df['Weeks_Left_to_Year']=((data_df[date_col] + pd.tseries.offsets.YearEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
        data_df['Weeks_Left_to_Quarter']=((data_df[date_col] + pd.tseries.offsets.QuarterEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
        data_df['Weeks_Left_to_Month']=((data_df[date_col] + pd.tseries.offsets.MonthEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
    elif freq.startswith("M") :
        freq_=freq
        data_df['Months_Left_to_Year']=12-data_df[date_col].dt.month
        data_df['Months_Left_to_Quarter']=((data_df[date_col] + pd.tseries.offsets.QuarterEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
    elif freq.startswith("D"):
        freq_=freq
        data_df['Days_Left_to_Year']=((data_df[date_col] + pd.tseries.offsets.YearEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
        data_df['Days_Left_to_Quarter']=((data_df[date_col] + pd.tseries.offsets.QuarterEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
        data_df['Days_Left_to_Month']=((data_df[date_col] + pd.tseries.offsets.MonthEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
        data_df['Days_Left_to_Week']=((data_df[date_col] + pd.tseries.offsets.Week(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
    return data_df

def make_future_df(data,date_col,freq,target_col):
    max_date=max(data[date_col])
    new_df=pd.DataFrame(pd.date_range(start=max_date, periods=period_seq+1, freq=freq),columns=['date']).iloc[1:]
    new_df[target_col]=""
    new_df= get_derived_feature(freq,new_df,'date')
    new_df.rename(columns={'date': date_col}, inplace=True)
    predict_new_df=data.append(new_df)
    return predict_new_df

def get_ts(date_col=None,stage=None,final_col=None,freq="M",period_seq=12,partner=False,lead_source_col=None,dir_name="",remove_anamoly=False):
    mdata=pd.read_csv("v_marketing_dashboard.csv",low_memory=False)
    
    is_partner=""
    data=mdata[mdata['pipeline_stage']==stage]
    if partner==True:
        data=data[data[lead_source_col]=='Partner']
        is_partner="partner"
    else:
        data=data[data[lead_source_col]!='Partner']
        is_partner="non_partner"
        
    fname = stage.lower()+"_"+freq.lower()
    
    if stage=='Qualified Opportunity':
        fname = "closed_won"
        data=data[data['is_won']==True]
    
    if stage=='SAL':
        data=data[data['s_a_l__c']==True]
    
    if stage=='MQL':
        if date_col=="converted_date":
            data=data[data['is_converted']==True]
        if remove_anamoly==True:
            print("Removing Anamoly...")
            data=data[~((data['created_date']=='2020-06-20') & (data['converted_opportunity_id'].isna()== True))]
            
    data.loc[:,date_col]=pd.to_datetime(data.loc[:,date_col])
    
    data_ts=data.groupby(data[date_col].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
    data_ts.rename(columns={'lead_or_opp_id':final_col},inplace=True)
    
    data_ts.index=pd.DatetimeIndex(data_ts.index.to_timestamp())
    data_ts=data_ts.resample(freq).sum()
    data_ts=data_ts.reset_index()
    data_ts= get_derived_feature(freq,data_ts,date_col)
    data_ts=data_ts[data_ts[date_col]>'2017-01-01']
    
    data_ts.loc[:,final_col].plot()
    directory="../processed_data/"+dir_name
    
    if not os.path.exists(directory):
        os.makedirs(directory)
    data_ts.to_csv(directory+"/"+fname+"_data_"+is_partner+"_ts.csv",index=None,header=True)
    data_ts_predict=make_future_df(data_ts,date_col,freq,final_col)
    data_ts_predict.to_csv(directory+"/"+fname+"_data_"+is_partner+"_ts_predict.csv",index=None,header=True)
    
    # return data_ts

frequency="M";period_sequence=6
frequency="W-SUN";period_sequence=12
frequency="D";period_sequence=90

get_ts(date_col='created_date',stage='MQL',final_col='created',freq=frequency,period_seq=period_sequence,partner=False,lead_source_col='lead_source',dir_name="mql",remove_anamoly=True)
get_ts(date_col='created_date',stage='MQL',final_col='created',freq=frequency,period_seq=period_sequence,partner=True,lead_source_col='lead_source',dir_name="mql",remove_anamoly=True)

get_ts(date_col='converted_date',stage='MQL',final_col='created',freq=frequency,period_seq=period_sequence,partner=False,lead_source_col='lead_source',dir_name="mql_sql",remove_anamoly=True)
get_ts(date_col='converted_date',stage='MQL',final_col='created',freq=frequency,period_seq=period_sequence,partner=True,lead_source_col='lead_source',dir_name="mql_sql",remove_anamoly=True)

get_ts(date_col='created_date',stage='SQL',final_col='created',freq=frequency,period_seq=period_sequence,partner=False,lead_source_col='opportunity_lead_source',dir_name="mql_sql",remove_anamoly=False)
get_ts(date_col='created_date',stage='SQL',final_col='created',freq=frequency,period_seq=period_sequence,partner=True,lead_source_col='opportunity_lead_source',dir_name="mql_sql",remove_anamoly=False)

get_ts(date_col='sales__accepted__date__c',stage='SAL',final_col='sales_accepted',freq="W-SUN",period_seq=period_sequence,partner=False,lead_source_col='opportunity_lead_source',dir_name="mql_sql",remove_anamoly=False)
get_ts(date_col='sales__accepted__date__c',stage='SAL',final_col='sales_accepted',freq="W-SUN",period_seq=period_sequence,partner=True,lead_source_col='opportunity_lead_source',dir_name="mql_sql",remove_anamoly=False)

get_ts(date_col='close_date',stage='Qualified Opportunity',final_col='closed',freq=frequency,period_seq=period_sequence,partner=False,lead_source_col='opportunity_lead_source',dir_name="mql_sql",remove_anamoly=False)
get_ts(date_col='close_date',stage='Qualified Opportunity',final_col='closed',freq=frequency,period_seq=period_sequence,partner=True,lead_source_col='opportunity_lead_source',dir_name="mql_sql",remove_anamoly=False)
