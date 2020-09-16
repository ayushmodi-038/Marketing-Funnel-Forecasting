# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 13:24:44 2020

@author: Ayush Modi
"""

import pandas as pd 

camp_data = pd.read_csv("../raw_data/All_Campaigns.csv",low_memory=False)
camp_data.columns
camp_data['Campaign Name'].value_counts()
camp_data['Campaign Type'].value_counts()

sum(camp_data['Start Date'].isna())
camp_data[camp_data['Start Date'].isna()]

pd.to_datetime(camp_data['Start Date']).describe()
camp_data['Start Date'].fillna(camp_data['Created Date'],inplace=True)

campaign_calendar=camp_data[['Start Date','Campaign Type']]
campaign_calendar.to_csv('../raw_data/campaign_calendar_type.csv',index=None,header=True)
campaign_calendar.loc[:,'Start Date']=pd.to_datetime(campaign_calendar.loc[:,'Start Date'])

freq="W-SUN"
period_seq=12

def get_derived_feature(data,date_col,campaign_calendar):

    if freq.startswith("W"):
        freq_="W"
    else:
        freq_=freq

    data['Weeks_Left_to_Year']=((data[date_col] + pd.tseries.offsets.YearEnd(n=0)) - data[date_col]).astype('timedelta64['+freq_+']').astype(int)
    data['Weeks_Left_to_Quarter']=((data[date_col] + pd.tseries.offsets.QuarterEnd(n=0)) - data[date_col]).astype('timedelta64['+freq_+']').astype(int)
    data['Weeks_Left_to_Month']=((data[date_col] + pd.tseries.offsets.MonthEnd(n=0)) - data[date_col]).astype('timedelta64['+freq_+']').astype(int)

    for col in campaign_calendar['Campaign Type'].value_counts().index:
        data[col]=0
    data['total_event_count']=0
    for index, row  in data.iterrows():
        pr_date=row[date_col]-  pd.to_timedelta(7, unit='d')
        cr_date=row[date_col]
        count = len(campaign_calendar[(campaign_calendar['Start Date']>=pr_date) & (campaign_calendar['Start Date']<cr_date)])
        data.loc[index,'total_event_count']=count    
        temp_c=campaign_calendar[(campaign_calendar['Start Date']>=pr_date) & (campaign_calendar['Start Date']<cr_date)]['Campaign Type'].value_counts()
        for col in temp_c.index:
            data.loc[index,col]=temp_c[col]

    return data

def make_future_df(data,date_col,campaign_calendar):
    max_date=max(data[date_col])
    new_df=pd.DataFrame(pd.date_range(start=max_date, periods=period_seq+1, freq=freq),columns=['date']).iloc[1:]
    new_df['lead_or_opp_id']=""
    new_df= get_derived_feature(new_df,'date',campaign_calendar)
    new_df.columns=data.columns
    predict_new_df=data.append(new_df)
    return predict_new_df


mdata=pd.read_csv("v_marketing_dashboard.csv")
# MQL conversion non-Partner Data
mql_data=mdata[mdata['pipeline_stage']=='MQL']
mql_data_not_partner=mql_data[mql_data['lead_source']!='Partner']
mql_data_not_partner=mql_data_not_partner[mql_data_not_partner['is_converted']==True]
mql_data_not_partner['converted_date']=pd.to_datetime(mql_data_not_partner['converted_date'])
mql_data_not_partner=mql_data_not_partner[~((mql_data_not_partner['converted_date']=='2020-06-20') & (mql_data_not_partner['converted_opportunity_id'].isna()== True))]
mql_data_not_partner_ts=mql_data_not_partner.groupby(mql_data_not_partner['converted_date'].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
mql_data_not_partner_ts.index=pd.DatetimeIndex(mql_data_not_partner_ts.index.to_timestamp())
mql_data_not_partner_ts=mql_data_not_partner_ts.resample(freq).sum()
mql_data_not_partner_ts=mql_data_not_partner_ts.reset_index()
mql_data_not_partner_ts= get_derived_feature(mql_data_not_partner_ts,'converted_date',campaign_calendar)

mql_data_not_partner_ts=mql_data_not_partner_ts[mql_data_not_partner_ts['converted_date']>='2019-12-25']

mql_data_not_partner_ts.to_csv("../processed_data/mql_sql/temp_mql_data_not_partner_ts.csv",index=None,header=True)
mql_data_not_partner_ts_predict=make_future_df(mql_data_not_partner_ts,'converted_date',campaign_calendar)
mql_data_not_partner_ts_predict.to_csv("../processed_data/mql_sql/temp_mql_data_not_partner_ts_predict.csv",index=None,header=True)
