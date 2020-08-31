# -*- coding: utf-8 -*-
"""
Created on Thu Aug 20 12:33:24 2020

@author: Mera Laptop
"""

import pandas as pd 

# Use seaborn style defaults and set the default figure size

freq="W-SUN"
period_seq=12

def get_derived_feature(data,date_col):

    if freq.startswith("W"):
        freq_="W"
    else:
        freq_=freq

    data['Weeks_Left_to_Year']=((data[date_col] + pd.tseries.offsets.YearEnd(n=0)) - data[date_col]).astype('timedelta64['+freq_+']').astype(int)
    data['Weeks_Left_to_Quarter']=((data[date_col] + pd.tseries.offsets.QuarterEnd(n=0)) - data[date_col]).astype('timedelta64['+freq_+']').astype(int)
    data['Weeks_Left_to_Month']=((data[date_col] + pd.tseries.offsets.MonthEnd(n=0)) - data[date_col]).astype('timedelta64['+freq_+']').astype(int)
    return data

def make_future_df(data,date_col):
    max_date=max(data[date_col])
    new_df=pd.DataFrame(pd.date_range(start=max_date, periods=period_seq+1, freq=freq),columns=['date']).iloc[1:]
    new_df['lead_or_opp_id']=""
    new_df= get_derived_feature(new_df,'date')
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
mql_data_not_partner_ts= get_derived_feature(mql_data_not_partner_ts,'converted_date')
mql_data_not_partner_ts=mql_data_not_partner_ts[mql_data_not_partner_ts['converted_date']>'2017-01-01']
mql_data_not_partner_ts.to_csv("../processed_data/mql_sql/mql_data_not_partner_ts.csv",index=None,header=True)
mql_data_not_partner_ts_predict=make_future_df(mql_data_not_partner_ts,'converted_date')
mql_data_not_partner_ts_predict.to_csv("../processed_data/mql_sql/mql_data_not_partner_ts_predict.csv",index=None,header=True)

# MQL conversion non-Partner Data based on SQL creation
sql_data=mdata[mdata['pipeline_stage']=='SQL']
sql_create_data_not_partner=sql_data[sql_data['opportunity_lead_source']!='Partner']
sql_create_data_not_partner['created_date']=pd.to_datetime(sql_create_data_not_partner['created_date'])
sql_create_data_not_partner_ts=sql_create_data_not_partner.groupby(sql_create_data_not_partner['created_date'].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
sql_create_data_not_partner_ts.index=pd.DatetimeIndex(sql_create_data_not_partner_ts.index.to_timestamp())
sql_create_data_not_partner_ts=sql_create_data_not_partner_ts.resample(freq).sum()
sql_create_data_not_partner_ts=sql_create_data_not_partner_ts.reset_index()
sql_create_data_not_partner_ts= get_derived_feature(sql_create_data_not_partner_ts,'created_date')
sql_create_data_not_partner_ts.to_csv("../processed_data/mql_sql/sql_create_data_not_partner_ts.csv",index=None,header=True)
sql_create_data_not_partner_ts_predict=make_future_df(sql_create_data_not_partner_ts,'created_date')
sql_create_data_not_partner_ts_predict.to_csv("../processed_data/mql_sql/sql_create_data_not_partner_ts_predict.csv",index=None,header=True)

# MQL conversion Partner Data
mql_data_partner=mql_data[mql_data['lead_source']=='Partner']
mql_data_partner=mql_data_partner[mql_data_partner['is_converted']==True]
mql_data_partner['converted_date']=pd.to_datetime(mql_data_partner['converted_date'])
mql_data_partner_ts=mql_data_partner.groupby(mql_data_partner['converted_date'].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
mql_data_partner_ts.index=pd.DatetimeIndex(mql_data_partner_ts.index.to_timestamp())
mql_data_partner_ts=mql_data_partner_ts.resample(freq).sum()
mql_data_partner_ts=mql_data_partner_ts.reset_index()
mql_data_partner_ts= get_derived_feature(mql_data_partner_ts,'converted_date')
mql_data_partner_ts.to_csv("../processed_data/mql_sql/mql_data_partner_ts.csv",index=None,header=True)
mql_data_partner_ts_predict=make_future_df(mql_data_partner_ts,'converted_date')
mql_data_partner_ts_predict.to_csv("../processed_data/mql_sql/mql_data_partner_ts_predict.csv",index=None,header=True)

# MQL conversion Partner Data based on SQL creation
sql_data=mdata[mdata['pipeline_stage']=='SQL']
sql_create_data_partner=sql_data[sql_data['opportunity_lead_source']=='Partner']
sql_create_data_partner['created_date']=pd.to_datetime(sql_create_data_partner['created_date'])
sql_create_data_partner_ts=sql_create_data_partner.groupby(sql_create_data_partner['created_date'].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
sql_create_data_partner_ts.index=pd.DatetimeIndex(sql_create_data_partner_ts.index.to_timestamp())
sql_create_data_partner_ts=sql_create_data_partner_ts.resample(freq).sum()
sql_create_data_partner_ts=sql_create_data_partner_ts.reset_index()
sql_create_data_partner_ts= get_derived_feature(sql_create_data_partner_ts,'created_date')
sql_create_data_partner_ts.to_csv("../processed_data/mql_sql/sql_create_data_partner_ts.csv",index=None,header=True)
sql_create_data_partner_ts_predict=make_future_df(sql_create_data_partner_ts,'created_date')
sql_create_data_partner_ts_predict.to_csv("../processed_data/mql_sql/sql_create_data_partner_ts_predict.csv",index=None,header=True)

# SQL to SAL conversion non-Partner Data
sal_data=mdata[mdata['pipeline_stage']=='SAL']
sal_data_not_partner=sal_data[sal_data['opportunity_lead_source']!='Partner']
sal_data_not_partner=sal_data_not_partner[sal_data_not_partner['s_a_l__c']==True]
sal_data_not_partner['sales__accepted__date__c']=pd.to_datetime(sal_data_not_partner['sales__accepted__date__c'])
sal_data_not_partner_ts=sal_data_not_partner.groupby(sal_data_not_partner['sales__accepted__date__c'].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
sal_data_not_partner_ts.index=pd.DatetimeIndex(sal_data_not_partner_ts.index.to_timestamp())
sal_data_not_partner_ts=sal_data_not_partner_ts.resample(freq).sum()
sal_data_not_partner_ts=sal_data_not_partner_ts.reset_index()
sal_data_not_partner_ts= get_derived_feature(sal_data_not_partner_ts,'sales__accepted__date__c')
#sal_data_not_partner_ts=sal_data_not_partner_ts[sal_data_not_partner_ts['sales__accepted__date__c']>'2017-01-01']
sal_data_not_partner_ts.to_csv("../processed_data/sql_sal/sal_data_not_partner_ts.csv",index=None,header=True)
sal_data_not_partner_ts_predict=make_future_df(sal_data_not_partner_ts,'sales__accepted__date__c')
sal_data_not_partner_ts_predict.to_csv("../processed_data/sql_sal/sal_data_not_partner_ts_predict.csv",index=None,header=True)

# SQL to SAL conversion Partner Data
sal_data=mdata[mdata['pipeline_stage']=='SAL']
sal_data_partner=sal_data[sal_data['opportunity_lead_source']=='Partner']
sal_data_partner=sal_data_partner[sal_data_partner['s_a_l__c']==True]
sal_data_partner['sales__accepted__date__c']=pd.to_datetime(sal_data_partner['sales__accepted__date__c'])
sal_data_partner_ts=sal_data_partner.groupby(sal_data_partner['sales__accepted__date__c'].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
sal_data_partner_ts.index=pd.DatetimeIndex(sal_data_partner_ts.index.to_timestamp())
sal_data_partner_ts=sal_data_partner_ts.resample(freq).sum()
sal_data_partner_ts=sal_data_partner_ts.reset_index()
sal_data_partner_ts= get_derived_feature(sal_data_partner_ts,'sales__accepted__date__c')
#sal_data_partner_ts=sal_data_partner_ts[sal_data_partner_ts['sales__accepted__date__c']>'2017-01-01']
sal_data_partner_ts.to_csv("../processed_data/sql_sal/sal_data_partner_ts.csv",index=None,header=True)
sal_data_partner_ts_predict=make_future_df(sal_data_not_partner_ts,'sales__accepted__date__c')
sal_data_partner_ts_predict.to_csv("../processed_data/sql_sal/sal_data_partner_ts_predict.csv",index=None,header=True)

# SAL to CW conversion non-Partner Data
cw_data=mdata[mdata['pipeline_stage']=='Qualified Opportunity']
cw_data_not_partner=cw_data[cw_data['opportunity_lead_source']!='Partner']
cw_data_not_partner=cw_data_not_partner[cw_data_not_partner['is_won']==True]
cw_data_not_partner['close_date']=pd.to_datetime(cw_data_not_partner['close_date'])
cw_data_not_partner_ts=cw_data_not_partner.groupby(cw_data_not_partner['close_date'].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
cw_data_not_partner_ts.index=pd.DatetimeIndex(cw_data_not_partner_ts.index.to_timestamp())
cw_data_not_partner_ts=cw_data_not_partner_ts.resample(freq).sum()
cw_data_not_partner_ts=cw_data_not_partner_ts.reset_index()
cw_data_not_partner_ts= get_derived_feature(cw_data_not_partner_ts,'close_date')
cw_data_not_partner_ts=cw_data_not_partner_ts[cw_data_not_partner_ts['close_date']>'2018-01-01']
cw_data_not_partner_ts.to_csv("../processed_data/sal_cw/cw_data_not_partner_ts.csv",index=None,header=True)
cw_data_not_partner_ts_predict=make_future_df(cw_data_not_partner_ts,'close_date')
cw_data_not_partner_ts_predict.to_csv("../processed_data/sal_cw/cw_data_not_partner_ts_predict.csv",index=None,header=True)

# SAL to CW conversion Partner Data
cw_data=mdata[mdata['pipeline_stage']=='Qualified Opportunity']
cw_data_partner=cw_data[cw_data['opportunity_lead_source']=='Partner']
cw_data_partner=cw_data_partner[cw_data_partner['is_won']==True]
cw_data_partner['close_date']=pd.to_datetime(cw_data_partner['close_date'])
cw_data_partner_ts=cw_data_partner.groupby(cw_data_partner['close_date'].dt.to_period(freq)).agg({'lead_or_opp_id':'nunique'})
cw_data_partner_ts.index=pd.DatetimeIndex(cw_data_partner_ts.index.to_timestamp())
cw_data_partner_ts=cw_data_partner_ts.resample(freq).sum()
cw_data_partner_ts=cw_data_partner_ts.reset_index()
cw_data_partner_ts= get_derived_feature(cw_data_partner_ts,'close_date')
cw_data_partner_ts=cw_data_partner_ts[cw_data_partner_ts['close_date']>'2018-01-01']
cw_data_partner_ts.to_csv("../processed_data/sal_cw/cw_data_partner_ts.csv",index=None,header=True)
cw_data_partner_ts_predict=make_future_df(cw_data_partner_ts,'close_date')
cw_data_partner_ts_predict.to_csv("../processed_data/sal_cw/cw_data_partner_ts_predict.csv",index=None,header=True)