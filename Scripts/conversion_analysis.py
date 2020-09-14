import pandas as pd 
import numpy as np

freq="M"

period_seq=12

def get_derived_feature(data_df,date_col):

    if freq.startswith("W"):
        freq_="W"
    else:
        freq_=freq

    data_df['Months_Left_to_Year']=12-data_df[date_col].dt.month
    data_df['Months_Left_to_Quarter']=((data_df[date_col] + pd.tseries.offsets.QuarterEnd(n=0)) - data_df[date_col]).astype('timedelta64['+freq_+']').astype(int)
    return data_df

def make_future_df(data,date_col,target_col):
    max_date=max(data[date_col])
    new_df=pd.DataFrame(pd.date_range(start=max_date, periods=period_seq+1, freq=freq),columns=['date']).iloc[1:]
    new_df[target_col]=""
    new_df= get_derived_feature(new_df,'date')
    new_df.rename(columns={'date': date_col}, inplace=True)
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
mql_data_not_partner_ts.rename(columns={'lead_or_opp_id':'converted'},inplace=True)

mql_data_not_partner_ts.index=pd.DatetimeIndex(mql_data_not_partner_ts.index.to_timestamp())
mql_data_not_partner_ts=mql_data_not_partner_ts.resample(freq).sum()
mql_data_not_partner_ts=mql_data_not_partner_ts.reset_index()
mql_data_not_partner_ts= get_derived_feature(mql_data_not_partner_ts,'converted_date')
mql_data_not_partner_ts=mql_data_not_partner_ts[mql_data_not_partner_ts['converted_date']>'2017-01-01']

mql_data_not_partner_ts['converted']=mql_data_not_partner_ts['converted'].cumsum()

mql_data_not_partner_ts.to_csv("../processed_data/conversion_rate/mql_data_conv_rate_not_partner_ts.csv",index=None,header=True)
mql_data_not_partner_ts_predict=make_future_df(mql_data_not_partner_ts,'converted_date','converted')
mql_data_not_partner_ts_predict.to_csv("../processed_data/conversion_rate/mql_data_conv_rate_not_partner_ts_predict.csv",index=None,header=True)
