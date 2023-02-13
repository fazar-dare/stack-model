import pandas as pd
import datetime as dt
import pytz
# from tqdm import tqdm
from DynamoDB_analytics3.mf_dynamo import DynamoDB
import numpy as np

plants = pd.read_csv('full_plant.csv')



def fetch_physical(date_start, date_end):
    df_mel = pd.DataFrame()
    df_pn = pd.DataFrame()
    # df_boalf = pd.DataFrame()

    for date in pd.date_range(date_start, date_end):
        str_date = dt.datetime.strftime(date, '%Y%m%d')
        print(date)
        o_dyndb_mel = DynamoDB('bmrs_api',
                               partition_key_value=f'mel-{str_date}')  # , start_date_str='2021-01-01 13:00', n_hours=1)
        o_dyndb_pn = DynamoDB('bmrs_api', partition_key_value=f'pn-{str_date}')
        # o_dyndb_boalf = DynamoDB('bmrs_api', partition_key_value=f'isp_stack-{str_date}')
        df_mel_temp = o_dyndb_mel.pull_data()
        df_pn_temp = o_dyndb_pn.pull_data()
        # df_boalf_temp = o_dyndb_boalf.pull_data()
        df_mel_temp['tr_date'] = [date] * len(df_mel_temp)
        df_pn_temp['tr_date'] = [date] * len(df_pn_temp)
        # df_boalf_temp['tr_date'] = [date]*len(df_boalf_temp)
        df_mel = pd.concat([df_mel, df_mel_temp])
        df_pn = pd.concat([df_pn, df_pn_temp])
        # df_boalf = pd.concat([df_boalf, df_boalf_temp])
    return df_mel, df_pn

def twap_base(df):
    df['mwt'] = df['mw_mid'] * df['duration']
    df = df[['fuel_type', 'bmu', 'tr_date', 'sp', 'duration', 'mwt']]
    df = df.groupby(['fuel_type', 'bmu', 'tr_date', 'sp']).sum()
    df['mw'] = df['mwt'] / df['duration']
    df = df.reset_index()
    df = df.drop(columns=['mwt'])
    return df

def twap_combine(df):
    df['mw'] = df['mw_mid'] * df['duration']/30
    df = df[['fuel_type', 'tr_date', 'sp', 'mw']]
    df = df.groupby(['fuel_type', 'tr_date', 'sp']).sum()
    df = df.reset_index()
    df['duration'] = [30]*len(df)
    return df

def combine_plants(df):
    df_combo = df.loc[df.fuel_type.isin(['Gas Recip', 'Battery', 'Hydro'])]
    df_base = df.loc[~df.fuel_type.isin(['Gas Recip', 'Battery', 'Hydro'])]
    df_base = twap_base(df_base)
    df_combo = twap_combine(df_combo)
    df = pd.concat([df_base, df_combo])
    return df

def twap(df):
    df = df.merge(plants[['plant_id', 'fuel_type']], how='inner', left_on='bmu', right_on='plant_id')
    df = df.loc[~df.fuel_type.isin((['Wind', 'Supply']))]
    df['ts_s'] = pd.to_datetime(df['ts_s'])
    df['ts_e'] = pd.to_datetime(df['ts_e'])
    df['duration'] = (df['ts_e'] - df['ts_s']).dt.total_seconds() / 60.0
    df['mw_mid'] = (df['mw_s'] + df['mw_e'])/2
    df['mw_mid'] = df['mw_mid'].astype('float')
    df_positive = df.loc[df.mw_mid >= 0]
    df_negative = df.loc[df.mw_mid < 0]
    df_positive = combine_plants(df_positive)
    df_negative = combine_plants(df_negative)
    df_out = pd.concat([df_positive, df_negative])
    df_out = df_out.sort_values(['tr_date', 'sp'])
    return df_out


def get_lf(df_mel, df_pn):
    df_mel = twap(df_mel)
    df_pn = twap(df_pn)
    df_physical = df_pn.merge(df_mel, how='outer', on=['fuel_type', 'bmu', 'tr_date', 'sp'], suffixes=['_pn', '_mel'])
    df_physical.loc[df_physical.mw_mel.isnull(), ['duration_mel', 'mw_mel']] = [30, 0]
    df_physical.loc[df_physical.mw_pn.isnull(), ['duration_pn', 'mw_pn']] = [30, 0]
    df_physical = df_physical.copy()
    df_physical = df_physical.loc[df_physical.duration_mel == 30]
    # Should I do this?
    df_physical['mw_pn'] = df_physical['mw_pn'] * df_physical['duration_pn']/30
    #Set mw_mel to be the ceiling to mw_pn (i.e. pn cannot be higher than the mel)
    df_physical['mw_pn'] = np.where(df_physical['mw_pn'] <= df_physical['mw_mel'], df_physical['mw_pn'], df_physical['mw_mel'])
    #Set mw_mel to be the ceiling when importing as well
    df_physical['mw_pn'] = np.where(-df_physical['mw_pn'] <= df_physical['mw_mel'], df_physical['mw_pn'], -df_physical['mw_mel'])
    #Ignore cases when the plant is out (mw_mel = 0)
    df_physical = df_physical.loc[df_physical.mw_mel != 0]
    df_import = df_physical.loc[df_physical.mw_pn < 0]
    df_export = df_physical.loc[df_physical.mw_pn >= 0]
    df_import['lf'] = -df_import['mw_pn']/df_import['mw_mel']
    df_export['lf'] = df_export['mw_pn']/df_export['mw_mel']
    df_lf_export = df_export[['fuel_type', 'bmu', 'lf']].groupby(['fuel_type', 'bmu']).mean()
    df_lf_export = df_lf_export.reset_index()
    return df_lf_export

# def explode_df(df):
#     df['ts_s'] = pd.to_datetime(df['ts_s'])
#     df['ts_e'] = pd.to_datetime(df['ts_e'])
#     df['mw_mid'] = (df['mw_s'] + df['mw_e']) / 2
#
#     df = df.sort_values(['ts_s'])
#     df['unique_sp'] = df.groupby(['bmu', 'tr_date', 'sp']).cumcount() + 1
#     intervals = df.set_index(['bmu', 'tr_date', 'sp', 'unique_sp']).apply(
#         lambda row: pd.date_range(row['ts_s'].replace(microsecond=0, second=0, minute=0),
#                                   row["ts_e"], freq="1min", inclusive='both').values, axis=1
#     ).explode()
#     intervals = intervals.reset_index().rename(columns={0: 'ts_gb'})
#     df = df.merge(intervals, how='inner', on=['bmu', 'tr_date', 'sp', 'unique_sp'])
#     df = df.rename(columns={'mw_mid': 'mw'})
#     df['ts_gb'] = df['ts_gb'].dt.tz_localize(pytz.timezone('Europe/London'))
#     return df[['ts_gb', 'bmu', 'tr_date', 'sp', 'mw']]
#
# df_mel2 = explode_df(df_mel)
# df_pn2 = explode_df(df_pn)
# df_physical = df_pn2.merge(df_mel2, how = 'outer', on = ['ts_gb', 'bmu', 'tr_date', 'sp'], suffixes=['_pn', '_mel'])
# df_physical2 = df_physical.set_index('ts_gb')
# df_physical2 = df_physical2.groupby(['bmu', 'tr_date', 'sp']).resample('1T').sum()
# print()

if __name__ == "__main__":
    date_start = dt.date(2022, 1, 1)
    date_end = dt.date(2023, 2, 12)
    df_mel, df_pn = fetch_physical(date_start, date_end)
    df_lf = get_lf(df_mel, df_pn)
    df_lf.to_csv('plant_lf.csv')