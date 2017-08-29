import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import datetime


def query(con):
    sql='''

        select
          u.user_id ,
          date (u.join_date) join_date,
          u.loyal_date,
          u.first_transaction_date,
          u.`1st_bidpack_date` ,
          u.`2nd_bidpack_date` ,
          u.ltv,
          uag.device_type ,
          uag.os,
          uag.user_agent regexp ('DealDashAndroidApp') android_app

        from dw.user u
        join defaultdb.user_agent uag on u.join_user_agent_id=uag.id
        where uag.os='Android' and u.join_date > '2016-10-01' and uag.device_type!='camera' and uag.device_type!='smart display'

        '''
    return pd.read_sql(sql,con)


def shape_df(dfm):
    df = dfm.copy()

    def add_features(df):

        df['month'] = df.join_date.apply(lambda x: x.replace(day=1))

        def func(row):
            if row['device_type'] == 'mobile' and row['android_app'] == 0:
                return 'mobile_web'
            elif row['device_type'] == 'mobile' and row['android_app'] == 1:
                return 'mobile_app'
            elif row['device_type'] == 'tablet' and row['android_app'] == 0:
                return 'tablet_web'
            elif row['device_type'] == 'tablet' and row['android_app'] == 1:
                return 'tablet_app'
            elif row['device_type'] == 'unkown' and row['android_app'] == 0:
                return 'unknown_web'
            elif row['device_type'] == 'unkown' and row['android_app'] == 1:
                return 'unknown_app'

        df['user_agent'] = df.apply(func, axis=1)

        return df

    def pivot_df(df):

        ac = df.groupby(['month', 'user_agent'])['join_date'].agg('count').reset_index().rename(
            columns={'join_date': 'accounts_created'})
        pivoted_ac = ac.pivot(index='month', columns='user_agent', values='accounts_created')
        pivoted_ac = pivoted_ac.fillna(0)

        return pivoted_ac

    df = add_features(df)
    df = pivot_df(df)

    return df

def imp(con ,output_path) :
    dir_path = os.path.abspath(os.path.join(output_path, os.path.pardir))
    os.makedirs(dir_path, exist_ok=True)
    print(dir_path)
    df=query(con)
    shaped_df=shape_df(df)
    shaped_df.to_csv(output_path ,mode='a')
    print('why it can not creat branch 4 {} '.format(output_path))

if __name__ == '__main__':
    from EE.engine_from_json_file import engine_from_json_file
    engine= engine_from_json_file('reportingdb.json')
    datestring = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    imp(engine , 'Android_'+datestring+'.csv')