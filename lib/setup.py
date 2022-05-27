import datetime
import pytz
import sys
import os

sys.path.append("..\\lib") # home made library folder
import ope

import pandas as pd


def setup():
    cursor, conn = ope.conn("utf-8") # connecting to the db

    '''
    Security check site
    '''
    site = os.environ['site']
    sql = """ SELECT site_no FROM [Dynamics_Xternal].[dbo].[DataEcoSystem_SiteSetup]""" # query to retrieve site list
    cursor.execute(sql) # executing the query
    df_check = pd.DataFrame.from_records(cursor.fetchall(),columns = [desc[0] for desc in cursor.description]) # retrieving the dataframe
    df_check = df_check[df_check['site_no'].isin([site])]
    #exists = site in df_check.site_no
    print(site)
    print(df_check)
    if len(df_check) != 1:
        print('Site not recognized')
        sys.exit(1)

    '''
    Manifest
    '''
    # querying Data Eco system setup table
    sql = """SELECT [site_no]
                ,[tz_site]
                ,[host_wms]
                ,[db_wms]
                ,[host_wms_Xternal]
                ,[db_wms_Xternal]
                ,[tz_wms]
                ,[company_name]
                ,reference
                ,encoding
                ,on_rds
            FROM [Dynamics_Xternal].[dbo].[DataEcoSystem_SiteSetup]
            WHERE site_no = ? """
    cursor.execute(sql,site) # executing the query
    df = pd.DataFrame.from_records(cursor.fetchall(),columns = [desc[0] for desc in cursor.description]) # retrieving the dataframe
    print(df)
    # filestamp and timestamp of the pipeline
    tz = pytz.timezone(df['tz_wms'][0])
    timestamp = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    filestamp = datetime.datetime.now(tz).strftime('%Y-%m-%d_%H:%M:%S')

    # timestamp of the site
    tz = pytz.timezone(df['tz_site'][0])
    timestamp_site = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    # time difference in minute
    diff = datetime.datetime.strptime(timestamp_site, '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    diff_in_s = diff.total_seconds()
    diff_site_nav = divmod(diff_in_s,60)[0]

    # querying the execution table
    sql = """SELECT package  [package], [{0}] [enabled] FROM [Dynamics_Xternal].[dbo].[DataEcoSystem_Packages] """.format(site)
    cursor, conn = ope.conn("utf-8") # connecting to the db
    cursor.execute(sql) # executing the query
    df_exec = pd.DataFrame.from_records(cursor.fetchall(),columns = [desc[0] for desc in cursor.description])

    return {
        'masterpackage' : 'WMS_transaction',
        'site' : site,
        'timestamp' : timestamp,
        'filestamp' : filestamp,
        'time_diff' : diff_site_nav,
        'db' : df['db_wms'],
        'company_name' : df['company_name'],
        'encoding' : df['encoding'],
        'df_exec' : df_exec,
        'on_rds' : df['on_rds'] 
    }
