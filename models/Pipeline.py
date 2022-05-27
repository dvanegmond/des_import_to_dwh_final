import os
import sys

import pandas as pd

from datetime import datetime

# from lib import redshift
# from lib import ope
# from lib import s3


class Pipeline:

    def __init__(self, masterpackage, site, timestamp, filestamp, time_diff, db, company_name, encoding, df_exec, on_rds) -> None:
        self.masterpackage = masterpackage
        self.site = site
        self.timestamp = timestamp
        self.filestamp = filestamp
        self.time_diff = time_diff
        self.db = db
        self.company_name = company_name
        self.encoding = encoding
        self.df_exec = df_exec
        self.on_rds = on_rds


    def can_execute(self, pipeline):
        return self.df_exec.loc[self.df_exec['package'] == pipeline, 'enabled'].iloc[0]


    def connect_rds(self):
        try:
            #connection to DWH
            self.conn_dwh, self.cur_dwh = redshift.connection_dwh(os.environ['dwh_user'],os.environ['dwh_pwd'])

        except Exception as e:
            self.log_exec(e, 'No connection to DWH')
            sys.exit(1)


    def connect_nav(self):
        try:
            #connection to NAV
            if self.on_rds == 1:
                self.cursor, self.conn = ope.conn(self.encoding) #connection to RDS
            else:
                self.cursor, self.conn = ope.conn_non_rds(self.host, self.db, self.encoding) #connection to RDS

        except Exception as e:
            self.log_exec(e, 'No connection to NAV')
            sys.exit(1)

    
    def log_exec(self, e, message):# this method needs to be overridden in order to be used
        redshift.log_execution(self.site, self.masterpackage, self.pipeline_name, self.package, e, message, datetime.now())


    def get_max_ts(self, trgt_table):
        '''
        Getting the max timestamps
        '''
        try:
            cmd = "SELECT COALESCE(MAX([ts_boltrics]),0) AS ""marker"" FROM {0} WHERE site = '{1}'".format(trgt_table, self.site)
            max_ts = redshift.marker(self.cur_dwh, cmd)

        except Exception as e:
            self.log_exec(e, 'Max retrieval failed')

        return max_ts


    def get_dataframe(self, query, max_ts):
        '''
        Getting the dataframe
        '''
        # Query the database
        try:
            sql = query(datetime.now(), self.db, self.company_name, self.site, max_ts)
            self.cursor.execute(sql)

        except Exception as e: # log failure of the query to db
            self.log_exec(str(e), 'Query failed')
            sys.exit(1)

        #forming the dataframe
        try:
            df = pd.DataFrame.from_records(self.cursor.fetchall(),columns = [desc[0] for desc in self.cursor.description])
            # stopping the script if no data
            if len(df) == 0:
                return

        except Exception as e: # log failure of the query to db
            self.log_exec(e, 'Dataframe writing failed')
            sys.exit(1)

        return df

    
    def add_ref_column(self, df, trgt_table):
        try:
            '''Adding the reference column'''
            lkp_copy_cmd = "SELECT COALESCE(MAX(reference),0) AS ""marker"" FROM {0}".format(trgt_table)
            df = Pipeline.wrap4copy(df, lkp_copy_cmd, self.cur_dwh)

        except Exception as e: ## If connection fails, log to the error to log_db and send email
            self.log_exec(e, 'Split failed')
            sys.exit(1)

            ##TODO: add closing mechanism


    def export_s3(self, df):
        '''
        Export to s3_dwh
        '''
        try:
            file_name = self.masterpackage + '/' + self.site + '/' + self.filestamp + '/' + self.ipeline + '.csv'
            s3.put_df2csv(df, file_name, self.bucket_copy)

        except Exception as e: ## If connection fails, log to the error to log_db and send email
            self.log_exec(e, 'File export 2 s3_dwh failed')
            sys.exit(1)

            ##TODO: add closing mechanism


    def copy_rds(self):
        '''
        COPY to Redshift
        '''
        try:
            filekey = "s3://"+str(self.bucket_copy)+"/" + self.file_name
            copy_cmd = "COPY " +str(self.trgt_table)+  " FROM '"+str(filekey)+"' IGNOREHEADER 1 delimiter '|' NULL AS '' CSV credentials 'aws_iam_role=arn:aws:iam::384726956211:role/dwh_redshift_s3_role'"
            self.cur_dwh.execute(copy_cmd)

        except Exception as e: ## If connection fails, log to the error to log_db and send email
            self.log_exec(e, 'Copy to Redshift failed')
            sys.exit(1)

            ##TODO: add closing mechanism


    def close_all_connections(self):
        self.conn.close()
        redshift.close_commit(self.conn_dwh)


    @staticmethod
    def wrap4copy(df, lkp_copy_cmd, cur_dwh):
        '''
        Prepartion of the df_copy and df_insert dataframes
        '''
        # preparing the dataframe to append
        df_ref = redshift.read(cur_dwh,lkp_copy_cmd)
        max_ref = df_ref['marker'][0]
        df = df.reset_index()
        df.insert(0,'reference',df.index + max_ref+1) #creating the reference column
        #df = df[headers] # right order of the columns
        df.drop('index',axis=1,inplace =True)
        return df

    
    @staticmethod
    def lookup2checkdup(df, join, cols):
        '''
        Lookups check for duplicate between arriving flow and trgt table
        '''
        # we inner merge the data arrival and the existing table, if len(df)>0 there are duplicates
        df = (
                df.merge(join,
                        how='left',
                        on = cols,
                        indicator=True)
                .query('_merge == "both"')
                .drop(columns = '_merge')
            )
        return df
    

    # @staticmethod
    # def lookup2checkdup_inner(df,cols):
    #     '''
    #     Lookups check for duplicates within arriving flow
    #     '''
    #     df_dupp = df[df.duplicated(cols)]
    #     if len(df_dupp) > 0:
    #         print("Duplicate Rows based on  column are:", df_dupp, sep='\n')
    #         redshift.log_execution(site,masterpackage,pipeline,step,e,'Duplicates found',timestamp)
    #         print(e)
    #         sys.exit(1)
    #     return