from models.Pipeline import Pipeline
from package.DN2ST.queries.DN2ST_ItemLedgerEntry_query import query

import inspect

class DN2ST_pipeline_ItemLedger(Pipeline):


    def __init__(self, pipeline_name, package, bucket_copy, trgt_table, pipeline):

        self.pipeline_name = pipeline_name
        self.package = package
        self.bucket_copy = bucket_copy
        self.trgt_table = trgt_table

        super().__init__(
            pipeline.masterpackage,
            pipeline.site,
            pipeline.timestamp,
            pipeline.filestamp,
            pipeline.time_diff,
            pipeline.db,
            pipeline.company_name,
            pipeline.encoding,
            pipeline.df_exec,
            pipeline.on_rds
            )

    
    def log_exec(self, *args, **kwargs):
        super(DN2ST_pipeline_ItemLedger, self).log_exec(self.pipeline_name, self.package, *args, **kwargs)

    
    def run_pipeline(self):

        print(self.pipeline_name)

        # if self.can_execute():

        #     self.connect_rds()     
        #     self.connect_nav()

        #     max_ts = self.get_max_ts(self.trgt_table)

        #     dataframe = self.get_dataframe(self.query, max_ts)
        #     dataframe['ts_boltrics'] =dataframe['ts_boltrics'].astype('Int64')

        #     dataframe = self.add_ref_column(dataframe, self.trgt_table)

        #     self.export_s3(dataframe)

        #     self.copy_rds()

        #     self.close_all_connections()
            
        #     return True

        # return False

