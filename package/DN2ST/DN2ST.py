
from des_import_to_dwh_final.models.Pipeline import Pipeline
#@from lib.setup import setup

from des_import_to_dwh_final.package.DN2ST.pipelines.DN2ST_ItemLedger_pipeline import DN2ST_pipeline_ItemLedger


setup_ = {
    'masterpackage' : 'WMS_transaction',
    'site' : 'BOZ',
    'timestamp' : 'x',
    'filestamp' : 'x',
    'time_diff' : 120,
    'db' : 'wms',
    'company_name' : 'lineage',
    'encoding' : 'utf-8',
    'df_exec' : 'x',
    'on_rds' : 1
    }



pipeline = Pipeline(**setup_)



pipeline_ItemLedger = DN2ST_pipeline_ItemLedger(
    pipeline_name = 'DN2ST_PDocumentHeader', package = 'DN2ST', 
    bucket_copy = 'data-eu-s3-dwh.lineage.network', trgt_table = 'staging.posted_document_header', pipeline = pipeline
    ).run_pipeline()