import redshift
import pandas as pd

'''
Prepartion of the df_copy and df_insert dataframes
'''
# preparing the dataframe to append
def wrap4copy(df,site,timestamp,lkp_copy_cmd,cur_dwh):
    df_ref = redshift.read(cur_dwh,lkp_copy_cmd)
    max_ref = df_ref['marker'][0]
    df = df.reset_index()
    df.insert(0,'reference',df.index + max_ref+1) #creating the reference column
    #df = df[headers] # right order of the columns
    df.drop('index',axis=1,inplace =True)
    return df

'''
Lookups check for duplicate between arriving flow and trgt table
'''
# we inner merge the data arrival and the existing table, if len(df)>0 there are duplicates
def lookup2checkdup(df,join,cols):
    df = (
            df.merge(join,
                      how='left',
                      on = cols,
                      indicator=True)
            .query('_merge == "both"')
            .drop(columns = '_merge')
        )
    return df
# '''
# Lookups check for duplicates within arriving flow
# '''
# def lookup2checkdup_inner(df,cols):
#     df_dupp = df[df.duplicated(cols)]
#     if len(df_dupp) > 0:
#         print("Duplicate Rows based on  column are:", df_dupp, sep='\n')
#         redshift.log_execution(site,masterpackage,pipeline,step,e,'Duplicates found',timestamp)
#         print(e)
#         sys.exit(1)
#     return
