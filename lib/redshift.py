import psycopg2
import pandas as pd
import os

'''
Connection, closing and commit
'''
# connection
def connection_dwh(db_user,db_password):
    conn = psycopg2.connect(
        dbname= 'dwh',
        user= db_user,
        password= db_password,
        port=5439,
        host="redshift-cluster-dwh.civ2myfs8pw6.eu-central-1.redshift.amazonaws.com")
    cur = conn.cursor()
    return conn,cur

def close_commit(conn):
    conn.commit()
    conn.close()
    return
'''
Retrieve data
'''
# retrieve a dataframe with parameter
def read(cur,cmd):
    cur.execute(cmd)
    df = pd.DataFrame.from_records(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    return df

# retrieve a marker (max timestamp for exammple "SELECT COALESCE(MAX(reference),0) AS ""ref"" FROM dbo.item")
def marker(cur,cmd,param):
    #retrieving the max
    cur.execute(cmd,param)
    df = pd.DataFrame.from_records(cur.fetchall())
    df.columns = [column[0] for column in cur.description]
    ref = df['marker'][0]
    #removing the miliseconds when timestamp
    ref = str(ref)
    ref = ref.split('.')[0]
    return ref

def marker_ref(cmd,conn):
    df = pd.read_sql(cmd,conn)
    ref = df['marker'][0]
    return ref


'''
Write data
'''
#log package execution
def log_execution(site,masterpackage,pipeline,step,e,message,timestamp):
    statement = '''INSERT INTO admin.etl_execution_status(site,masterpackage,pipeline,step,error,message,timestamp)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)'''
    #conn, cur = connection_dwh(os.environ['dwh_user'],os.environ['dwh_pwd'])
    #cur.execute(statement,(site,masterpackage,pipeline,step,e,message,timestamp))
    #conn.commit()
    #conn.close()
    return
