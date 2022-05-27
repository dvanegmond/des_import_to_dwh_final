import pyodbc
import os
import mysql
from mysql.connector import MySQLConnection, Error


'''
Connection to operational databases
'''
#connexion to ado (Cooling tags)
def conn_ado():
    conn = mysql.connector.connect(
      host="10.31.2.109",
      user="LineageData",
      password=str(os.environ['ado_pwd']),
      database="Cofely")
    cursor = conn.cursor()
    return cursor,conn

def conn(encoding):
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=rds-nav-prod.lineage.services;'
                          'Database=Dynamics_Xternal;'
                          'Trusted_Connection=yes;'
                          )
    cursor = conn.cursor()
    conn.setencoding(encoding)
    return cursor,conn

# connexion to other NAV db
def conn_non_rds(host,db,encoding):
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server='+str(host)+';'
                          'Database='+str(db)+';'
                          'Trusted_Connection=no;'
                          'UID='+ str(os.environ['non_rds_user']) +';'
                          'PWD=' + str(os.environ['non_rds_pwd']) + ';'
                          )
    cursor = conn.cursor()
    conn.setencoding(encoding)
    return cursor,conn


# connexion to WCS
def conn_wcs(host,db,encoding):
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=' + str(host) + ';'
                      'Database=' + str(db) + ';'
                      'Trusted_Connection=no;'
                      'UID='+ str(os.environ['wcs_user']) +';'
                      'PWD=' + str(os.environ['wcs_pwd']) + ';'
                      )
    cursor = conn.cursor()
    conn.setencoding(encoding)
    return cursor,conn
