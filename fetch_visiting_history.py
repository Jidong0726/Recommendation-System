# -*- coding: utf-8 -*-
"""
Created on Tue Jan 14 10:36:54 2020

@author: jidon
"""
import mysql.connector
import pandas as pd


def fetch(uid,string):
    cnx = mysql.connector.connect(user = 'grmds054_edison', password = 'Cmethods1G', 
                                      host = '198.20.83.186', database = 'grmds054_drup881')
    db_cursor = cnx.cursor()
    query = 'select visitors_uid, visitors_date_time, visitors_referer, visitors_path FROM dr_visitors where visitors_uid='+ str(uid) +' and visitors_path like \''+string+ '%\' order by visitors_date_time desc'
    print (query)
    if uid!=0:
        db_cursor.execute(query)
    else:
        db_cursor.execute(query+' limit 500')
    column_names = ['uid','time','referer','path']
    wholetable = pd.DataFrame(columns = column_names)
    for tuples in db_cursor:
        dic = dict(zip(column_names, list(tuples)))
        wholetable = wholetable.append(dic,ignore_index = True)
    
    cnx.close
        
    return wholetable


x = fetch(488,'/product/')
print (x)