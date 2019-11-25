# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 14:56:10 2019

@author: jidong
"""
from loaddata import load_table_from_MySQL
from query_data_from_dataverse import query_dataset_information
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table


class recom_dataverse(object):
    def __init__(self):
        self.interest_list = ['communication_services','consumer_discretionary','consumer_staples',
                         'energy','financials','health_care','industrials','information_technology','materials',
                         'real_estate','utilities']
        self.user_table = pd.DataFrame()
        self.regular_key_word = ['machine%20learning','data%20science','statistics']
        self.recom_result = pd.DataFrame(columns = ['uid','name','url','doi_id', 'description','publish_date', 'author' ,'score'])
        
    def retrain_whole(self):
        x = load_table_from_MySQL(username = 'grmds054_edison', password = 'Cmethods1G')
        wholetable, column_names = x.load_data('dr_rmds_users')
        self.user_table = wholetable[['uid']+self.interest_list]
        user_list = list(self.user_table['uid'])
        
        default_uid = 1
        for keyword in self.regular_key_word:
            tabel = query_dataset_information(keyword,0,10)
            tabel.insert(loc=0, column='uid', value=default_uid)
            self.recom_result = pd.concat([self.recom_result,tabel],ignore_index=True)
        
        for user in user_list:
            key_word_list = []
            person_info = self.user_table.loc[self.user_table['uid']==user, self.interest_list].to_dict(orient = 'lists')
            for key in self.interest_list:
                if int(person_info[key][0])==1:
                    if "_" in key:
                        conkey = key
                        conkey = conkey.replace("_", "%20")
                        key_word_list.append(conkey)
                    else:
                        key_word_list.append(key)
            if len(key_word_list)!=0:
                for keyword in key_word_list:
                    table = query_dataset_information(keyword,0,10)
                    table.insert(loc=0, column='uid', value=user)
                    self.recom_result = pd.concat([self.recom_result,table],ignore_index=True)
        
        for (idx, row) in self.recom_result.iterrows():
            if row.author!=None:
                strs = ''
                for elem in row.author:
                    strs = strs+elem+' | '
                self.recom_result.loc[idx, 'author'] = strs
        return
    
    def create_dataset_rs_table(self):
        engine = create_engine('mysql+mysqlconnector://grmds054_edison:Cmethods1G@198.20.83.186/grmds054_drup881',echo=True)
        self.recom_result.uid = self.recom_result.uid.astype(int)
        self.recom_result.to_sql(name='dr_recom_dataverse_to_users', con=engine, if_exists = 'replace', index=False)
            
                
    def compute_one_user(self,uid):
        x = load_table_from_MySQL(username = 'grmds054_edison', password = 'Cmethods1G')
        wholetable, column_names = x.load_data('dr_rmds_users')
        self.user_table = wholetable[['uid']+self.interest_list]
        person_info = self.user_table.loc[self.user_table['uid']==uid, self.interest_list].to_dict(orient = 'lists')
        if len(person_info)==0:
            return
        key_word_list = []
        for key in self.interest_list:
                if int(person_info[key][0])==1:
                    if "_" in key:
                        conkey = key
                        conkey = conkey.replace("_", "%20")
                        key_word_list.append(conkey)
                    else:
                        key_word_list.append(key)
        if len(key_word_list)!=0:
            for keyword in key_word_list:
                table = query_dataset_information(keyword,0,10)
                table.insert(loc=0, column='uid', value=uid)
                self.recom_result = pd.concat([self.recom_result,table],ignore_index=True)
        
            for (idx, row) in self.recom_result.iterrows():
                if row.author!=None:
                    strs = ''
                    for elem in row.author:
                        strs = strs+elem+' | '
                    self.recom_result.loc[idx, 'author'] = strs
            
            engine = create_engine('mysql+mysqlconnector://grmds054_edison:Cmethods1G@198.20.83.186/grmds054_drup881',echo=True)
            metadata = MetaData()
            conn = engine.connect()
            dr_recom_dataverse_to_users = Table('dr_recom_dataverse_to_users', metadata, autoload=True, autoload_with=engine)
            conn.execute(dr_recom_dataverse_to_users.delete().where(dr_recom_dataverse_to_users.c.uid == uid))
            self.recom_result.uid = self.recom_result.uid.astype(int)
            self.recom_result.to_sql(name='dr_recom_dataverse_to_users', con=engine, if_exists = 'append', index=False)   
        return

if __name__ == "__main__":
    x = recom_dataverse()
    #x.retrain_whole()
    #x.create_dataset_rs_table()
    x.compute_one_user(246)
