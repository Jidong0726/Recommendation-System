import pandas as pd
import numpy as np
import mysql.connector


def preprocessing(wholetable,scale):
    topic_table = wholetable[['user','topic']]
    topic_table['counts'] = 1
    topic_table = topic_table.groupby(['user','topic'], as_index = False)['counts'].sum()
    topic_table['user_total'] = topic_table.groupby('user').counts.transform(np.max)
    topic_table['rating'] = topic_table['counts'] / topic_table['user_total'] * scale
    
    return topic_table,scale

def generate_data(project_num,user_num,topic_num):
    wholetable = pd.DataFrame(columns = ['user','topic'])
    n = project_num
    wholetable['user'] = np.random.randint(1,user_num,size = n)
    wholetable['topic'] = np.random.randint(1,topic_num,size = n)
    
    return wholetable

def load_data_from_db():
    cnx = mysql.connector.connect(user = 'grmds054_analyst', password = 'Cmethods1', 
                                  host = '146.66.69.53', database = 'grmds054_drup881')
    db_cursor = cnx.cursor()
    query = ('SELECT project_id,publisher_id,type,average_stars FROM dr_rmds_project ')
    db_cursor.execute(query)
    wholetable = pd.DataFrame(columns = ['project','user','topic','ave_star'])
    exist_table = {}
    project_and_topic = {}
    for (project_id,publisher_id,type,average_stars) in db_cursor:
        wholetable = wholetable.append({'project':project_id,'user':publisher_id,'topic':type,'ave_star':average_stars},ignore_index = True)
        try:
            exist_table[str(publisher_id)].append(str(project_id))
        except KeyError:
            exist_table[str(publisher_id)] = [str(project_id)]
        try:
            project_and_topic[str(type)].append(str(project_id))
        except KeyError:
            project_and_topic[str(type)] = [str(project_id)]
    
    return wholetable,exist_table,project_and_topic

def recom_project(project_and_topic, topic_table, rating_matrix, exist_table, topn):
    recom_project_table = pd.DataFrame(columns = ['user','project'])
    for user in rating_matrix.keys():
        topic_list = rating_matrix[user][:topn]
        for elem in topic_list:
            topic = elem[0]
            project_list = project_and_topic[topic]
            for item in project_list:
                if item not in exist_table[user]:
                    recom_project_table = recom_project_table.append({'user':int(user),'project':int(item)},ignore_index = True)
    
    return recom_project_table
                
        
        
        
    
        