import pandas as pd
import mysql.connector
from similarity.normalized_levenshtein import NormalizedLevenshtein
from sqlalchemy import create_engine


class user_similarity(object):
    def __init__(self):
        cnx = mysql.connector.connect(user = 'grmds054_analyst', password = 'Cmethods1', 
                                  host = '146.66.69.53', database = 'grmds054_drup881')
        db_cursor = cnx.cursor()
        self.cursor = db_cursor
        company_table = self.load_data(query = ('SELECT entity_id, field_company_value FROM dr_user__field_company'), column_name = ['user_id','company'])
        univer_table = self.load_data(query = ('SELECT entity_id,field_university_value FROM dr_user__field_university'), column_name = ['user_id','university'])
        position_table = self.load_data(query = ('SELECT entity_id,field_position_value FROM dr_user__field_position;'), column_name = ['user_id','position'])
        self.user_table = company_table.merge(univer_table, how = 'outer', on = 'user_id')
        self.user_table = self.user_table.merge(position_table, how = 'outer', on = 'user_id')
        self.feature_weight = {'company':0.3, 'university': 0.5, 'position': 0.2}
        self.user_sim = pd.DataFrame(columns = ['user','sim_user','sim_score'])
       
    def load_data(self, query, column_name):
        self.cursor.execute(query)
        wholetable = pd.DataFrame(columns = column_name)
        for tuples in self.cursor:
            dic = {}
            for i in range(len(column_name)):
                dic[column_name[i]] = tuples[i]
            wholetable = wholetable.append(dic,ignore_index = True)
        return wholetable
    
    def compare(self, uid1, uid2):
        info1 = self.user_table.loc[self.user_table['user_id']==uid1,['company','university','position']].to_dict(orient = 'lists')
        info2 = self.user_table.loc[self.user_table['user_id']==uid2,['company','university','position']].to_dict(orient = 'lists')
        normalized_levenshtein = NormalizedLevenshtein()
        dist = 0
        upper = 1
        for key in self.feature_weight.keys():
            if type(info1[key][0]) is str and type(info2[key][0]) is str:
                dist += self.feature_weight[key]*normalized_levenshtein.similarity(info1[key][0], info2[key][0])
            else:
                upper = upper - self.feature_weight[key]
        return dist/upper
                    
    def compute_sim_matrix(self):
        user_list = list(self.user_table['user_id'])
        for user1 in user_list:
            lists = []
            for user2 in user_list:
                if user1 != user2:
                    lists.append([user2, self.compare(user1,user2)])
            lists.sort(key = lambda x:x[1], reverse = True)
            lists = lists[:7]
            for elem in lists:
                self.user_sim = self.user_sim.append({'user': user1, 'sim_user': elem[0], 'sim_score':elem[1]}, ignore_index = True)
        self.user_sim.user = self.user_sim.user.astype(int)
        self.user_sim.sim_user = self.user_sim.sim_user.astype(int)
        return self.user_sim
    
    def update_user_sim_table(self):
        engine = create_engine('mysql+mysqlconnector://grmds054_edison:Cmethods1@146.66.69.53/grmds054_drup881',echo=True)
        self.user_sim.to_sql(name='dr_recom_simliar_users', con=engine, if_exists = 'replace', index=True)
                    
                    
        
if __name__ == "__main__":
    x = user_similarity()
    x.compute_sim_matrix()
    x.update_user_sim_table()
        
