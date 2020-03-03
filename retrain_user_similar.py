import pandas as pd
from similarity.normalized_levenshtein import NormalizedLevenshtein
from sqlalchemy import create_engine, MetaData, Table
from loaddata import load_table_from_MySQL
from sklearn.metrics.pairwise import cosine_similarity
import warnings
import time


class user_similarity(object):
    def __init__(self):
        self.feature_weight = {'company':0.2, 'university': 0.4, 'job_title': 0.1, 'interest': 0.3}
        self.user_sim = pd.DataFrame(columns = ['user','sim_user','sim_score'])
        self.interest_list = ['Fraud_Detection', 'Risk_Scoring', 'Healthcare', 'Internet_Search', 'Marketing_Effectiveness', 
                              'Website_Recommendations', 'Image_Recognition', 'Speech_Recognition', 'Airline_Route_Planning', 
                              'Price_Analytics', 'Supply_Chain_Optimization', 'Talent_Acquisition_Analytics', 'Environment_Analytics',                              
                              'Epidemiology', 'Social_Policy', 'Evaluation_and_Assessment']
        self.user_table = pd.DataFrame()
        
          
 
    def compare(self, uid1, uid2):
        info1 = self.user_table.loc[self.user_table['uid']==uid1][['company','university','job_title']].to_dict(orient = 'lists')
        info2 = self.user_table.loc[self.user_table['uid']==uid2][['company','university','job_title']].to_dict(orient = 'lists')
        normalized_levenshtein = NormalizedLevenshtein()
        interest1 = self.user_table.loc[self.user_table['uid']==uid1,self.interest_list].values
        interest2 = self.user_table.loc[self.user_table['uid']==uid2,self.interest_list].values
        sim = 0
        upper = 1-self.feature_weight['interest']
        for key in self.feature_weight.keys():
            if key=='interest': 
                if interest1[0][1]!= None and interest2[0][1]!= None:
                    sim += self.feature_weight[key]*cosine_similarity(interest1,interest2)[0][0]
                    upper += self.feature_weight[key]              
            else:
                if type(info1[key][0]) is str and type(info2[key][0]) is str:
                    sim += self.feature_weight[key]*normalized_levenshtein.similarity(info1[key][0], info2[key][0])
                else:
                    upper = upper - self.feature_weight[key]
                    
        if upper==0:
            return 0
        else:
            return sim/upper
                    
    def compute_sim_matrix(self):
        x = load_table_from_MySQL(username = 'grmds054_edison', password = 'Cmethods1G')
        wholetable, column_names = x.load_data('dr_rmds_users')
        self.user_table = wholetable[['uid','company','university','job_title']+self.interest_list]
        
        user_list = list(self.user_table['uid'])
        for user1 in user_list:
            lists = []
            for user2 in user_list:
                if user1 != user2:
                    sim = self.compare(user1,user2)
                    if sim != 0:
                        lists.append([user2, sim])
            lists.sort(key = lambda x:x[1], reverse = True)
            lists = lists[:7]
            for elem in lists:
                self.user_sim = self.user_sim.append({'user': user1, 'sim_user': elem[0], 'sim_score':elem[1]}, ignore_index = True)
        self.user_sim.user = self.user_sim.user.astype(int)
        self.user_sim.sim_user = self.user_sim.sim_user.astype(int)
        return self.user_sim
    
    def create_user_sim_table(self):
        engine = create_engine('mysql+mysqlconnector://grmds054_edison:Cmethods1G@198.20.83.186/grmds054_drup881',echo=True)
        self.user_sim.to_sql(name='dr_recom_simliar_users', con=engine, if_exists = 'replace', index=False)
        
    def update_user_sim_using_uid(self,uid):
        x = load_table_from_MySQL(username = 'grmds054_edison', password = 'Cmethods1G')
        wholetable, column_names = x.load_data('dr_rmds_users')
        self.user_table = wholetable[['uid','company','university','job_title']+self.interest_list]
        
        user_list = list(self.user_table['uid'])
        if uid not in user_list:
            return
        lists = []
        for user in user_list:
            if user!=uid:
                sim = self.compare(uid,user)
                if sim != 0:
                    lists.append([user,sim])
        lists.sort(key = lambda x:x[1], reverse = True)
        lists = lists[:7]
        if len(lists)!=0:
            engine = create_engine('mysql+mysqlconnector://grmds054_edison:Cmethods1G@198.20.83.186/grmds054_drup881',echo=False)
            metadata = MetaData()
            conn = engine.connect()
            dr_recom_simliar_users = Table('dr_recom_simliar_users', metadata, autoload=True, autoload_with=engine)
            conn.execute(dr_recom_simliar_users.delete().where(dr_recom_simliar_users.c.user == uid))
            for elem in lists:
                self.user_sim = self.user_sim.append({'user': uid, 'sim_user': elem[0], 'sim_score':elem[1]}, ignore_index = True)
            self.user_sim.user = self.user_sim.user.astype(int)
            self.user_sim.sim_user = self.user_sim.sim_user.astype(int)
            self.user_sim.to_sql(name='dr_recom_simliar_users', con=engine, if_exists = 'append', index=False)
        return
        
        
        
                    
                    
        
if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    x = user_similarity()
    x.update_user_sim_using_uid(488)
    #x.compute_sim_matrix()
    #x.create_user_sim_table()
    
