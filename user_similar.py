import pandas as pd
from similarity.normalized_levenshtein import NormalizedLevenshtein
from sqlalchemy import create_engine
from loaddata import load_table_from_MySQL
from sklearn.metrics.pairwise import cosine_similarity


class user_similarity(object):
    def __init__(self):
        x = load_table_from_MySQL(username = 'grmds054_analyst', password = 'Cmethods1')
        wholetable, column_names = x.load_data('dr_rmds_users')
        self.feature_weight = {'company':0.2, 'university': 0.4, 'position': 0.1, 'interest': 0.3}
        self.user_sim = pd.DataFrame(columns = ['user','sim_user','sim_score'])
        self.interest_list = ['communication_services','consumer_discretionary','consumer_staples',
                         'energy','financials','health_care','industrials','information_technology','materials',
                         'real_estate','utilities']
        self.user_table = wholetable[['uid','company','university','job_title']+self.interest_list]
        
       
    
    def compare(self, uid1, uid2):
        info1 = self.user_table.loc[self.user_table['uid']==uid1,['company','university','position']].to_dict(orient = 'lists')
        info2 = self.user_table.loc[self.user_table['uid']==uid2,['company','university','position']].to_dict(orient = 'lists')
        normalized_levenshtein = NormalizedLevenshtein()
        interest1 = self.user_table.loc[self.user_table['uid']==uid1,self.interest_list].values
        interest2 = self.user_table.loc[self.user_table['uid']==uid2,self.interest_list].values
        sim = 0
        upper = 1-self.feature_weight['interest']
        for key in self.feature_weight.keys():
            if key=='interest':
                sim += self.feature_weight[key]*cosine_similarity(interest1,interest2)[0][0]                
            else:
                if type(info1[key][0]) is str and type(info2[key][0]) is str:
                    sim += self.feature_weight[key]*normalized_levenshtein.similarity(info1[key][0], info2[key][0])
                else:
                    upper = upper - self.feature_weight[key]
        return sim/upper
                    
    def compute_sim_matrix(self):
        user_list = list(self.user_table['uid'])
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
        
