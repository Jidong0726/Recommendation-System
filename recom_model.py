import pandas as pd
from surprise import Dataset
from surprise import SVD
from surprise.model_selection import KFold
from surprise import accuracy
from surprise import KNNBasic
from utility import preprocessing
from utility import generate_data
from utility import load_data_from_db
from surprise import Reader

class recommendation_system(object):
    def __init__(self, types, topic_table, rating_scale):
        if types=='SVD':
            self.model = SVD()
        elif types=='KNN':
            self.model = KNNBasic()
        self.topic_table = topic_table
        self.rat_scale = rating_scale
        
    def train(self, cross_valid = False, k = 0):
        reader = Reader(rating_scale = (0,self.rat_scale))
        recom_data = Dataset.load_from_df(self.topic_table[['user', 'topic', 'rating']], reader)
        if not cross_valid:
            train_set = recom_data.build_full_trainset()
            self.model.fit(train_set)
            test_set = train_set.build_testset()
            predictions = self.model.test(test_set)
            accuracy.rmse(predictions, verbose=True)
        else:
            kf = KFold(n_splits=k)
            error = 999
            for trainset, testset in kf.split(recom_data):
                algo = self.model
                algo.fit(trainset)
                predictions = algo.test(testset)
                temp = accuracy.rmse(predictions, verbose=True)
                if temp<error:
                    error = temp
                    self.model = algo
    
    def estimate_rating_matrix(self):
        rating_matrix = {}
        for user in pd.unique(self.topic_table['user']):
            rating_matrix[str(user)] = []
            for item in pd.unique(self.topic_table['topic']):
                rating_matrix[str(user)].append([str(item),self.model.predict(user,item).est])
            rating_matrix[str(user)].sort(key = lambda x:x[1], reverse = True)
        return rating_matrix



if __name__ == "__main__":
    topic_table,scale = preprocessing(generate_data(1000,100,20),5)
    model = recommendation_system('SVD',topic_table,scale)
    model.train(cross_valid = True, k = 10)
    result = model.estimate_rating_matrix()
    result