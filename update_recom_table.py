from recom_model import recommendation_system
from utility import load_data_from_db
from utility import preprocessing
from utility import recom_project
from sqlalchemy import create_engine

whole_table, exist_table, project_and_topic = load_data_from_db()
topic_table, scale = preprocessing(whole_table,5)
model = recommendation_system('SVD',topic_table,scale)
model.train(cross_valid = False, k = 0)
result = model.estimate_rating_matrix()
recom_project_table = recom_project(project_and_topic, topic_table, result, exist_table, 4)


engine = create_engine('mysql+mysqlconnector://grmds054_edison:Cmethods1@146.66.69.53/grmds054_drup881',echo=True)
recom_project_table.to_sql(name='dr_recom_project_to_user', con=engine, if_exists = 'replace', index=True)