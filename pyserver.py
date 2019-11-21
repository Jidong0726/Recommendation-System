from flask import Flask
from recom_dataverse_to_user import recom_dataverse
from retrain_user_similar import user_similarity
import warnings



app = Flask(__name__)

@app.route('/update_recommendation/<int:uid>')
def update_recommendation(uid):
    x = recom_dataverse()
    x.compute_one_user(uid)
    return str('recommendation update successfully')

@app.route('/update_user_sim/<int:uid>')
def update_user_sim(uid):
    warnings.filterwarnings("ignore")
    x = user_similarity()
    x.update_user_sim_using_uid(uid)
    return str('user similarity update successfully')
    
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
