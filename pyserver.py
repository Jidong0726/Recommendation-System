from flask import Flask
app = Flask(__name__)

@app.route('/update_recommendation/<int:uid>')
def update_recommendation(uid):
    return str(uid*5)

@app.route('/update_user_sim/<int:uid>')
def update_user_sim(uid):
    return str(uid/5)
    
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
