# coding=utf-8
from datetime import datetime
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "mysql+pymysql://root:root6114EveryAi!root6114EveryAi@192.144.143.127:3306/zgc_analysis?charset=utf8"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"]=True
app.config["SQLALCHEMY_ECHO"]=True

db = SQLAlchemy(app)

class User(db.Model):
    __tablename__ = "user"

    id = db.Column(db.Integer, primary_key=True, unique=True)
    username = db.Column(db.String(50))
    password = db.Column(db.String(16))
    creattime = db.Column(db.DateTime, default=datetime.now)

    def __init__(self, userid,username, password, creattime=datetime.now()):
        self.userid = userid
        self.username = username
        self.password = password
        self.creattime = creattime

@app.route("/")
def initdb():
    db.create_all()
    return "成功！"

if __name__ == '__main__':
    app.run(debug=True)
