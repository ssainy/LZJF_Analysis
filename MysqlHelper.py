import pymysql
import prettytable as pt
import pandas as pd
from sqlalchemy import create_engine

sql="SELECT * FROM as_bbt_loan where IDNO = '91500108MA5U8F7536';"
def DB_Select(sql):
    dbinfo = {"host": "192.144.143.127",
              "user": "root",
              "password": "root6114EveryAi!root6114EveryAi",
              "db": "4fdbankdev"
              }
    connect1 = pymysql.connect(**dbinfo)
    cursor1 = connect1.cursor()
    cursor1.execute(sql)
    r2 = cursor1.fetchall()
    fileds = [filed[0] for filed in cursor1.description]  # 读取表结构定义
    tb = pt.PrettyTable()
    tb.field_names = fileds
    for i in r2:
        tb.add_row(list(i))

    cursor1.close()
    connect1.close
    return tb

def DB_Test(sql):
    engine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset={}"
                           .format('root',
                                   'root6114EveryAi!root6114EveryAi',
                                   '192.144.143.127:3306',
                                   '4fdbankdev',
                                   'utf8')
                           )
    df = pd.read_sql_query(sql, engine)
    print(df)
    return df

def write_mysql(df_raw,table_name):

    mysqlInfo = {
        "host": '192.144.143.127',
        "user": 'root',
        "password": 'root6114EveryAi!root6114EveryAi',
        "database": '4fdbankdev',
        "port": 3306,
        "charset": 'utf8'
    }
    engine = create_engine(
        'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % mysqlInfo,
        encoding='utf-8')
    # engine = create_engine('mysql+pymysql://amrw:qubPbNoITJ4tDzjw@localhost1:3306/etcpdw_dev')
    pd.io.sql.to_sql(df_raw, table_name, con=engine, index=False, if_exists='replace')







