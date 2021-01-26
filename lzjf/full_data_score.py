# -*- coding: UTF-8 -*-
import pandas as pd
import datetime
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, Float, Integer,String
import warnings
warnings.filterwarnings("ignore")

pd.set_option('display.max_columns', 100000)  # a就是你要设置显示的最大列数参数
pd.set_option('display.max_rows', 100)  # b就是你要设置显示的最大的行数参数
# hostip = "172.16.21.173"
# hostport = 3306
# hostdb = "stat_info"
# username = "fd_user"
# password = "1qazxsw2"
# REGISTERNO="FF80808173942BCB0173942BCB330002"


# hostip="192.144.143.127"
# hostport=3306
# hostdb="zgc_analysis"
# username="root"
# password="root6114EveryAi!root6114EveryAi"

hostip = "172.16.182.131"
hostport = 3306
hostdb = "stat_info"
username = "litongke"
password = "123456"

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(username, password, hostip, hostport, hostdb))

# buy_order = pd.read_sql_query("select * from con_trs_order where REGISTERNO = '{}' and order_time > DATE_SUB(CURDATE(), INTERVAL 2 YEAR) and order_type = '采购';".format(REGISTERNO),engine)
# saler_order = pd.read_sql_query("select * from con_trs_order where REGISTERNO = '{}' and order_time > DATE_SUB(CURDATE(), INTERVAL 2 YEAR) and order_type = '销售';".format(REGISTERNO),engine)
weight_info = pd.read_sql_query("select * from con_index_weight;", engine)

def write_mysql(df_raw,table_name):
    mysqlInfo = {
        "host": hostip,
        "user": username,
        "password": password,
        "database": hostdb,
        "port": hostport,
        "charset": 'utf8'
    }
    engine = create_engine(
        'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % mysqlInfo,
        encoding='utf-8')
    dtypedict = {'REGISTERNO':VARCHAR(length=200),
                 'company_ID': VARCHAR(length=64)}
    pd.io.sql.to_sql(df_raw, name = table_name, con=engine, index=False, if_exists='append',dtype=dtypedict)

def convert_time(time):
    '''字符转换为时间'''
    if not time:
        return
    try:
        return datetime.datetime.strptime(str(time), "%Y-%m-%d")
    except:
        try:
            return datetime.datetime.strptime(str(time), "%Y-%m-%d %H:%M:%S")
        except:
            try:
                return datetime.datetime.strptime(str(time), "%Y%m%d%H%M%S")
            except:
                try:
                    return datetime.datetime.strptime(str(time), "%Y%m%d")
                except:
                    try:
                         return datetime.datetime.strptime(str(time), "%Y/%m/%d")
                    except:
                        try:
                            return datetime.datetime.strptime(str(time), "%Y/%m/%d %H:%M")
                        except:
                            try:
                                return datetime.datetime.strptime(str(time), "%Y/%m/%d %H:%M:%S")
                            except:
                                try:
                                    return datetime.datetime.strptime(str(time), "%Y-%m-%d %H:%M")
                                except:
                                    try:
                                        return datetime.datetime.strptime(str(time), "%m/%d/%Y %H:%M:%S")
                                    except:
                                        try:
                                            return datetime.datetime.strptime(str(time), "%Y-%m-%dT%H:%M:%S.000+08:00")
                                        except:
                                            print("Wrong date format : %s " % time)
                                            return None


def suppvaluerank(index_full_data,df, max, min, avg,groupby_col=['company_ID_num'], weights={}):
    result = pd.DataFrame()
    for col in min.index:
        i = min.loc[col]
        min_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'min'].item()
        max_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'max'].item()
        tmp = abs((max_data - df[i]) / (max_data - min_data))
        if max_data == min_data and max_data == 0:
            tmp[i] = 1
        if ('company_id'  in result.columns):
            result = pd.concat([result, tmp], axis=1, )
        else:
            result = pd.concat([df['company_id'],result, tmp], axis=1, )
    for col in max.index:
        i = max.loc[col]
        min_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'min'].item()
        max_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'max'].item()
        tmp = abs((df[i] - min_data) / (max_data - min_data))
        if max_data == min_data and max_data == 0:
            tmp[i] = 0
        result = pd.concat([result, tmp], axis=1, )
    for col in avg.index:
        i = avg.loc[col]
        min_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'min'].item()
        max_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'max'].item()
        avg_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'avg'].item()
        tmp = abs((1 - abs(df[i] - avg_data)) / (max_data - min_data))
        if max_data == min_data and max_data == 0:
            tmp[i] = 1
        result = pd.concat([result, tmp], axis=1, )
    return result
def score_exception(a):
    if a > 90:
        return 90.0
    elif a < 0:
        return 0
    else:
        return a
def get_score(rank,weights=[]):
    rank = rank * weights
    rank['score'] = round(rank.sum(axis=1), 4)
   # rank['score'] = round(rank['score'].apply(lambda x :x * 1.6),2)
    rank['rank'] = rank['score'].rank(ascending=False, method='dense').apply(int)
    #重新设置索引列
    rank = rank.reset_index()
    rank = rank.sort_values('rank')
    rank['score'] = rank.apply(lambda x: score_exception(x.score), axis=1)
    rank['grade'] = pd.cut(rank['score'], [0, 15, 30, 45, 60, 100], labels=['E', 'D', 'C', 'B', 'A'])
    rank['bank_creditlimit'] = rank['grade'].map({'A': 700000, 'B': 500000,'C':300000.0,'D':0,'E':0})
    return rank
result = pd.read_sql_query("select * from full_index_info ;",engine)
result = result.fillna(0)
print(result)
#print(weight_info)
weight_info = pd.read_sql_query("select * from con_index_weight;", engine)
index_full_data = pd.read_sql_query("select * from con_index_full_data;",engine)
supp_rank = suppvaluerank(index_full_data,result, weight_info[['index_name', 'index_weight']].loc[weight_info['index_direction'] == 'max', ['index_name']],weight_info.loc[weight_info['index_direction'] == 'min', ['index_name']],weight_info.loc[weight_info['index_direction'] == 'avg', ['index_name']],weights=weight_info)
# write_mysql(supp_rank, "con_index_info_0_1")
print(supp_rank)
supp_weight = dict(zip(weight_info['index_name'], weight_info['index_weight']))
weight_value = []
for i in supp_rank.columns:
    if i != 'company_id' :
        weight_value.append(supp_weight[i])


score = get_score(supp_rank.set_index(['company_id']),weights = weight_value)
cre = pd.merge(result, score, on=['company_id'])[['company_id', 'month_amt_orders_x', 'sale_month_amt_orders_x', 'bank_creditlimit']]
cre = cre[[u'company_id', u'month_amt_orders_x', u'sale_month_amt_orders_x', u'bank_creditlimit']]
cre.columns = ['company_id', 'month_amt_orders', 'sale_month_amt_orders', 'bank_creditlimit']
cre['month_amt_orders'] = cre['month_amt_orders'].apply(lambda x: x * 3)
cre['sale_month_amt_orders'] = cre['sale_month_amt_orders'].apply(lambda x: x * 3)
score['creditlimit'] = cre[["month_amt_orders", "sale_month_amt_orders", "bank_creditlimit"]].min(axis=1)


def function(a,b,c):
    if a in {'A','B','C'} and b > 0 and c > 0 and b/c < 2:
        return 0
    else:
        return 1
def remark(a,b,c):
    if a not in {'A','B','C'}:
        return "评分等级不在范围内"
    elif b < 0 or b == 0:
        return "月均采购订单小于0"
    elif c < 0 or c == 0:
        return "月均销售订单小于0"
    elif b/c > 2 or b/c == 2:
        return "月均采购金额与月均销售金额的比值<= 2"

ss = result[['company_id','month_amt_orders','sale_month_amt_orders']]
ss['grade'] = score[['grade']]
ss['MODELRESULT'] = ss.apply(lambda x: function(x.grade, x.month_amt_orders,x.sale_month_amt_orders), axis=1)
ss['REMARK'] = ss.apply(lambda x: remark(x.grade, x.month_amt_orders,x.sale_month_amt_orders), axis=1)
# print(ss)
score['MODELRESULT'] = ss[['MODELRESULT']]
score['score'] = score.apply(lambda x : score_exception(x.score), axis=1)
print(score)
write_mysql(score, "con_index_score")

#modelRes = score[['company_id','creditlimit','MODELRESULT','grade','score']]
#modelRes.columns = ['company_id','MODELLIMIT','MODELRESULT','MODELLEVEL','MODELSCORE']
#modelRes['REMARK'] = ss[['REMARK']]
# del_sql = "delete from con_approve_score where REGISTERNO = '{}';".format(REGISTERNO)
# query_sql = "select * from con_approve_score where REGISTERNO = '{}';".format(REGISTERNO)
# if pd.read_sql_query(query_sql, engine).empty is not True:
#     engine.execute(del_sql)
# print(modelRes)
# write_mysql(modelRes, "con_approve_score")