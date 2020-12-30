# -*- coding: UTF-8 -*-
import pandas as pd
import datetime
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, Float, Integer,String

pd.set_option('display.max_columns', 100000)  # a就是你要设置显示的最大列数参数
pd.set_option('display.max_rows', 100)  # b就是你要设置显示的最大的行数参数
hostip="192.144.143.127"
hostport=3306
hostdb="pscs_congnition_dev"
username="root"
password="root6114EveryAi!root6114EveryAi"
REGISTERNO="FF80808173942BCB0173942BCB330007"

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(username, password, hostip, hostport, hostdb))
index_full_data = pd.read_sql_query("select * from con_index_full_data;",engine)
buy_order = pd.read_sql_query("select * from con_trs_order_copy where REGISTERNO = '{}' and order_time > DATE_SUB(CURDATE(), INTERVAL 2 YEAR) and order_type = '采购';".format(REGISTERNO),engine)
saler_order = pd.read_sql_query("select * from con_trs_order_copy where REGISTERNO = '{}' and order_time > DATE_SUB(CURDATE(), INTERVAL 2 YEAR) and order_type = '销售';".format(REGISTERNO),engine)
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
                                        return datetime.datetime.strptime(str(time), "%Y-%m-%dT%H:%M:%S.000+08:00")
                                    except:
                                        print("Wrong date format : %s " % time)
                                        return None
def update_index(index_full_data,df,weight_info):
    for i in weight_info['index_name']:
        min_data = index_full_data.loc[index_full_data['index'] == i, 'min'].item()
        max_data = index_full_data.loc[index_full_data['index'] == i, 'max'].item()
        data = round(df.loc[0, i].item(),4)
        if data > max_data:
            max_sql = "update con_index_full_data a set a.max = '{}' where a.index = '{}';".format(data, i)
            engine.execute(max_sql)
        if data < min_data:
            min_sql = "update con_index_full_data a set a.min = '{}' where a.index = '{}';".format(data, i)
            engine.execute(min_sql)
    index_full_data = pd.read_sql_query("select * from con_index_full_data;",engine)
    return index_full_data

def suppvaluerank(index_full_data,df, max, min, groupby_col=['company_ID_num'], weights={}):
    result = pd.DataFrame()
    for col in min.index:
        i = min.loc[col]
        min_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'min'].item()
        max_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'max'].item()
        tmp = abs(round((max_data - df[i]) / (max_data - min_data), 4))
        if ('company_ID' and 'REGISTERNO' in result.columns):
            result = pd.concat([result, tmp], axis=1, )
        else:
            result = pd.concat([df['company_ID'],df['REGISTERNO'],result, tmp], axis=1, )
    for col in max.index:
        i = max.loc[col]
        min_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'min'].item()
        max_data = index_full_data.loc[index_full_data['index'] == i['index_name'], 'max'].item()
        tmp = abs(round((df[i] - min_data) / (max_data - min_data), 4))
        result = pd.concat([result, tmp], axis=1, )
    return result
def score_exception(a):
    if a > 100 or a < 0:
        return 0.0001
    else:
        return a
def grade_exception(a,b):
    if a == 0.0001:
        return "N"
    else:
        return b
def credit_exception(a,b):
    if a == "N":
        return 0
    else:
        return b
def get_score(rank,weights=[]):
    rank = rank * weights
    rank['score'] = round(rank.sum(axis=1), 4)
    rank['rank'] = rank['score'].rank(ascending=False, method='dense').apply(int)
    rank['score'] = rank['score'].apply(lambda x: score_exception(x))
    #重新设置索引列
    rank = rank.reset_index()
    rank = rank.sort_values('rank')
    rank['grade'] = pd.cut(rank['score'], [0, 15, 30, 45, 60, 100], labels=['E', 'D', 'C', 'B', 'A'])
    rank['grade'] = rank.apply(lambda x:grade_exception(x.score,x.grade),axis = 1)
    rank['bank_creditlimit'] = rank['grade'].map({'A': 2000000, 'B': 500000,'C':300000,'D':0,'E':0,'N':0})
    return rank

buy_order['order_time'] = buy_order['order_time'].apply(lambda x: convert_time(x))
buy_order['send_time'] = buy_order['send_time'].apply(lambda x: convert_time(x))
buy_order['receive_time'] = buy_order['receive_time'].apply(lambda x: convert_time(x))
# 最早交易时间
tmp = buy_order.groupby(['company_ID','REGISTERNO'])['order_time'].min().to_frame()
tmp.columns = [u'first_tran_time']
buy_result = tmp.reset_index()
buy_result.columns = [u'company_ID',u'REGISTERNO', u'first_tran_time']

# 最晚交易时间
tmp = buy_order.groupby(['company_ID','REGISTERNO'])['order_time'].max().to_frame()
tmp.columns = [u'last_tran_time']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 下单总次数
tmp = buy_order.groupby(['company_ID','REGISTERNO'])['order_status'].count().to_frame()
tmp.columns = [u'order_count']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 取消总次数
tmp = buy_order[buy_order['order_status'] == u'已取消'].groupby(['company_ID','REGISTERNO'])['order_status'].count().to_frame()
tmp.columns = [u'order_cancel_count']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

buy_order_finish = buy_order[buy_order['order_status'] == u'已完成']
# 完成总次数
tmp = buy_order_finish.groupby(['company_ID','REGISTERNO'])['order_status'].count().to_frame()
tmp.columns = [u'order_finish_count']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')
# 退货总金额
tmp = buy_order[buy_order['order_status'] == u'已取消'].groupby(['company_ID','REGISTERNO'])['pay_amt'].sum().to_frame()
tmp.columns = [u'order_cancel_amt']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 完成率
buy_result = buy_result.fillna(0)
buy_result[u'order_finish_rate'] = buy_result[u'order_finish_count'] / buy_result[u'order_count'].apply(lambda x: float(x))
buy_result[u'order_finish_rate'] = buy_result[u'order_finish_rate'].apply(lambda x: round(x, 4))


# 平均交易周期
buy_result[u'order_avg_date'] = (buy_result[u'last_tran_time'] - buy_result[u'first_tran_time']) / buy_result[u'order_count']
buy_result[u'order_avg_date'] = buy_result[u'order_avg_date'].apply(lambda x: round(x.total_seconds() / 3600 / 24, 1))

# 完成交易总金额
tmp = buy_order_finish.groupby(['company_ID','REGISTERNO'])['pay_amt'].sum().to_frame()
tmp.columns = [u'order_finish_amt']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 优惠次数
tmp = buy_order_finish[buy_order_finish['discount_amt'] > 0].groupby(['company_ID','REGISTERNO'])[
    'discount_amt'].count().to_frame()
tmp.columns = [u'discount_count']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 优惠金额
tmp = buy_order_finish[buy_order_finish['discount_amt'] > 0].groupby(['company_ID','REGISTERNO'])['discount_amt'].sum().to_frame()
tmp.columns = [u'discount_amt']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')
buy_order_finish[u'send_gap'] = buy_order_finish['order_num'] - buy_order_finish['send_num']
buy_order_finish[u'receive_gap'] = buy_order_finish['send_num'] - buy_order_finish['receive_num']
buy_order_finish[u'send_gap_amt'] = buy_order_finish[u'send_gap'] * buy_order_finish['unit_price']
buy_order_finish[u'receive_gap_amt'] = buy_order_finish[u'receive_gap'] * buy_order_finish['unit_price']
# 发货数量少于下单数量的次数
tmp = buy_order_finish[buy_order_finish[u'send_gap'] > 0].groupby(['company_ID','REGISTERNO'])['order_ID'].count().to_frame()
tmp.columns = [u'send_gap_count']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 发货缺口金额
tmp = buy_order_finish[buy_order_finish[u'send_gap_amt'] > 0].groupby(['company_ID','REGISTERNO'])[u'send_gap_amt'].sum().to_frame()
tmp.columns = [u'send_gap_amt']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 收货数量少于发货数量的次数
tmp = buy_order_finish[buy_order_finish[u'receive_gap'] > 0].groupby(['company_ID','REGISTERNO'])['order_ID'].count().to_frame()
tmp.columns = [u'receive_gap_count']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 收货缺口金额
tmp = buy_order_finish[buy_order_finish[u'receive_gap_amt'] > 0].groupby(['company_ID','REGISTERNO'])[u'receive_gap_amt'].sum().to_frame()
tmp.columns = [u'receive_gap_amt']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 品类个数
tmp = buy_order_finish.groupby(['company_ID','REGISTERNO', 'type_of_merchandize'])['order_ID'].count().to_frame()
tmp = tmp.reset_index().groupby(['company_ID','REGISTERNO'])['type_of_merchandize'].count().to_frame()
tmp.columns = [u'prod_type_count']
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 周交易密度
buy_order_finish.index = buy_order_finish['order_time']
tmp = buy_order_finish.groupby(['company_ID','REGISTERNO', buy_order_finish.index.year, buy_order_finish.index.week])[
    'pay_amt'].count().to_frame().reset_index(level='company_ID')
tmp = tmp.groupby(['company_ID','REGISTERNO'])['pay_amt'].mean().to_frame()
tmp.columns = [u'week_num_orders']
tmp = tmp.round(0)
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 周交易金额
tmp = buy_order_finish.groupby(['company_ID','REGISTERNO', buy_order_finish.index.year, buy_order_finish.index.week])[
    'pay_amt'].sum().to_frame().reset_index(level='company_ID')
tmp = tmp.groupby(['company_ID','REGISTERNO'])['pay_amt'].mean().to_frame()
tmp.columns = [u'week_amt_orders']
tmp = tmp.round(2)

buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 月交易密度
tmp = buy_order_finish.groupby(['company_ID','REGISTERNO', buy_order_finish.index.year, buy_order_finish.index.month])[
    'pay_amt'].count().to_frame().reset_index(level='company_ID')
tmp = tmp.groupby(['company_ID','REGISTERNO'])['pay_amt'].mean().to_frame()
tmp.columns = [u'month_num_orders']
tmp = tmp.round(0)
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

# 月交易金额
tmp = buy_order_finish.groupby(['company_ID','REGISTERNO', buy_order_finish.index.year, buy_order_finish.index.month])[
    'pay_amt'].sum().to_frame().reset_index(level='company_ID')
tmp = tmp.groupby(['company_ID','REGISTERNO'])['pay_amt'].mean().to_frame()

tmp.columns = [u'month_amt_orders']
tmp = tmp.round(2)
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

buy_order_finish = buy_order_finish.reset_index(drop=True)
# 过去1、2、3、6月的交易次数和交易金额和
data_time = buy_order_finish["order_time"].max()
data_time = datetime.datetime.now()
timediff_list = [30, 60, 90, 180]
for timediff in timediff_list:
    timespan = datetime.timedelta(days=timediff)
    buy_order_finish_timespan = buy_order_finish[buy_order_finish["order_time"] >= data_time - timespan]
    tmp = buy_order_finish_timespan.groupby(["company_ID",'REGISTERNO'])["pay_amt"].agg(["count", "sum"])
    tmp.columns = [u"num_{}_orders".format(timediff), u"amt_{}_orders".format(timediff)]
    buy_result = pd.merge(buy_result, tmp, left_on=[u"company_ID",u'REGISTERNO'], right_index=True, how="left")

    buy_result = buy_result.fillna(0)

# fill NAN with 0.0
buy_result = buy_result.fillna(0)

buy_order_finish_unique = buy_order_finish[["company_ID",'REGISTERNO', "order_ID", "order_time"]].drop_duplicates()

def timedelta2days(tf):
    try:
        return round(tf.total_seconds() / 3600 / 24, 2)
    except AttributeError:
        return None

buy_order_finish_unique["diff"] = buy_order_finish_unique.sort_values(by=["order_time"]).groupby(["company_ID",'REGISTERNO'])[
    "order_time"].diff().apply(lambda x: timedelta2days(x))
tmp = buy_order_finish_unique.groupby(["company_ID",'REGISTERNO'])["diff"].agg(["mean", "std", "max"])
tmp.columns = [u"avg_tran_day", u"std_tran_day", u"max_tran_day"]
# tmp = tmp.apply(lambda x: round(x,1))
buy_result = pd.merge(buy_result, tmp, left_on=[u'company_ID',u'REGISTERNO'], right_index=True, how='left')

buy_result = buy_result.fillna(0)

saler_order['order_time'] = saler_order['order_time'].apply(lambda x: convert_time(x))
saler_order['send_time'] = saler_order['send_time'].apply(lambda x: convert_time(x))
saler_order['receive_time'] = saler_order['receive_time'].apply(lambda x: convert_time(x))

# 最早交易时间
tmp = saler_order.groupby(['saler_ID','REGISTERNO'])['order_time'].min().to_frame()
tmp.columns = [u'sale_first_tran_time']
saler_result = tmp.reset_index()
saler_result.columns = [u'saler_ID',u'REGISTERNO', u'sale_first_tran_time']

# 最晚交易时间
tmp = saler_order.groupby(['saler_ID','REGISTERNO'])['order_time'].max().to_frame()
tmp.columns = [u'sale_last_tran_time']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')
# 下单总次数
tmp = saler_order.groupby(['saler_ID','REGISTERNO'])['order_status'].count().to_frame()
tmp.columns = [u'sale_order_count']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 取消总次数
tmp = saler_order[saler_order['order_status'] == u'已取消'].groupby(['saler_ID','REGISTERNO'])['order_status'].count().to_frame()
tmp.columns = [u'sale_order_cancel_count']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

saler_order_finish = saler_order[saler_order['order_status'] == u'已完成']
# 完成总次数
tmp = saler_order_finish.groupby(['saler_ID','REGISTERNO'])['order_status'].count().to_frame()
tmp.columns = [u'sale_order_finish_count']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 退货总金额
tmp = saler_order[saler_order['order_status'] == u'已取消'].groupby(['saler_ID','REGISTERNO'])['pay_amt'].sum().to_frame()
tmp.columns = [u'sale_order_cancel_amt']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 完成率
saler_result[u'sale_order_finish_rate'] = saler_result[u'sale_order_finish_count'] / saler_result[u'sale_order_count'].apply(lambda x: float(x))
saler_result[u'sale_order_finish_rate'] = saler_result[u'sale_order_finish_rate'].apply(lambda x: round(x, 4))

# 平均交易周期
saler_result[u'sale_order_avg_date'] = (saler_result[u'sale_last_tran_time'] - saler_result[u'sale_first_tran_time']) / saler_result[u'sale_order_count']
saler_result[u'sale_order_avg_date'] = saler_result[u'sale_order_avg_date'].apply(lambda x: round(x.total_seconds() / 3600 / 24, 1))

# 完成交易总金额
tmp = saler_order_finish.groupby(['saler_ID','REGISTERNO'])['pay_amt'].sum().to_frame()
tmp.columns = [u'sale_order_finish_amt']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 优惠次数
tmp = saler_order_finish[saler_order_finish['discount_amt'] > 0].groupby(['saler_ID','REGISTERNO'])[
    'discount_amt'].count().to_frame()
tmp.columns = [u'sale_discount_count']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 优惠金额
tmp = saler_order_finish[saler_order_finish['discount_amt'] > 0].groupby(['saler_ID','REGISTERNO'])['discount_amt'].sum().to_frame()
tmp.columns = [u'sale_discount_amt']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

saler_order_finish[u'sale_send_gap'] = saler_order_finish['order_num'] - saler_order_finish['send_num']
saler_order_finish[u'sale_receive_gap'] = saler_order_finish['send_num'] - saler_order_finish['receive_num']
saler_order_finish[u'sale_send_gap_amt'] = saler_order_finish[u'sale_send_gap'] * saler_order_finish['unit_price']
saler_order_finish[u'sale_receive_gap_amt'] = saler_order_finish[u'sale_receive_gap'] * saler_order_finish['unit_price']
# 发货数量少于下单数量的次数
tmp = saler_order_finish[saler_order_finish[u'sale_send_gap'] > 0].groupby(['saler_ID','REGISTERNO'])['order_ID'].count().to_frame()
tmp.columns = [u'sale_send_gap_count']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 发货缺口金额
tmp = saler_order_finish[saler_order_finish[u'sale_send_gap_amt'] > 0].groupby(['saler_ID','REGISTERNO'])[u'sale_send_gap_amt'].sum().to_frame()
tmp.columns = [u'sale_send_gap_amt']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 收货数量少于发货数量的次数
tmp = saler_order_finish[saler_order_finish[u'sale_receive_gap'] > 0].groupby(['saler_ID','REGISTERNO'])['order_ID'].count().to_frame()
tmp.columns = [u'sale_receive_gap_count']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 收货缺口金额
tmp = saler_order_finish[saler_order_finish[u'sale_receive_gap_amt'] > 0].groupby(['saler_ID','REGISTERNO'])[u'sale_receive_gap_amt'].sum().to_frame()
tmp.columns = [u'sale_receive_gap_amt']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 品类个数
tmp = saler_order_finish.groupby(['saler_ID','REGISTERNO', 'type_of_merchandize'])['order_ID'].count().to_frame()
tmp = tmp.reset_index().groupby(['saler_ID','REGISTERNO'])['type_of_merchandize'].count().to_frame()
tmp.columns = [u'sale_prod_type_count']
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 周交易密度
saler_order_finish.index = saler_order_finish['order_time']
tmp = saler_order_finish.groupby(['saler_ID','REGISTERNO', saler_order_finish.index.year, saler_order_finish.index.week])[
    'pay_amt'].count().to_frame().reset_index(level='saler_ID')
tmp = tmp.groupby(['saler_ID','REGISTERNO'])['pay_amt'].mean().to_frame()
tmp.columns = [u'sale_week_num_orders']
tmp = tmp.round(0)
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 周交易金额
tmp = saler_order_finish.groupby(['saler_ID','REGISTERNO', saler_order_finish.index.year, saler_order_finish.index.week])[
    'pay_amt'].sum().to_frame().reset_index(level='saler_ID')
tmp = tmp.groupby(['saler_ID','REGISTERNO'])['pay_amt'].mean().to_frame()
tmp.columns = [u'sale_week_amt_orders']
tmp = tmp.round(2)
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 月交易密度
tmp = saler_order_finish.groupby(['saler_ID','REGISTERNO', saler_order_finish.index.year, saler_order_finish.index.month])[
    'pay_amt'].count().to_frame().reset_index(level='saler_ID')
tmp = tmp.groupby(['saler_ID','REGISTERNO'])['pay_amt'].mean().to_frame()
tmp.columns = [u'sale_month_num_orders']
tmp = tmp.round(0)
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

# 月交易金额
tmp = saler_order_finish.groupby(['saler_ID','REGISTERNO', saler_order_finish.index.year, saler_order_finish.index.month])[
    'pay_amt'].sum().to_frame().reset_index(level='saler_ID')
tmp = tmp.groupby(['saler_ID','REGISTERNO'])['pay_amt'].mean().to_frame()
tmp.columns = [u'sale_month_amt_orders']
tmp = tmp.round(2)
saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

saler_order_finish = saler_order_finish.reset_index(drop=True)
# 过去1、2、3、6月的交易次数和交易金额和
data_time = saler_order_finish["order_time"].max()
data_time = datetime.datetime.now()
timediff_list = [30, 60, 90, 180]
for timediff in timediff_list:
    timespan = datetime.timedelta(days=timediff)
    saler_order_finish_timespan = saler_order_finish[saler_order_finish["order_time"] >= data_time - timespan]
    tmp = saler_order_finish_timespan.groupby(["saler_ID",'REGISTERNO'])["pay_amt"].agg(["count", "sum"])
    tmp.columns = [u"sale_num_{}_orders".format(timediff), u"sale_amt_{}_orders".format(timediff)]
    saler_result = pd.merge(saler_result, tmp, left_on=[u"saler_ID",u'REGISTERNO'], right_index=True, how="left")

    saler_result = saler_result.fillna(0)

# fill NAN with 0.0
saler_result = saler_result.fillna(0)

saler_order_finish_unique = saler_order_finish[["saler_ID",'REGISTERNO', "order_ID", "order_time"]].drop_duplicates()


saler_order_finish_unique["diff"] = saler_order_finish_unique.sort_values(by=["order_time"]).groupby(["saler_ID",'REGISTERNO'])[
    "order_time"].diff().apply(lambda x: timedelta2days(x))
tmp = saler_order_finish_unique.groupby(["saler_ID",'REGISTERNO'])["diff"].agg(["mean", "std", "max"])
tmp.columns = [u"sale_avg_tran_day", u"sale_std_tran_day", u"sale_max_tran_day"]

saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_ID',u'REGISTERNO'], right_index=True, how='left')

saler_result =saler_result.rename(columns={'saler_ID': 'company_ID'})
result = pd.merge(buy_result,saler_result,on = ['company_ID','REGISTERNO'] , how='left')
# 指标值计算结果
pd.io.sql.to_sql(result, name = "con_index_info", con=engine, index=False, if_exists='append',dtype={'REGISTERNO':VARCHAR(length=200),'company_ID': VARCHAR(length=64)})
# with engine.connect() as con:
#     con.execute('alter table con_index_info drop primary key;')
#     con.execute('ALTER TABLE  con_index_info add constraint p_key primary key (REGISTERNO,company_ID);')

index_info = result
print("指标参数数据为：")
print(index_info)
# 指标参数归一化计算
index_full_data = index_full_data.fillna(0)
index_info = index_info.fillna(0)
index_full_data = update_index(index_full_data,index_info,weight_info[['index_name', 'index_weight']])
supp_rank = suppvaluerank(index_full_data,index_info, weight_info[['index_name', 'index_weight']].loc[weight_info['index_direction'] == 'max', ['index_name']],weight_info.loc[weight_info['index_direction'] == 'min', ['index_name']],weights=weight_info)
# write_mysql(supp_rank, "con_index_info_0_1")
supp_weight = dict(zip(weight_info['index_name'], weight_info['index_weight']))
weight_value = []
for i in supp_rank.columns:
    if i != 'company_ID' and i != 'REGISTERNO':
        weight_value.append(supp_weight[i])
print("指标得分数据（不含权重）为：")
print(supp_rank)
# 指标参数得分乘以权重数据
score = get_score(supp_rank.set_index(['company_ID','REGISTERNO']),weights = weight_value)
print("指标得分数据（包含权重）为：")
print(score)
cre = pd.merge(index_info, score, on=['company_ID','REGISTERNO'])[['company_ID','REGISTERNO', 'month_amt_orders_x', 'sale_month_amt_orders_x', 'bank_creditlimit']]
cre = cre[[u'company_ID',u'REGISTERNO', u'month_amt_orders_x', u'sale_month_amt_orders_x', u'bank_creditlimit']]
cre.columns = ['company_ID','REGISTERNO', 'month_amt_orders', 'sale_month_amt_orders', 'bank_creditlimit']
print(cre)
cre['month_amt_orders'] = cre['month_amt_orders'].apply(lambda x: x * 3)
cre['sale_month_amt_orders'] = cre['sale_month_amt_orders'].apply(lambda x: x * 3)
# min（月均采购金额*3，月均销售金额*3，评分等级对应授信最大限额）
score['creditlimit'] = round(cre[["month_amt_orders", "sale_month_amt_orders", "bank_creditlimit"]].min(axis=1),4)
print(cre)
def function(a,b,c):
    print(a,b,c)
    if a in {'A','B','C'} and b > 0 and c > 0 and b/c < 2:
        return 0
    else:
        return 1

def remark(a,b,c):
    if a == "N":
        return "评级出错了，请检查订单数据~"
    elif a not in {'A','B','C'}:
        return "评分等级不在范围内"
    elif b < 0 or b == 0.0:
        return "月均采购订单小于等于0"
    elif c < 0 or c == 0.0:
        return "月均销售订单小于等于0"
    elif b/c > 2 or b/c == 2:
        return "月均采购金额与月均销售金额的比值>= 2"

ss = index_info[['company_ID','REGISTERNO','month_amt_orders','sale_month_amt_orders']]
ss['grade'] = score[['grade']]
ss['MODELRESULT'] = ss.apply(lambda x: function(x.grade, x.month_amt_orders,x.sale_month_amt_orders), axis=1)
ss['REMARK'] = ss.apply(lambda x: remark(x.grade, x.month_amt_orders,x.sale_month_amt_orders), axis=1)
# print(ss)
score['MODELRESULT'] = ss[['MODELRESULT']]

def credit_fun(a,b):
    if a == 1:
        return 0
    else:
        return b

score['creditlimit'] = score.apply(lambda x:credit_fun(x.MODELRESULT,x.creditlimit),axis=1)
# print(score)
write_mysql(score, "con_index_score")
modelRes = score[['REGISTERNO','creditlimit','MODELRESULT','grade','score']]
modelRes.columns = ['REGISTERNO','MODELLIMIT','MODELRESULT','MODELLEVEL','MODELSCORE']
modelRes['REMARK'] = ss[['REMARK']]
del_sql = "delete from con_approve_score where REGISTERNO = '{}';".format(REGISTERNO)
query_sql = "select * from con_approve_score where REGISTERNO = '{}';".format(REGISTERNO)
if pd.read_sql_query(query_sql, engine).empty is not True:
    engine.execute(del_sql)
print(modelRes)
write_mysql(modelRes, "con_approve_score")