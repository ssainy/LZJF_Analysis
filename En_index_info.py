# -*- coding: UTF-8 -*-
import pandas as pd
import datetime
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy.types import VARCHAR, Float, Integer,String

sql_host = '192.144.143.127'
sql_port = '3306'
sql_db = '4fdbankdev'
sql_username = 'root'
sql_password = 'root6114EveryAi!root6114EveryAi'
engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(sql_username, sql_password, sql_host, sql_port, sql_db))

def write_mysql(df_raw,table_name):
    mysqlInfo = {
        "host": sql_host,
        "user": sql_username,
        "password": sql_password,
        "database": sql_db,
        "port": sql_port,
        "charset": 'utf8'
    }
    engine = create_engine(
        'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % mysqlInfo,
        encoding='utf-8')
    dtypedict = {'Register_No':VARCHAR(length=200),
                 'company_id': VARCHAR(length=64)}
    pd.io.sql.to_sql(df_raw, name = table_name, con=engine, index=False, if_exists='append',dtype=dtypedict)
    # with engine.connect() as con:
    #     con.execute('ALTER TABLE  lzjf_index_info add constraint p_key primary key (Register_No);')

order = pd.read_sql_query("select * from lzjf_order_test where Register_No = {};".format("20200731"),engine)

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

order['order_time'] = order['order_time'].apply(lambda x: convert_time(x))
order['send_time'] = order['send_time'].apply(lambda x: convert_time(x))
order['receive_time'] = order['receive_time'].apply(lambda x: convert_time(x))

# 最早交易时间
tmp = order.groupby(['company_id','Register_No'])['order_time'].min().to_frame()
tmp.columns = [u'first_tran_time']
result = tmp.reset_index()
result.columns = [u'company_id',u'Register_No', u'first_tran_time']

# 最晚交易时间
tmp = order.groupby(['company_id','Register_No'])['order_time'].max().to_frame()
tmp.columns = [u'last_tran_time']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 下单总次数
tmp = order.groupby(['company_id','Register_No'])['order_status'].count().to_frame()
tmp.columns = [u'order_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 取消总次数
tmp = order[order['order_status'] == u'已取消'].groupby(['company_id','Register_No'])['order_status'].count().to_frame()
tmp.columns = [u'order_cancel_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

order_finish = order[order['order_status'] == u'已完成']
# 完成总次数
tmp = order_finish.groupby(['company_id','Register_No'])['order_status'].count().to_frame()
tmp.columns = [u'order_finish_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')
# 退货总金额
tmp = order[order['pay_amt'] < 0].groupby(['company_id','Register_No'])['pay_amt'].sum().to_frame()
tmp.columns = [u'order_cancel_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 完成率
result = result.fillna(0)
result[u'order_finish_rate'] = result[u'order_finish_count'] / result[u'order_count'].apply(lambda x: float(x))
result[u'order_finish_rate'] = result[u'order_finish_rate'].apply(lambda x: round(x, 4))


# 平均交易周期
result[u'order_avg_date'] = (result[u'last_tran_time'] - result[u'first_tran_time']) / result[u'order_count']
result[u'order_avg_date'] = result[u'order_avg_date'].apply(lambda x: round(x.total_seconds() / 3600 / 24, 1))

# 完成交易总金额
tmp = order_finish.groupby(['company_id','Register_No'])['pay_amt'].sum().to_frame()
tmp.columns = [u'order_finish_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 优惠次数
tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['company_id','Register_No'])[
    'discount_amt'].count().to_frame()
tmp.columns = [u'discount_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 优惠金额
tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['company_id','Register_No'])['discount_amt'].sum().to_frame()
tmp.columns = [u'discount_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

order_finish[u'send_gap'] = order_finish['order_num'] - order_finish['send_num']
order_finish[u'receive_gap'] = order_finish['send_num'] - order_finish['receive_num']
order_finish[u'send_gap_amt'] = order_finish[u'send_gap'] * order_finish['unit_price']
order_finish[u'receive_gap_amt'] = order_finish[u'receive_gap'] * order_finish['unit_price']
# 发货数量少于下单数量的次数
tmp = order_finish[order_finish[u'send_gap'] > 0].groupby(['company_id','Register_No'])['order_ID'].count().to_frame()
tmp.columns = [u'send_gap_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 发货缺口金额
tmp = order_finish[order_finish[u'send_gap_amt'] > 0].groupby(['company_id','Register_No'])[u'send_gap_amt'].sum().to_frame()
tmp.columns = [u'send_gap_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 收货数量少于发货数量的次数
tmp = order_finish[order_finish[u'receive_gap'] > 0].groupby(['company_id','Register_No'])['order_ID'].count().to_frame()
tmp.columns = [u'receive_gap_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 收货缺口金额
tmp = order_finish[order_finish[u'receive_gap_amt'] > 0].groupby(['company_id','Register_No'])[u'receive_gap_amt'].sum().to_frame()
tmp.columns = [u'receive_gap_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 品类个数
tmp = order_finish.groupby(['company_id','Register_No', 'type_of_merchandize'])['order_ID'].count().to_frame()
tmp = tmp.reset_index().groupby(['company_id','Register_No'])['type_of_merchandize'].count().to_frame()
tmp.columns = [u'prod_type_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 周交易密度
order_finish.index = order_finish['order_time']
tmp = order_finish.groupby(['company_id','Register_No', order_finish.index.year, order_finish.index.week])[
    'pay_amt'].count().to_frame().reset_index(level='company_id')
tmp = tmp.groupby(['company_id','Register_No'])['pay_amt'].mean().to_frame()
tmp.columns = [u'week_num_orders']
tmp = tmp.round(0)
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 周交易金额
tmp = order_finish.groupby(['company_id','Register_No', order_finish.index.year, order_finish.index.week])[
    'pay_amt'].sum().to_frame().reset_index(level='company_id')
tmp = tmp.groupby(['company_id','Register_No'])['pay_amt'].mean().to_frame()
tmp.columns = [u'week_amt_orders']
tmp = tmp.round(2)

result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 月交易密度
tmp = order_finish.groupby(['company_id','Register_No', order_finish.index.year, order_finish.index.month])[
    'pay_amt'].count().to_frame().reset_index(level='company_id')
tmp = tmp.groupby(['company_id','Register_No'])['pay_amt'].mean().to_frame()
tmp.columns = [u'month_num_orders']
tmp = tmp.round(0)
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 月交易金额
tmp = order_finish.groupby(['company_id','Register_No', order_finish.index.year, order_finish.index.month])[
    'pay_amt'].sum().to_frame().reset_index(level='company_id')
tmp = tmp.groupby(['company_id','Register_No'])['pay_amt'].mean().to_frame()
tmp.columns = [u'month_amt_orders']
tmp = tmp.round(2)
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

order_finish = order_finish.reset_index(drop=True)
# 过去1、2、3、6月的交易次数和交易金额和
data_time = order_finish["order_time"].max()
timediff_list = [30, 60, 90, 180]
for timediff in timediff_list:
    timespan = datetime.timedelta(days=timediff)
    order_finish_timespan = order_finish[order_finish["order_time"] >= data_time - timespan]
    tmp = order_finish_timespan.groupby(["company_id",'Register_No'])["pay_amt"].agg(["count", "sum"])
    tmp.columns = [u"num_{}_orders".format(timediff), u"amt_{}_orders".format(timediff)]
    result = pd.merge(result, tmp, left_on=[u"company_id",u'Register_No'], right_index=True, how="left")

    result = result.fillna(0)

# fill NAN with 0.0
result = result.fillna(0)

order_finish_unique = order_finish[["company_id",'Register_No', "order_ID", "order_time"]].drop_duplicates()

def timedelta2days(tf):
    try:
        return round(tf.total_seconds() / 3600 / 24, 2)
    except AttributeError:
        return None

order_finish_unique["diff"] = order_finish_unique.sort_values(by=["order_time"]).groupby(["company_id",'Register_No'])[
    "order_time"].diff().apply(lambda x: timedelta2days(x))
tmp = order_finish_unique.groupby(["company_id",'Register_No'])["diff"].agg(["mean", "std", "max"])
tmp.columns = [u"avg_tran_day", u"std_tran_day", u"max_tran_day"]
# tmp = tmp.apply(lambda x: round(x,1))
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 最早交易时间
tmp = order.groupby(['saler_id','Register_No'])['order_time'].min().to_frame()
tmp.columns = [u'sale_first_tran_time']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')
result[u'sale_first_tran_time'] = result[u'sale_first_tran_time'].apply(lambda x: convert_time(x))
# 最晚交易时间
tmp = order.groupby(['saler_id','Register_No'])['order_time'].max().to_frame()
tmp.columns = [u'sale_last_tran_time']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')
result[u'sale_last_tran_time'] = result[u'sale_last_tran_time'].apply(lambda x: convert_time(x))

# 下单总次数
tmp = order.groupby(['saler_id','Register_No'])['order_status'].count().to_frame()
tmp.columns = [u'sale_order_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 取消总次数
tmp = order[order['order_status'] == u'已取消'].groupby(['saler_id','Register_No'])['order_status'].count().to_frame()
tmp.columns = [u'sale_order_cancel_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

order_finish = order[order['order_status'] == u'已完成']
# 完成总次数
tmp = order_finish.groupby(['saler_id','Register_No'])['order_status'].count().to_frame()
tmp.columns = [u'sale_order_finish_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 退货总金额
tmp = order[order['pay_amt'] < 0].groupby(['saler_id','Register_No'])['pay_amt'].sum().to_frame()
tmp.columns = [u'sale_order_cancel_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 完成率
result = result.fillna(0)
result[u'sale_order_finish_rate'] = result[u'sale_order_finish_count'] / result[u'sale_order_count'].apply(lambda x: float(x))
result[u'sale_order_finish_rate'] = result[u'sale_order_finish_rate'].apply(lambda x: round(x, 4))


# 平均交易周期
result[u'sale_order_avg_date'] = (result[u'sale_last_tran_time'] - result[u'sale_first_tran_time']) / result[u'sale_order_count']
result[u'sale_order_avg_date'] = result[u'sale_order_avg_date'].apply(lambda x: round(x.total_seconds() / 3600 / 24, 1))

# 完成交易总金额
tmp = order_finish.groupby(['saler_id','Register_No'])['pay_amt'].sum().to_frame()
tmp.columns = [u'sale_order_finish_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 优惠次数
tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['saler_id','Register_No'])[
    'discount_amt'].count().to_frame()
tmp.columns = [u'sale_discount_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 优惠金额
tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['saler_id','Register_No'])['discount_amt'].sum().to_frame()
tmp.columns = [u'sale_discount_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

order_finish[u'sale_send_gap'] = order_finish['order_num'] - order_finish['send_num']
order_finish[u'sale_receive_gap'] = order_finish['send_num'] - order_finish['receive_num']
order_finish[u'sale_send_gap_amt'] = order_finish[u'sale_send_gap'] * order_finish['unit_price']
order_finish[u'sale_receive_gap_amt'] = order_finish[u'sale_receive_gap'] * order_finish['unit_price']
# 发货数量少于下单数量的次数
tmp = order_finish[order_finish[u'sale_send_gap'] > 0].groupby(['saler_id','Register_No'])['order_ID'].count().to_frame()
tmp.columns = [u'sale_send_gap_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 发货缺口金额
tmp = order_finish[order_finish[u'sale_send_gap_amt'] > 0].groupby(['saler_id','Register_No'])[u'sale_send_gap_amt'].sum().to_frame()
tmp.columns = [u'sale_send_gap_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 收货数量少于发货数量的次数
tmp = order_finish[order_finish[u'sale_receive_gap'] > 0].groupby(['saler_id','Register_No'])['order_ID'].count().to_frame()
tmp.columns = [u'sale_receive_gap_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 收货缺口金额
tmp = order_finish[order_finish[u'sale_receive_gap_amt'] > 0].groupby(['saler_id','Register_No'])[u'sale_receive_gap_amt'].sum().to_frame()
tmp.columns = [u'sale_receive_gap_amt']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 品类个数
tmp = order_finish.groupby(['saler_id','Register_No', 'type_of_merchandize'])['order_ID'].count().to_frame()
tmp = tmp.reset_index().groupby(['saler_id','Register_No'])['type_of_merchandize'].count().to_frame()
tmp.columns = [u'sale_prod_type_count']
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 周交易密度
order_finish.index = order_finish['order_time']
tmp = order_finish.groupby(['saler_id','Register_No', order_finish.index.year, order_finish.index.week])[
    'pay_amt'].count().to_frame().reset_index(level='saler_id')
tmp = tmp.groupby(['saler_id','Register_No'])['pay_amt'].mean().to_frame()
tmp.columns = [u'sale_week_num_orders']
tmp = tmp.round(0)
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 周交易金额
tmp = order_finish.groupby(['saler_id','Register_No', order_finish.index.year, order_finish.index.week])[
    'pay_amt'].sum().to_frame().reset_index(level='saler_id')
tmp = tmp.groupby(['saler_id','Register_No'])['pay_amt'].mean().to_frame()
tmp.columns = [u'sale_week_amt_orders']
tmp = tmp.round(2)
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 月交易密度
tmp = order_finish.groupby(['saler_id','Register_No', order_finish.index.year, order_finish.index.month])[
    'pay_amt'].count().to_frame().reset_index(level='saler_id')
tmp = tmp.groupby(['saler_id','Register_No'])['pay_amt'].mean().to_frame()
tmp.columns = [u'sale_month_num_orders']
tmp = tmp.round(0)
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

# 月交易金额
tmp = order_finish.groupby(['saler_id','Register_No', order_finish.index.year, order_finish.index.month])[
    'pay_amt'].sum().to_frame().reset_index(level='saler_id')
tmp = tmp.groupby(['saler_id','Register_No'])['pay_amt'].mean().to_frame()
tmp.columns = [u'sale_month_amt_orders']
tmp = tmp.round(2)
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

order_finish = order_finish.reset_index(drop=True)
# 过去1、2、3、6月的交易次数和交易金额和
data_time = order_finish["order_time"].max()
timediff_list = [30, 60, 90, 180]
for timediff in timediff_list:
    timespan = datetime.timedelta(days=timediff)
    order_finish_timespan = order_finish[order_finish["order_time"] >= data_time - timespan]
    tmp = order_finish_timespan.groupby(["saler_id",'Register_No'])["pay_amt"].agg(["count", "sum"])
    tmp.columns = [u"sale_num_{}_orders".format(timediff), u"sale_amt_{}_orders".format(timediff)]
    result = pd.merge(result, tmp, left_on=[u"company_id",u'Register_No'], right_index=True, how="left")

    result = result.fillna(0)

# fill NAN with 0.0
result = result.fillna(0)

order_finish_unique = order_finish[["saler_id",'Register_No', "order_ID", "order_time"]].drop_duplicates()


order_finish_unique["diff"] = order_finish_unique.sort_values(by=["order_time"]).groupby(["saler_id",'Register_No'])[
    "order_time"].diff().apply(lambda x: timedelta2days(x))
tmp = order_finish_unique.groupby(["saler_id",'Register_No'])["diff"].agg(["mean", "std", "max"])
tmp.columns = [u"sale_avg_tran_day", u"sale_std_tran_day", u"sale_max_tran_day"]
result = pd.merge(result, tmp, left_on=[u'company_id',u'Register_No'], right_index=True, how='left')

print(result)
pd.io.sql.to_sql(result, name = "lzjf_index_info", con=engine, index=False, if_exists='append',dtype={'Register_No':VARCHAR(length=200),'company_id': VARCHAR(length=64)})
with engine.connect() as con:
    con.execute('alter table lzjf_index_info drop primary key;')
    con.execute('ALTER TABLE  lzjf_index_info add constraint p_key primary key (Register_No,company_id);')