# -*- coding: GBK -*-
import pandas as pd
import datetime
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, Float, Integer, String
import warnings
warnings.filterwarnings("ignore")


hostip = "172.16.182.131"
hostport = 3306
hostdb = "stat_info"
username = "litongke"
password = "123456"

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(username, password, hostip, hostport, hostdb))

pd.set_option('display.max_columns', 100000)  # a就是你要设置显示的最大列数参数
pd.set_option('display.max_rows', 10)  # b就是你要设置显示的最大的行数参数

def write_mysql(df_raw, table_name):
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
    dtypedict = {
        'company_id': VARCHAR(length=64)}
    pd.io.sql.to_sql(df_raw, name=table_name, con=engine, index=False, if_exists='append', dtype=dtypedict)


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
                                        try:
                                            return datetime.datetime.strptime(str(time), "%m/%d/%Y %H:%M:%S")
                                        except:
                                            #print("Wrong date format : %s " % time)
                                            return None



def suppvaluerank(df, max, min, groupby_col=['company_ID_num'], weights={}):
    buy_result = pd.DataFrame()
    for col in min.index:
        i = min.loc[col]
        tmp = abs(round((df[i].max() - df[i]) / (df[i].max() - df[i].min()), 4))
        if ("company_id" in buy_result.columns):
            buy_result = pd.concat([buy_result, tmp], axis=1, )
        else:
            buy_result = pd.concat([df['company_id'], buy_result, tmp], axis=1, )
    for col in max.index:
        i = max.loc[col]
        tmp = abs(round((df[i] - df[i].min()) / (df[i].max() - df[i].min()), 4))
        buy_result = pd.concat([buy_result, tmp], axis=1, )
    return buy_result


def get_score(rank, weights=[]):
    rank = rank * weights
    rank['score'] = round(rank.sum(axis=1), 4)
    rank['rank'] = rank['score'].rank(ascending=False, method='dense').apply(int)
    # 重新设置索引列
    rank = rank.reset_index()
    rank = rank.sort_values('rank')
    rank['grade'] = pd.cut(rank['score'], [0, 15, 30, 45, 60, 100], labels=['E', 'D', 'C', 'B', 'A'])
    rank['bank_creditlimit'] = rank['grade'].map({'A': 5000000, 'B': 1000000, 'C': 350000, 'D': 0, 'E': 0})
    return rank

com_id = pd.read_sql_query("select buyer_id  from tra_purchase_order where  ordering_time > DATE_SUB(CURDATE(), INTERVAL 2 YEAR) group by buyer_id ;",engine)
seller_id = pd.read_sql_query("select seller_id  from tra_sales_order where  ordering_time > DATE_SUB(CURDATE(), INTERVAL 2 YEAR) group by seller_id ;",engine)
buy_list = com_id['buyer_id']
seller_list= seller_id['seller_id']
buy_result_all = pd.DataFrame()
saler_result_all = pd.DataFrame()
order = pd.DataFrame()
step = 50
company_list = seller_list.to_list()
print(company_list)
b = [company_list[i:i+step] for i in range(0,len(company_list),step)]
for buyer_id in b:
    buyer_id = ','.join(["'%s'" % item for item in buyer_id])
    #print(buyer_id)
    sql = "select * from tra_purchase_order where buyer_id in ({}) and  ordering_time > DATE_SUB(CURDATE(), INTERVAL 2 YEAR) ;"
    order = pd.read_sql_query(sql.format(buyer_id), engine)
    print("--------------采购订单------------------")
    print(order)
    order = order[
        [u'order_header_code', u'buyer_id', u'product_name', u'unit_price', u'ordering_quantity', u'ordering_time',
         u'discount_money', u'seller_stock_change_time', u'seller_stock_change_quantity', u'buyer_stock_change_time',
         u'buyer_stock_change_quantity', u'order_status', u'total_money', u'seller_id']]

    order.columns = ['order_ID', 'company_id', 'type_of_merchandize', 'unit_price', 'order_num', 'order_time',
                     'discount_amt', 'send_time', 'send_num', 'receive_time', 'receive_num', 'order_status', 'pay_amt',
                     'saler_id']
    order['order_time'] = order['order_time'].apply(lambda x: convert_time(x))
    order['send_time'] = order['send_time'].apply(lambda x: convert_time(x))
    order['receive_time'] = order['receive_time'].apply(lambda x: convert_time(x))
    # 最早交易时间
    tmp = order.groupby(['company_id'])['order_time'].min().to_frame()
    tmp.columns = [u'first_tran_time']
    buy_result = tmp.reset_index()
    buy_result.columns = [u'company_id', u'first_tran_time']

    # 最晚交易时间
    tmp = order.groupby(['company_id'])['order_time'].max().to_frame()
    tmp.columns = [u'last_tran_time']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 下单总次数
    tmp = order.groupby(['company_id'])['order_status'].count().to_frame()
    tmp.columns = [u'order_count']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 取消总次数
    tmp = order[order['order_status'] == u'已取消'].groupby(['company_id'])['order_status'].count().to_frame()
    tmp.columns = [u'order_cancel_count']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    order_finish = order[order['order_status'] == u'已完成']
    # 完成总次数
    tmp = order_finish.groupby(['company_id'])['order_status'].count().to_frame()
    tmp.columns = [u'order_finish_count']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')
    # 退货总金额
    tmp = order[order['order_status'] == u'已取消'].groupby(['company_id'])['pay_amt'].sum().to_frame()
    tmp.columns = [u'order_cancel_amt']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 完成率
    buy_result = buy_result.fillna(0)
    buy_result[u'order_finish_rate'] = buy_result[u'order_finish_count'] / buy_result[u'order_count'].apply(
        lambda x: float(x))
    buy_result[u'order_finish_rate'] = buy_result[u'order_finish_rate'].apply(lambda x: round(x, 4))

    # 平均交易周期
    buy_result[u'order_avg_date'] = (buy_result[u'last_tran_time'] - buy_result[u'first_tran_time']) / buy_result[
        u'order_count']
    buy_result[u'order_avg_date'] = buy_result[u'order_avg_date'].apply(
        lambda x: round(x.total_seconds() / 3600 / 24, 1))

    # 完成交易总金额
    tmp = order_finish.groupby(['company_id'])['pay_amt'].sum().to_frame()
    tmp.columns = [u'order_finish_amt']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 优惠次数
    tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['company_id'])[
        'discount_amt'].count().to_frame()
    tmp.columns = [u'discount_count']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 优惠金额
    tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['company_id'])['discount_amt'].sum().to_frame()
    tmp.columns = [u'discount_amt']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    order_finish[u'send_gap'] = order_finish['order_num'] - order_finish['send_num']
    order_finish[u'receive_gap'] = order_finish['send_num'] - order_finish['receive_num']
    order_finish[u'send_gap_amt'] = order_finish[u'send_gap'] * order_finish['unit_price']
    order_finish[u'receive_gap_amt'] = order_finish[u'receive_gap'] * order_finish['unit_price']
    # 发货数量少于下单数量的次数
    tmp = order_finish[order_finish[u'send_gap'] > 0].groupby(['company_id'])['order_ID'].count().to_frame()
    tmp.columns = [u'send_gap_count']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 发货缺口金额
    tmp = order_finish[order_finish[u'send_gap_amt'] > 0].groupby(['company_id'])[u'send_gap_amt'].sum().to_frame()
    tmp.columns = [u'send_gap_amt']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 收货数量少于发货数量的次数
    tmp = order_finish[order_finish[u'receive_gap'] > 0].groupby(['company_id'])['order_ID'].count().to_frame()
    tmp.columns = [u'receive_gap_count']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 收货缺口金额
    tmp = order_finish[order_finish[u'receive_gap_amt'] > 0].groupby(['company_id'])[
        u'receive_gap_amt'].sum().to_frame()
    tmp.columns = [u'receive_gap_amt']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 品类个数
    tmp = order_finish.groupby(['company_id', 'type_of_merchandize'])['order_ID'].count().to_frame()
    tmp = tmp.reset_index().groupby(['company_id'])['type_of_merchandize'].count().to_frame()
    tmp.columns = [u'prod_type_count']
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 周交易密度
    order_finish.index = order_finish['order_time']
    tmp = order_finish.groupby(['company_id', order_finish.index.year, order_finish.index.week])[
        'pay_amt'].count().to_frame().reset_index(level='company_id')
    tmp = tmp.groupby(['company_id'])['pay_amt'].mean().to_frame()
    tmp.columns = [u'week_num_orders']
    tmp = tmp.round(0)
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 周交易金额
    tmp = order_finish.groupby(['company_id', order_finish.index.year, order_finish.index.week])[
        'pay_amt'].sum().to_frame().reset_index(level='company_id')
    tmp = tmp.groupby(['company_id'])['pay_amt'].mean().to_frame()
    tmp.columns = [u'week_amt_orders']
    tmp = tmp.round(2)

    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 月交易密度
    tmp = order_finish.groupby(['company_id', order_finish.index.year, order_finish.index.month])[
        'pay_amt'].count().to_frame().reset_index(level='company_id')
    tmp = tmp.groupby(['company_id'])['pay_amt'].mean().to_frame()
    tmp.columns = [u'month_num_orders']
    tmp = tmp.round(0)
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    # 月交易金额
    tmp = order_finish.groupby(['company_id', order_finish.index.year, order_finish.index.month])[
        'pay_amt'].sum().to_frame().reset_index(level='company_id')
    tmp = tmp.groupby(['company_id'])['pay_amt'].mean().to_frame()
    tmp.columns = [u'month_amt_orders']
    tmp = tmp.round(2)
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

    order_finish = order_finish.reset_index(drop=True)
    # 过去1、2、3、6月的交易次数和交易金额和
    data_time = order_finish["order_time"].max()
    data_time = datetime.datetime.now()
    timediff_list = [30, 60, 90, 180]
    for timediff in timediff_list:
        timespan = datetime.timedelta(days=timediff)
        order_finish_timespan = order_finish[order_finish["order_time"] >= data_time - timespan]
        tmp = order_finish_timespan.groupby(["company_id"])["pay_amt"].agg(["count", "sum"])
        tmp.columns = [u"num_{}_orders".format(timediff), u"amt_{}_orders".format(timediff)]
        buy_result = pd.merge(buy_result, tmp, left_on=[u"company_id"], right_index=True, how="left")

        buy_result = buy_result.fillna(0)

    # fill NAN with 0.0
    buy_result = buy_result.fillna(0)

    order_finish_unique = order_finish[["company_id", "order_ID", "order_time"]].drop_duplicates()


    def timedelta2days(tf):
        try:
            return round(tf.total_seconds() / 3600 / 24, 2)
        except AttributeError:
            return None


    order_finish_unique["diff"] = order_finish_unique.sort_values(by=["order_time"]).groupby(["company_id"])[
        "order_time"].diff().apply(lambda x: timedelta2days(x))
    tmp = order_finish_unique.groupby(["company_id"])["diff"].agg(["mean", "std", "max"])
    tmp.columns = [u"avg_tran_day", u"std_tran_day", u"max_tran_day"]
    buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')
    buy_result_all = buy_result_all.append(buy_result)
    print(buy_result_all)
    order = pd.read_sql_query("select * from tra_sales_order where seller_id in ({}) and ordering_time > DATE_SUB(CURDATE(), INTERVAL 2 YEAR) ;".format(buyer_id), engine)
    print("--------------销售订单------------------")
    print(order)
    if order.empty == False:
        order = order[
            [u'order_header_code', u'buyer_id', u'product_name', u'unit_price', u'ordering_quantity', u'ordering_time',
             u'discount_money', u'coupon_money', u'seller_stock_change_time', u'seller_stock_change_quantity',
             u'buyer_stock_change_time',
             u'buyer_stock_change_quantity', u'order_status', u'total_money', u'seller_id']]

        order.columns = ['order_ID', 'company_id', 'type_of_merchandize', 'unit_price', 'order_num', 'order_time',
                         'discount_money', 'coupon_money', 'send_time', 'send_num', 'receive_time', 'receive_num',
                         'order_status', 'pay_amt',
                         'saler_id']
        order['order_time'] = order['order_time'].apply(lambda x: convert_time(x))
        order['send_time'] = order['send_time'].apply(lambda x: convert_time(x))
        order['receive_time'] = order['receive_time'].apply(lambda x: convert_time(x))

        # 最早交易时间
        tmp = order.groupby(['saler_id'])['order_time'].min().to_frame()
        tmp.columns = [u'sale_first_tran_time']
        saler_result = tmp.reset_index()
        saler_result.columns = [u'saler_id', u'sale_first_tran_time']
        # 最晚交易时间
        tmp = order.groupby(['saler_id'])['order_time'].max().to_frame()
        tmp.columns = [u'sale_last_tran_time']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')
        saler_result[u'sale_last_tran_time'] = saler_result[u'sale_last_tran_time'].apply(lambda x: convert_time(x))

        # 下单总次数
        tmp = order.groupby(['saler_id'])['order_status'].count().to_frame()
        tmp.columns = [u'sale_order_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 取消总次数
        tmp = order[order['order_status'] == u'已取消'].groupby(['saler_id'])['order_status'].count().to_frame()
        tmp.columns = [u'sale_order_cancel_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        order_finish = order[order['order_status'] == u'已完成']
        # 完成总次数
        tmp = order_finish.groupby(['saler_id'])['order_status'].count().to_frame()
        tmp.columns = [u'sale_order_finish_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 退货总金额
        tmp = order[order['order_status'] == u'已取消'].groupby(['saler_id'])['pay_amt'].sum().to_frame()
        tmp.columns = [u'sale_order_cancel_amt']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 完成率
        saler_result = saler_result.fillna(0)
        saler_result[u'sale_order_finish_rate'] = saler_result[u'sale_order_finish_count'] / saler_result[
            u'sale_order_count'].apply(lambda x: float(x))
        saler_result[u'sale_order_finish_rate'] = saler_result[u'sale_order_finish_rate'].apply(lambda x: round(x, 4))

        # 平均交易周期
        saler_result[u'sale_order_avg_date'] = (saler_result[u'sale_last_tran_time'] - saler_result[
            u'sale_first_tran_time']) / saler_result[u'sale_order_count']
        saler_result[u'sale_order_avg_date'] = saler_result[u'sale_order_avg_date'].apply(
            lambda x: round(x.total_seconds() / 3600 / 24, 1))

        # 完成交易总金额
        tmp = order_finish.groupby(['saler_id'])['pay_amt'].sum().to_frame()
        tmp.columns = [u'sale_order_finish_amt']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        order_finish[u'sale_discount_amt'] = order_finish['discount_money'] + order_finish['coupon_money']
        # 优惠次数
        tmp = order_finish[order_finish['sale_discount_amt'] > 0].groupby(['saler_id'])[
            'sale_discount_amt'].count().to_frame()
        tmp.columns = [u'sale_discount_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 优惠金额
        tmp = order_finish[order_finish['sale_discount_amt'] > 0].groupby(['saler_id'])[
            'sale_discount_amt'].sum().to_frame()
        tmp.columns = [u'sale_discount_amt']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        order_finish[u'sale_send_gap'] = order_finish['order_num'] - order_finish['send_num']
        order_finish[u'sale_receive_gap'] = order_finish['send_num'] - order_finish['receive_num']
        order_finish[u'sale_send_gap_amt'] = order_finish[u'sale_send_gap'] * order_finish['unit_price']
        order_finish[u'sale_receive_gap_amt'] = order_finish[u'sale_receive_gap'] * order_finish['unit_price']
        # 发货数量少于下单数量的次数
        tmp = order_finish[order_finish[u'sale_send_gap'] > 0].groupby(['saler_id'])['order_ID'].count().to_frame()
        tmp.columns = [u'sale_send_gap_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 发货缺口金额
        tmp = order_finish[order_finish[u'sale_send_gap_amt'] > 0].groupby(['saler_id'])[
            u'sale_send_gap_amt'].sum().to_frame()
        tmp.columns = [u'sale_send_gap_amt']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 收货数量少于发货数量的次数
        tmp = order_finish[order_finish[u'sale_receive_gap'] > 0].groupby(['saler_id'])['order_ID'].count().to_frame()
        tmp.columns = [u'sale_receive_gap_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 收货缺口金额
        tmp = order_finish[order_finish[u'sale_receive_gap_amt'] > 0].groupby(['saler_id'])[
            u'sale_receive_gap_amt'].sum().to_frame()
        tmp.columns = [u'sale_receive_gap_amt']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 品类个数
        tmp = order_finish.groupby(['saler_id', 'type_of_merchandize'])['order_ID'].count().to_frame()
        tmp = tmp.reset_index().groupby(['saler_id'])['type_of_merchandize'].count().to_frame()
        tmp.columns = [u'sale_prod_type_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 周交易密度
        order_finish.index = order_finish['order_time']
        tmp = order_finish.groupby(['saler_id', order_finish.index.year, order_finish.index.week])[
            'pay_amt'].count().to_frame().reset_index(level='saler_id')
        tmp = tmp.groupby(['saler_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'sale_week_num_orders']
        tmp = tmp.round(0)
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 周交易金额
        tmp = order_finish.groupby(['saler_id', order_finish.index.year, order_finish.index.week])[
            'pay_amt'].sum().to_frame().reset_index(level='saler_id')
        tmp = tmp.groupby(['saler_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'sale_week_amt_orders']
        tmp = tmp.round(2)
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 月交易密度
        tmp = order_finish.groupby(['saler_id', order_finish.index.year, order_finish.index.month])[
            'pay_amt'].count().to_frame().reset_index(level='saler_id')
        tmp = tmp.groupby(['saler_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'sale_month_num_orders']
        tmp = tmp.round(0)
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 月交易金额
        tmp = order_finish.groupby(['saler_id', order_finish.index.year, order_finish.index.month])[
            'pay_amt'].sum().to_frame().reset_index(level='saler_id')
        tmp = tmp.groupby(['saler_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'sale_month_amt_orders']
        tmp = tmp.round(2)
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        order_finish = order_finish.reset_index(drop=True)
        # 过去1、2、3、6月的交易次数和交易金额和
        data_time = order_finish["order_time"].max()
        data_time = datetime.datetime.now()
        timediff_list = [30, 60, 90, 180]
        for timediff in timediff_list:
            timespan = datetime.timedelta(days=timediff)
            order_finish_timespan = order_finish[order_finish["order_time"] >= data_time - timespan]
            tmp = order_finish_timespan.groupby(["saler_id"])["pay_amt"].agg(["count", "sum"])
            tmp.columns = [u"sale_num_{}_orders".format(timediff), u"sale_amt_{}_orders".format(timediff)]
            saler_result = pd.merge(saler_result, tmp, left_on=[u"saler_id"], right_index=True, how="left")

            saler_result = saler_result.fillna(0)

        # fill NAN with 0.0
        saler_result = saler_result.fillna(0)

        order_finish_unique = order_finish[["saler_id", "order_ID", "order_time"]].drop_duplicates()

        order_finish_unique["diff"] = order_finish_unique.sort_values(by=["order_time"]).groupby(["saler_id"])[
            "order_time"].diff().apply(lambda x: timedelta2days(x))
        tmp = order_finish_unique.groupby(["saler_id"])["diff"].agg(["mean", "std", "max"])
        tmp.columns = [u"sale_avg_tran_day", u"sale_std_tran_day", u"sale_max_tran_day"]
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')
        saler_result_all = saler_result_all.append(saler_result)
        print(saler_result_all)

if saler_result_all.empty == False:
    saler_result_all = saler_result_all.rename(columns={'saler_id': 'company_id'})
    saler_result_all['company_id'] = saler_result_all['company_id'].apply(int)
    result = pd.merge(buy_result_all, saler_result_all, on='company_id', how='left')
    write_mysql(result, 'full_index_info')
    result = result.drop(
        ['company_id', 'first_tran_time', 'last_tran_time', 'sale_first_tran_time', 'sale_last_tran_time'], axis=1)
    con_index_full_data = pd.DataFrame()
    con_index_full_data['max'] = round(result.max(), 4)
    con_index_full_data['min'] = round(result.min(), 4)
    con_index_full_data['avg'] = round(result.mean(), 4)
    con_index_full_data = con_index_full_data.reset_index()
    print(con_index_full_data)
    write_mysql(con_index_full_data, 'con_index_full_data')
