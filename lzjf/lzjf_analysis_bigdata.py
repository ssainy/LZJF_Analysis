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

datelimit = '2020-12-31'
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


buyer_id = pd.read_sql_query("select buyer_id  from tra_purchase_order where  ordering_time > DATE_SUB('{}', INTERVAL 2 YEAR) group by buyer_id ;".format(datelimit),engine)
seller_id = pd.read_sql_query("select seller_id  from tra_sales_order where  ordering_time > DATE_SUB('{}', INTERVAL 2 YEAR) group by seller_id ;".format(datelimit),engine)
buy_list = buyer_id['buyer_id'].to_list()
# 采购客户ID转为str形
buy_list = [ str(i) for i in buy_list ]
seller_list= seller_id['seller_id'].to_list()
buy_result_all = pd.DataFrame()
saler_result_all = pd.DataFrame()
order = pd.DataFrame()
step = 50
# 取采购订单和销售订单的交集客户
company_list = list(set(buy_list).intersection(set(seller_list)))
b = [company_list[i:i+step] for i in range(0,len(company_list),step)]
for buyer_id in b:
    buyer_id = ','.join(["'%s'" % item for item in buyer_id])
    # print(buyer_id)
    sql = "select * from tra_purchase_order where buyer_id in ({}) and  ordering_time > DATE_SUB('{}', INTERVAL 2 YEAR) ;"
    buy_order = pd.read_sql_query(sql.format(buyer_id,datelimit), engine)
    print("--------------采购订单------------------")
    print(buy_order)
    if buy_order.empty == False:
        buy_order = buy_order[
            [u'order_header_code', u'buyer_id', u'product_name', u'unit_price', u'ordering_quantity', u'ordering_time',
             u'discount_money', u'seller_stock_change_time', u'seller_stock_change_quantity',
             u'buyer_stock_change_time',
             u'buyer_stock_change_quantity', u'order_status', u'total_money', u'seller_id']]

        buy_order.columns = ['order_ID', 'company_id', 'type_of_merchandize', 'unit_price', 'order_num', 'order_time',
                             'discount_amt', 'send_time', 'send_num', 'receive_time', 'receive_num', 'order_status',
                             'pay_amt',
                             'saler_id']
        buy_order['order_time'] = buy_order['order_time'].apply(lambda x: convert_time(x))
        buy_order['send_time'] = buy_order['send_time'].apply(lambda x: convert_time(x))
        buy_order['receive_time'] = buy_order['receive_time'].apply(lambda x: convert_time(x))
        # 最早交易时间
        tmp = buy_order.groupby(['company_id'])['order_time'].min().to_frame()
        tmp.columns = [u'first_tran_time']
        buy_result = tmp.reset_index()
        buy_result.columns = [u'company_id', u'first_tran_time']

        # 最晚交易时间
        tmp = buy_order.groupby(['company_id'])['order_time'].max().to_frame()
        tmp.columns = [u'last_tran_time']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 下单总次数
        tmp = buy_order.groupby(['company_id'])['order_status'].count().to_frame()
        tmp.columns = [u'order_count']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 取消总次数
        tmp = buy_order[buy_order['order_status'] == u'已取消'].groupby(['company_id'])['order_status'].count().to_frame()
        tmp.columns = [u'order_cancel_count']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        buy_order_finish = buy_order[buy_order['order_status'] == u'已完成']
        # 完成总次数
        tmp = buy_order_finish.groupby(['company_id'])['order_status'].count().to_frame()
        tmp.columns = [u'order_finish_count']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')
        # 退货总金额
        tmp = buy_order[buy_order['order_status'] == u'已取消'].groupby(['company_id'])['pay_amt'].sum().to_frame()
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
        tmp = buy_order_finish.groupby(['company_id'])['pay_amt'].sum().to_frame()
        tmp.columns = [u'order_finish_amt']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 优惠次数
        tmp = buy_order_finish[buy_order_finish['discount_amt'] > 0].groupby(['company_id'])[
            'discount_amt'].count().to_frame()
        tmp.columns = [u'discount_count']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 优惠金额
        tmp = buy_order_finish[buy_order_finish['discount_amt'] > 0].groupby(['company_id'])[
            'discount_amt'].sum().to_frame()
        tmp.columns = [u'discount_amt']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        buy_order_finish[u'send_gap'] = buy_order_finish['order_num'] - buy_order_finish['send_num']
        buy_order_finish[u'receive_gap'] = buy_order_finish['send_num'] - buy_order_finish['receive_num']
        buy_order_finish[u'send_gap_amt'] = buy_order_finish[u'send_gap'] * buy_order_finish['unit_price']
        buy_order_finish[u'receive_gap_amt'] = buy_order_finish[u'receive_gap'] * buy_order_finish['unit_price']
        # 发货数量少于下单数量的次数
        tmp = buy_order_finish[buy_order_finish[u'send_gap'] > 0].groupby(['company_id'])['order_ID'].count().to_frame()
        tmp.columns = [u'send_gap_count']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 发货缺口金额
        tmp = buy_order_finish[buy_order_finish[u'send_gap_amt'] > 0].groupby(['company_id'])[
            u'send_gap_amt'].sum().to_frame()
        tmp.columns = [u'send_gap_amt']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 收货数量少于发货数量的次数
        tmp = buy_order_finish[buy_order_finish[u'receive_gap'] > 0].groupby(['company_id'])[
            'order_ID'].count().to_frame()
        tmp.columns = [u'receive_gap_count']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 收货缺口金额
        tmp = buy_order_finish[buy_order_finish[u'receive_gap_amt'] > 0].groupby(['company_id'])[
            u'receive_gap_amt'].sum().to_frame()
        tmp.columns = [u'receive_gap_amt']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 品类个数
        tmp = buy_order_finish.groupby(['company_id', 'type_of_merchandize'])['order_ID'].count().to_frame()
        tmp = tmp.reset_index().groupby(['company_id'])['type_of_merchandize'].count().to_frame()
        tmp.columns = [u'prod_type_count']
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 周交易密度
        buy_order_finish.index = buy_order_finish['order_time']
        tmp = buy_order_finish.groupby(['company_id', buy_order_finish.index.year, buy_order_finish.index.week])[
            'pay_amt'].count().to_frame().reset_index(level='company_id')
        tmp = tmp.groupby(['company_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'week_num_orders']
        tmp = tmp.round(0)
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 周交易金额
        tmp = buy_order_finish.groupby(['company_id', buy_order_finish.index.year, buy_order_finish.index.week])[
            'pay_amt'].sum().to_frame().reset_index(level='company_id')
        tmp = tmp.groupby(['company_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'week_amt_orders']
        tmp = tmp.round(2)

        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 月交易密度
        tmp = buy_order_finish.groupby(['company_id', buy_order_finish.index.year, buy_order_finish.index.month])[
            'pay_amt'].count().to_frame().reset_index(level='company_id')
        tmp = tmp.groupby(['company_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'month_num_orders']
        tmp = tmp.round(0)
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        # 月交易金额
        tmp = buy_order_finish.groupby(['company_id', buy_order_finish.index.year, buy_order_finish.index.month])[
            'pay_amt'].sum().to_frame().reset_index(level='company_id')
        tmp = tmp.groupby(['company_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'month_amt_orders']
        tmp = tmp.round(2)
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')

        buy_order_finish = buy_order_finish.reset_index(drop=True)
        # 过去1、2、3、6月的交易次数和交易金额和
        data_time = buy_order_finish["order_time"].max()
        # data_time = datetime.datetime.now()
        data_time = datetime.datetime.strptime('{}'.format(datelimit), '%Y-%m-%d')
        timediff_list = [30, 60, 90, 180]
        for timediff in timediff_list:
            timespan = datetime.timedelta(days=timediff)
            buy_order_finish_timespan = buy_order_finish[buy_order_finish["order_time"] >= data_time - timespan]
            tmp = buy_order_finish_timespan.groupby(["company_id"])["pay_amt"].agg(["count", "sum"])
            tmp.columns = [u"num_{}_orders".format(timediff), u"amt_{}_orders".format(timediff)]
            buy_result = pd.merge(buy_result, tmp, left_on=[u"company_id"], right_index=True, how="left")

            buy_result = buy_result.fillna(0)

        # fill NAN with 0.0
        buy_result = buy_result.fillna(0)

        buy_order_finish_unique = buy_order_finish[["company_id", "order_ID", "order_time"]].drop_duplicates()


        def timedelta2days(tf):
            try:
                return round(tf.total_seconds() / 3600 / 24, 2)
            except AttributeError:
                return None


        buy_order_finish_unique["diff"] = \
        buy_order_finish_unique.sort_values(by=["order_time"]).groupby(["company_id"])[
            "order_time"].diff().apply(lambda x: timedelta2days(x))
        tmp = buy_order_finish_unique.groupby(["company_id"])["diff"].agg(["mean", "std", "max"])
        tmp.columns = [u"avg_tran_day", u"std_tran_day", u"max_tran_day"]
        buy_result = pd.merge(buy_result, tmp, left_on=[u'company_id'], right_index=True, how='left')
        buy_result_all = buy_result_all.append(buy_result)
        print(buy_result_all)

    sale_order = pd.read_sql_query("select * from tra_sales_order where seller_id in ({}) and ordering_time > DATE_SUB('{}', INTERVAL 2 YEAR) ;".format(buyer_id,datelimit), engine)
    print("--------------销售订单------------------")
    print(sale_order)
    if sale_order.empty == False:
        sale_order = sale_order[
            [u'order_header_code', u'buyer_id', u'product_name', u'unit_price', u'ordering_quantity', u'ordering_time',
             u'discount_money', u'coupon_money', u'seller_stock_change_time', u'seller_stock_change_quantity',
             u'buyer_stock_change_time',
             u'buyer_stock_change_quantity', u'order_status', u'total_money', u'seller_id']]

        sale_order.columns = ['order_ID', 'company_id', 'type_of_merchandize', 'unit_price', 'order_num', 'order_time',
                         'discount_money', 'coupon_money', 'send_time', 'send_num', 'receive_time', 'receive_num',
                         'order_status', 'pay_amt',
                         'saler_id']
        sale_order['order_time'] = sale_order['order_time'].apply(lambda x: convert_time(x))
        sale_order['send_time'] = sale_order['send_time'].apply(lambda x: convert_time(x))
        sale_order['receive_time'] = sale_order['receive_time'].apply(lambda x: convert_time(x))

        # 最早交易时间
        tmp = sale_order.groupby(['saler_id'])['order_time'].min().to_frame()
        tmp.columns = [u'sale_first_tran_time']
        saler_result = tmp.reset_index()
        saler_result.columns = [u'saler_id', u'sale_first_tran_time']
        # 最晚交易时间
        tmp = sale_order.groupby(['saler_id'])['order_time'].max().to_frame()
        tmp.columns = [u'sale_last_tran_time']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')
        saler_result[u'sale_last_tran_time'] = saler_result[u'sale_last_tran_time'].apply(lambda x: convert_time(x))

        # 下单总次数
        tmp = sale_order.groupby(['saler_id'])['order_status'].count().to_frame()
        tmp.columns = [u'sale_order_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 取消总次数
        tmp = sale_order[sale_order['order_status'] == u'已取消'].groupby(['saler_id'])['order_status'].count().to_frame()
        tmp.columns = [u'sale_order_cancel_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        sale_order_finish = sale_order[sale_order['order_status'] == u'已完成']
        # 完成总次数
        tmp = sale_order_finish.groupby(['saler_id'])['order_status'].count().to_frame()
        tmp.columns = [u'sale_order_finish_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 退货总金额
        tmp = sale_order[sale_order['order_status'] == u'已取消'].groupby(['saler_id'])['pay_amt'].sum().to_frame()
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
        tmp = sale_order_finish.groupby(['saler_id'])['pay_amt'].sum().to_frame()
        tmp.columns = [u'sale_order_finish_amt']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        sale_order_finish[u'sale_discount_amt'] = sale_order_finish['discount_money'] + sale_order_finish['coupon_money']
        # 优惠次数
        tmp = sale_order_finish[sale_order_finish['sale_discount_amt'] > 0].groupby(['saler_id'])[
            'sale_discount_amt'].count().to_frame()
        tmp.columns = [u'sale_discount_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 优惠金额
        tmp = sale_order_finish[sale_order_finish['sale_discount_amt'] > 0].groupby(['saler_id'])[
            'sale_discount_amt'].sum().to_frame()
        tmp.columns = [u'sale_discount_amt']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        sale_order_finish[u'sale_send_gap'] = sale_order_finish['order_num'] - sale_order_finish['send_num']
        sale_order_finish[u'sale_receive_gap'] = sale_order_finish['send_num'] - sale_order_finish['receive_num']
        sale_order_finish[u'sale_send_gap_amt'] = sale_order_finish[u'sale_send_gap'] * sale_order_finish['unit_price']
        sale_order_finish[u'sale_receive_gap_amt'] = sale_order_finish[u'sale_receive_gap'] * sale_order_finish['unit_price']
        # 发货数量少于下单数量的次数
        tmp = sale_order_finish[sale_order_finish[u'sale_send_gap'] > 0].groupby(['saler_id'])['order_ID'].count().to_frame()
        tmp.columns = [u'sale_send_gap_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 发货缺口金额
        tmp = sale_order_finish[sale_order_finish[u'sale_send_gap_amt'] > 0].groupby(['saler_id'])[
            u'sale_send_gap_amt'].sum().to_frame()
        tmp.columns = [u'sale_send_gap_amt']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 收货数量少于发货数量的次数
        tmp = sale_order_finish[sale_order_finish[u'sale_receive_gap'] > 0].groupby(['saler_id'])['order_ID'].count().to_frame()
        tmp.columns = [u'sale_receive_gap_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 收货缺口金额
        tmp = sale_order_finish[sale_order_finish[u'sale_receive_gap_amt'] > 0].groupby(['saler_id'])[
            u'sale_receive_gap_amt'].sum().to_frame()
        tmp.columns = [u'sale_receive_gap_amt']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 品类个数
        tmp = sale_order_finish.groupby(['saler_id', 'type_of_merchandize'])['order_ID'].count().to_frame()
        tmp = tmp.reset_index().groupby(['saler_id'])['type_of_merchandize'].count().to_frame()
        tmp.columns = [u'sale_prod_type_count']
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 周交易密度
        sale_order_finish.index = sale_order_finish['order_time']
        tmp = sale_order_finish.groupby(['saler_id', sale_order_finish.index.year, sale_order_finish.index.week])[
            'pay_amt'].count().to_frame().reset_index(level='saler_id')
        tmp = tmp.groupby(['saler_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'sale_week_num_orders']
        tmp = tmp.round(0)
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 周交易金额
        tmp = sale_order_finish.groupby(['saler_id', sale_order_finish.index.year, sale_order_finish.index.week])[
            'pay_amt'].sum().to_frame().reset_index(level='saler_id')
        tmp = tmp.groupby(['saler_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'sale_week_amt_orders']
        tmp = tmp.round(2)
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 月交易密度
        tmp = sale_order_finish.groupby(['saler_id', sale_order_finish.index.year, sale_order_finish.index.month])[
            'pay_amt'].count().to_frame().reset_index(level='saler_id')
        tmp = tmp.groupby(['saler_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'sale_month_num_orders']
        tmp = tmp.round(0)
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        # 月交易金额
        tmp = sale_order_finish.groupby(['saler_id', sale_order_finish.index.year, sale_order_finish.index.month])[
            'pay_amt'].sum().to_frame().reset_index(level='saler_id')
        tmp = tmp.groupby(['saler_id'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'sale_month_amt_orders']
        tmp = tmp.round(2)
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')

        sale_order_finish = sale_order_finish.reset_index(drop=True)
        # 过去1、2、3、6月的交易次数和交易金额和
        data_time = sale_order_finish["order_time"].max()
        # data_time = datetime.datetime.now()
        data_time = datetime.datetime.strptime('{}'.format(datelimit), '%Y-%m-%d')
        timediff_list = [30, 60, 90, 180]
        for timediff in timediff_list:
            timespan = datetime.timedelta(days=timediff)
            sale_order_finish_timespan = sale_order_finish[sale_order_finish["order_time"] >= data_time - timespan]
            tmp = sale_order_finish_timespan.groupby(["saler_id"])["pay_amt"].agg(["count", "sum"])
            tmp.columns = [u"sale_num_{}_orders".format(timediff), u"sale_amt_{}_orders".format(timediff)]
            saler_result = pd.merge(saler_result, tmp, left_on=[u"saler_id"], right_index=True, how="left")

            saler_result = saler_result.fillna(0)

        # fill NAN with 0.0
        saler_result = saler_result.fillna(0)

        sale_order_finish_unique = sale_order_finish[["saler_id", "order_ID", "order_time"]].drop_duplicates()

        sale_order_finish_unique["diff"] = sale_order_finish_unique.sort_values(by=["order_time"]).groupby(["saler_id"])[
            "order_time"].diff().apply(lambda x: timedelta2days(x))
        tmp = sale_order_finish_unique.groupby(["saler_id"])["diff"].agg(["mean", "std", "max"])
        tmp.columns = [u"sale_avg_tran_day", u"sale_std_tran_day", u"sale_max_tran_day"]
        saler_result = pd.merge(saler_result, tmp, left_on=[u'saler_id'], right_index=True, how='left')
        saler_result_all = saler_result_all.append(saler_result)
        print(saler_result_all)

if saler_result_all.empty == False and buy_result_all.empty == False:
    saler_result_all = saler_result_all.rename(columns={'saler_id': 'company_id'})
    # print(saler_result_all)
    saler_result_all['company_id'] = saler_result_all['company_id'].apply(int)
    result = pd.merge(buy_result_all, saler_result_all, on='company_id', how='inner')
    result = result.fillna(0)
    print(result)
    result = result[(result['order_finish_count']>=1)&(result['sale_order_finish_count']>=1)]
    #write_mysql(result, 'full_index_info')
    result = result.drop(
        ['company_id', 'first_tran_time', 'last_tran_time', 'sale_first_tran_time', 'sale_last_tran_time'], axis=1)
    con_index_full_data = pd.DataFrame()
    con_index_full_data['max'] = round(result.max(), 4)
    con_index_full_data['min'] = round(result.min(), 4)
    con_index_full_data['avg'] = round(result.mean(), 4)
    con_index_full_data = con_index_full_data.reset_index()
    print(con_index_full_data)
    #write_mysql(con_index_full_data, 'con_index_full_data')