import MysqlHelper
import unittest
import pandas as pd
import datetime
pd.options.mode.chained_assignment = None # default='warn'
# 控制台打印完整内容设置方法
pd.set_option('display.max_columns', 100000)  # a就是你要设置显示的最大列数参数
pd.set_option('display.max_rows', 10)  # b就是你要设置显示的最大的行数参数


#   pd.set_option('display.width', 1000000)  # x就是你要设置的显示的宽度，防止轻易换行

class SimpleUnitTest(unittest.TestCase):

    def test_order(self):
        order = MysqlHelper.DB_Test("select * from lzjf_order_test;")
        # order = order[[ u'company_ID', u'saler_ID']]
        # order.columns = [ 'company_ID', 'saler_ID',]

        # print(order)
        def convert_time(time):
            try:
                return datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
            except:
                return ''

        order['order_time'] = order['order_time'].apply(lambda x: convert_time(x))
        order['send_time'] = order['send_time'].apply(lambda x: convert_time(x))
        order['receive_time'] = order['receive_time'].apply(lambda x: convert_time(x))

        # 最早交易时间
        tmp = order.groupby(['company_ID'])['order_time'].min().to_frame()
        tmp.columns = [u'最早交易时间']
        result = tmp.reset_index()
        result.columns = [u'企业名称', u'最早交易时间']

        # 最晚交易时间
        tmp = order.groupby(['company_ID'])['order_time'].max().to_frame()
        tmp.columns = [u'最近交易时间']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 下单总次数
        tmp = order.groupby(['company_ID'])['order_status'].count().to_frame()
        tmp.columns = [u'下单总次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 取消总次数
        tmp = order[order['order_status'] == u'已取消'].groupby(['company_ID'])['order_status'].count().to_frame()
        tmp.columns = [u'订单取消总次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        order_finish = order[order['order_status'] == u'已完成']
        # 完成总次数
        tmp = order_finish.groupby(['company_ID'])['order_status'].count().to_frame()
        tmp.columns = [u'订单完成总次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')
        # 退货总金额
        tmp = order[order['pay_amt'] < 0].groupby(['company_ID'])['pay_amt'].sum().to_frame()
        tmp.columns = [u'退货总金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 完成率
        result = result.fillna(0)
        result[u'订单完成率'] = result[u'订单完成总次数'] / result[u'下单总次数'].apply(lambda x: float(x))
        result[u'订单完成率'] = result[u'订单完成率'].apply(lambda x: round(x, 4))

        # 平均交易周期
        result[u'平均下单周期（天）'] = (result[u'最近交易时间'] - result[u'最早交易时间']) / result[u'下单总次数']
        result[u'平均下单周期（天）'] = result[u'平均下单周期（天）'].apply(lambda x: round(x.total_seconds() / 3600 / 24, 1))

        # 完成交易总金额
        tmp = order_finish.groupby(['company_ID'])['pay_amt'].sum().to_frame()
        tmp.columns = [u'完成交易总金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 优惠次数
        tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['company_ID'])[
            'discount_amt'].count().to_frame()
        tmp.columns = [u'优惠次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 优惠金额
        tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['company_ID'])['discount_amt'].sum().to_frame()
        tmp.columns = [u'优惠总金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        order_finish[u'发货缺口'] = order_finish['order_num'] - order_finish['send_num']
        order_finish[u'收货缺口'] = order_finish['send_num'] - order_finish['receive_num']
        order_finish[u'发货缺口金额'] = order_finish[u'发货缺口'] * order_finish['unit_price']
        order_finish[u'收货缺口金额'] = order_finish[u'收货缺口'] * order_finish['unit_price']
        # 发货数量少于下单数量的次数
        tmp = order_finish[order_finish[u'发货缺口'] > 0].groupby(['company_ID'])['order_ID'].count().to_frame()
        tmp.columns = [u'发货缺口次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 发货缺口金额
        tmp = order_finish[order_finish[u'发货缺口金额'] > 0].groupby(['company_ID'])[u'发货缺口金额'].sum().to_frame()
        tmp.columns = [u'发货缺口金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 收货数量少于发货数量的次数
        tmp = order_finish[order_finish[u'收货缺口'] > 0].groupby(['company_ID'])['order_ID'].count().to_frame()
        tmp.columns = [u'收货缺口次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 收货缺口金额
        tmp = order_finish[order_finish[u'收货缺口金额'] > 0].groupby(['company_ID'])[u'收货缺口金额'].sum().to_frame()
        tmp.columns = [u'收货缺口金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 品类个数
        tmp = order_finish.groupby(['company_ID', 'type_of_merchandize'])['order_ID'].count().to_frame()
        tmp = tmp.reset_index().groupby(['company_ID'])['type_of_merchandize'].count().to_frame()
        tmp.columns = [u'品类个数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 周交易密度
        order_finish.index = order_finish['order_time']
        tmp = order_finish.groupby(['company_ID', order_finish.index.year, order_finish.index.week])[
            'pay_amt'].count().to_frame().reset_index(level='company_ID')
        tmp = tmp.groupby(['company_ID'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'周交易密度']
        tmp = tmp.round(0)
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 周交易金额
        tmp = order_finish.groupby(['company_ID', order_finish.index.year, order_finish.index.week])[
            'pay_amt'].sum().to_frame().reset_index(level='company_ID')
        tmp = tmp.groupby(['company_ID'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'周交易金额']
        tmp = tmp.round(2)

        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 月交易密度
        tmp = order_finish.groupby(['company_ID', order_finish.index.year, order_finish.index.month])[
            'pay_amt'].count().to_frame().reset_index(level='company_ID')
        tmp = tmp.groupby(['company_ID'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'月交易密度']
        tmp = tmp.round(0)
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 月交易金额
        tmp = order_finish.groupby(['company_ID', order_finish.index.year, order_finish.index.month])[
            'pay_amt'].sum().to_frame().reset_index(level='company_ID')
        tmp = tmp.groupby(['company_ID'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'月交易金额']
        tmp = tmp.round(2)
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        order_finish = order_finish.reset_index(drop=True)
        # 过去1、2、3、6月的交易次数和交易金额和
        data_time = order_finish["order_time"].max()
        timediff_list = [30, 60, 90, 180]
        for timediff in timediff_list:
            timespan = datetime.timedelta(days=timediff)
            order_finish_timespan = order_finish[order_finish["order_time"] >= data_time - timespan]
            tmp = order_finish_timespan.groupby(["company_ID"])["pay_amt"].agg(["count", "sum"])
            tmp.columns = [u"过去{}天交易次数".format(timediff), u"过去{}天交易金额".format(timediff)]
            result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how="left")

            result = result.fillna(0)

        # fill NAN with 0.0
        result = result.fillna(0)

        order_finish_unique = order_finish[["company_ID", "order_ID", "order_time"]].drop_duplicates()

        def timedelta2days(tf):
            try:
                return round(tf.total_seconds() / 3600 / 24, 2)
            except AttributeError:
                return None

        order_finish_unique["diff"] = order_finish_unique.sort_values(by=["order_time"]).groupby(["company_ID"])[
            "order_time"].diff().apply(lambda x: timedelta2days(x))
        tmp = order_finish_unique.groupby(["company_ID"])["diff"].agg(["mean", "std", "max"])
        tmp.columns = [u"交易间隔时间的均值（天）", u"交易间隔时间的标准差（天）", u"最大交易间隔时间（天）"]
        # tmp = tmp.apply(lambda x: round(x,1))
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 最早交易时间
        tmp = order.groupby(['saler_ID'])['order_time'].min().to_frame()
        tmp.columns = [u'销售_最早交易时间']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')
        # 最晚交易时间
        tmp = order.groupby(['saler_ID'])['order_time'].max().to_frame()
        tmp.columns = [u'销售_最近交易时间']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 下单总次数
        tmp = order.groupby(['saler_ID'])['order_status'].count().to_frame()
        tmp.columns = [u'销售_下单总次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 取消总次数
        tmp = order[order['order_status'] == u'已取消'].groupby(['saler_ID'])['order_status'].count().to_frame()
        tmp.columns = [u'销售_订单取消总次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        order_finish = order[order['order_status'] == u'已完成']
        # 完成总次数
        tmp = order_finish.groupby(['saler_ID'])['order_status'].count().to_frame()
        tmp.columns = [u'销售_订单完成总次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 退货总金额
        tmp = order[order['pay_amt'] < 0].groupby(['saler_ID'])['pay_amt'].sum().to_frame()
        tmp.columns = [u'销售_退货总金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 完成率
        result = result.fillna(0)
        result[u'销售_订单完成率'] = result[u'销售_订单完成总次数'] / result[u'销售_下单总次数'].apply(lambda x: float(x))
        result[u'销售_订单完成率'] = result[u'销售_订单完成率'].apply(lambda x: round(x, 4))

        # 平均交易周期
        result[u'销售_平均下单周期（天）'] = (result[u'销售_最近交易时间'] - result[u'销售_最早交易时间']) / result[u'销售_下单总次数']
        result[u'销售_平均下单周期（天）'] = result[u'销售_平均下单周期（天）'].apply(lambda x: round(x.total_seconds() / 3600 / 24, 1))

        # 完成交易总金额
        tmp = order_finish.groupby(['saler_ID'])['pay_amt'].sum().to_frame()
        tmp.columns = [u'销售_完成交易总金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 优惠次数
        tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['saler_ID'])[
            'discount_amt'].count().to_frame()
        tmp.columns = [u'销售_优惠次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 优惠金额
        tmp = order_finish[order_finish['discount_amt'] > 0].groupby(['saler_ID'])['discount_amt'].sum().to_frame()
        tmp.columns = [u'销售_优惠总金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        order_finish[u'销售_发货缺口'] = order_finish['order_num'] - order_finish['send_num']
        order_finish[u'销售_收货缺口'] = order_finish['send_num'] - order_finish['receive_num']
        order_finish[u'销售_发货缺口金额'] = order_finish[u'销售_发货缺口'] * order_finish['unit_price']
        order_finish[u'销售_收货缺口金额'] = order_finish[u'销售_收货缺口'] * order_finish['unit_price']
        # 发货数量少于下单数量的次数
        tmp = order_finish[order_finish[u'销售_发货缺口'] > 0].groupby(['saler_ID'])['order_ID'].count().to_frame()
        tmp.columns = [u'销售_发货缺口次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 发货缺口金额
        tmp = order_finish[order_finish[u'销售_发货缺口金额'] > 0].groupby(['saler_ID'])[u'销售_发货缺口金额'].sum().to_frame()
        tmp.columns = [u'销售_发货缺口金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 收货数量少于发货数量的次数
        tmp= order_finish[order_finish[u'销售_收货缺口'] > 0].groupby(['saler_ID'])['order_ID'].count().to_frame()
        tmp.columns = [u'销售_收货缺口次数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 收货缺口金额
        tmp = order_finish[order_finish[u'销售_收货缺口金额'] > 0].groupby(['saler_ID'])[u'销售_收货缺口金额'].sum().to_frame()
        tmp.columns = [u'销售_收货缺口金额']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 品类个数
        tmp = order_finish.groupby(['saler_ID', 'type_of_merchandize'])['order_ID'].count().to_frame()
        tmp = tmp.reset_index().groupby(['saler_ID'])['type_of_merchandize'].count().to_frame()
        tmp.columns = [u'销售_品类个数']
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 周交易密度
        order_finish.index = order_finish['order_time']
        tmp = order_finish.groupby(['saler_ID', order_finish.index.year, order_finish.index.week])[
            'pay_amt'].count().to_frame().reset_index(level='saler_ID')
        tmp = tmp.groupby(['saler_ID'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'销售_周交易密度']
        tmp = tmp.round(0)
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 周交易金额
        tmp = order_finish.groupby(['saler_ID', order_finish.index.year, order_finish.index.week])[
            'pay_amt'].sum().to_frame().reset_index(level='saler_ID')
        tmp = tmp.groupby(['saler_ID'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'销售_周交易金额']
        tmp = tmp.round(2)
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 月交易密度
        tmp = order_finish.groupby(['saler_ID', order_finish.index.year, order_finish.index.month])[
            'pay_amt'].count().to_frame().reset_index(level='saler_ID')
        tmp = tmp.groupby(['saler_ID'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'销售_月交易密度']
        tmp = tmp.round(0)
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        # 月交易金额
        tmp = order_finish.groupby(['saler_ID', order_finish.index.year, order_finish.index.month])[
            'pay_amt'].sum().to_frame().reset_index(level='saler_ID')
        tmp = tmp.groupby(['saler_ID'])['pay_amt'].mean().to_frame()
        tmp.columns = [u'销售_月交易金额']
        tmp = tmp.round(2)
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        order_finish = order_finish.reset_index(drop=True)
        # 过去1、2、3、6月的交易次数和交易金额和
        data_time = order_finish["order_time"].max()
        timediff_list = [30, 60, 90, 180]
        for timediff in timediff_list:
            timespan = datetime.timedelta(days=timediff)
            order_finish_timespan = order_finish[order_finish["order_time"] >= data_time - timespan]
            tmp = order_finish_timespan.groupby(["saler_ID"])["pay_amt"].agg(["count", "sum"])
            tmp.columns = [u"销售_过去{}天交易次数".format(timediff), u"销售_过去{}天交易金额".format(timediff)]
            result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how="left")

            result = result.fillna(0)

        # fill NAN with 0.0
        result = result.fillna(0)

        order_finish_unique = order_finish[["saler_ID", "order_ID", "order_time"]].drop_duplicates()


        order_finish_unique["diff"] = order_finish_unique.sort_values(by=["order_time"]).groupby(["saler_ID"])[
            "order_time"].diff().apply(lambda x: timedelta2days(x))
        tmp = order_finish_unique.groupby(["saler_ID"])["diff"].agg(["mean", "std", "max"])
        tmp.columns = [u"销售_交易间隔时间的均值（天）", u"销售_交易间隔时间的标准差（天）", u"销售_最大交易间隔时间（天）"]
        # tmp = tmp.apply(lambda x: round(x,1))
        result = pd.merge(result, tmp, left_on=[u'企业名称'], right_index=True, how='left')

        print(result)
        MysqlHelper.write_mysql(result, "lzjf_index_info")



if __name__ == '__main__':
    unittest.main()





