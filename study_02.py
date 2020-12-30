# coding: UTF-8

#pandas工具的应用
#lambda表达式与map
#类型转换语句
#字符串格式化输出
#单引号版本和双引号版本注释
import pandas as pd
import functools
import math
import numpy as np
from pandas import DataFrame
class Demo:
    def __init__(self):
        pass

    def read_csv(self,filename):
        return  pd.read_csv(filename)

    def fun(self,x):
        return x+1

    def fun2(self,x, y, z):
        return x * y * z

    def fun3(self,x):
        if x%2 == 0:
            return x*2
        elif x%3 == 0:
            return x*3
        else:
            return math.pow(x,2)

    def df_change_operate(self,df):
        #改变值
        df.loc[0, 'v_0'] = 15  # 思路：先用loc找到要更改的值，再用赋值（=）的方法实现更换值
        df.iloc[0:100, 2] = 25  # iloc：用索引位置来查找
        # at 、iat只能更换单个值
        df.at[0, 'v_1'] = 28  # iat 用来取某个单值,参数只能用数字索引
        df.iat[0, 2] =29  # at 用来取某个单值,参数只能用index和columns索引名称
        print(df)

    def df_add_operate(self):
        df = DataFrame(np.arange(16).reshape((4, 4)), index=['a', 'b', 'c', 'd'], columns=['one', 'two', 'three', 'four'])
        # 新插入的行一定要加 index,不然会报错
        df.loc['new'] = ['a', 'a', 'a', 'a']
        print(df)
        #新增列
        df['add_column'] = [1,2,3,4,5]
        print(df)
        # 写数据到csv
        df.to_csv("df.scv")
        return df

    def df_remove_operate(self):
        df1 = pd.DataFrame([['Snow', 'M', 22], ['Tyrion', 'M', 32], ['Sansa', 'F', 18], ['Arya', 'F', 14]],
                           columns=['name', 'gender', 'age'])
        print(df1)

        print('---------删除行或列:DataFrame.drop()--------')
        # drop默认对原表不生效，如果要对原表生效，需要加参数：inplace=True

        print("----删除单行----")
        df2 = df1.drop(labels=0)  # axis默认等于0，即按行删除，这里表示按行删除第0行
        print(df2)

        print("------删除多行------")
        # 通过labels来控制删除行或列的个数，如果是删多行/多列，需写成labels=[1,3]，不能写成labels=[1:2],用:号会报错
        # 删除指定的某几行（非连续的）
        df21 = df1.drop(labels=[1, 3], axis=0)  # axis=0 表示按行删除，删除第1行和第3行
        print(df21)

        # 要删除连续的多行可以用range(),删除连续的多列不能用此方法
        df22 = df1.drop(labels=range(1, 4), axis=0)  # axis=0 表示按行删除，删除索引值是第1行至第3行的正行数据
        print(df22)

        print("----删除单列----")
        df3 = df1.drop(labels='gender', axis=1)  # axis=1 表示按列删除，删除gender列
        print(df3)

        print("----删除多列----")
        # 删除指定的某几列
        df4 = df1.drop(labels=['gender', "age"], axis=1)  # axis=1 表示按列删除，删除gender、age列
        print(df4)



if __name__ == '__main__':
    demo_class = Demo()
    csv_result = demo_class.read_csv("demo.csv")
    print(csv_result)
    print(csv_result.columns.tolist())
    # pandas工具的应用
    demo_class.df_change_operate(csv_result)

    demo_class.df_add_operate()

    demo_class.df_remove_operate()

    # lambda表达式与map
    list(map(demo_class.fun, [1, 2, 3]))
    s = [1, 2, 3]
    list(map(lambda x: x + 1, s))

    list(map(demo_class.fun2, [1, 2, 3], [1, 2, 3], [1, 2, 3]))
    s = [1, 2, 3]
    list(map(lambda x, y, z: x * y * z, s, s, s))

    list(map(demo_class.fun3, [1, 2, 3]))

    listDemo = [1, 2, 3, 4, 5]
    new_list = filter(lambda x: x % 2 == 0, listDemo)
    print(list(new_list))

    listDemo = [1, 2, 3, 4, 5]
    product = functools.reduce(lambda x, y: x * y, listDemo)
    print(product)

    # 类型转换语句
    str_test = "1111"
    print(type(str_test))
    str_to_int = int(str_test)
    print(type(str_to_int))

    int_test = 111
    print(type(int_test))
    int_to_str = str(int_test)
    print(type(int_to_str))

    # 字符串格式化输出
    s1 = "i am %s, i am %d years old" % ('jeck', 26)  # 按位置顺序依次输出
    s2 = "i am %(name)s, i am %(age)d years old" % {'name': 'jeck', 'age': 26}  # 自定义key输出
    s3 = "i am %(name)+10s, i am %(age)d years old, i am %(height).2f" % {'name': 'jeck', 'age': 26,
                                                                          'height': 1.7512}  # 定义名字宽度为10,并右对齐.定义身高为浮点类型,保留小数点2位
    s4 = "原数: %d, 八进制:%o , 十六进制:%x" % (15, 15, 15)  # 八进制\十六进制转换
    s5 = "原数:%d, 科学计数法e:%e, 科学计数法E:%E" % (1000000000, 1000000000, 1000000000)  # 科学计数法表示
    s6 = "百分比显示:%.2f %%" % 0.75  # 百分号表示
    print(s1)
    print(s2)
    print(s3)
    print(s4)
    print(s5)


    #单引号版本和双引号版本注释
    """
    print("Hi")
    """
    print("hello, world")

    '''
    print("Hi")
    '''
    print("hello, world")
