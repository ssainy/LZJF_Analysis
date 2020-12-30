import numpy as np
import pandas as pd
import os
import datetime
import sys
import re
import json
import time
import zipfile
# import cpca  # needs manually installing
from sklearn import preprocessing


unique = lambda x:len(set(x))  # 唯一值的个数

def read_file(f,**kw):
    '''读文件'''
    try:
        tmp = pd.read_csv(f,**kw)
    except:
        try:
            tmp = pd.read_excel(f,**kw)
        except:
            try:
                tmp = pd.read_table(f,**kw)
            except:
                print('Wrong file: ',f)
                return None
    return tmp

def walk_zip_files(path,pattern_zip='.*.zip',pattern_file='.*trx.*.csv',colnum=False,
                   pattern_date='201[89].\d{2}.\d{2}',**kw):
    '''
    解析文件夹下所有zip里的符合pattern的文件
    参数：
    -- path：文件路径
    -- pattern_zip：zip文件正则表达式
    -- pattern_file：文本文件正则表达式
    -- colnum：文件列数；int；默认False代表不用指定列数来过滤文件
    -- pattern_date：文件日期正则表达式，False表示不取日期列
    -- **kw：文件读取参数
    返回：
    -- 符合条件的所有文件合并后的dataframe
    '''
    import zipfile
    import re
    trx = None

    for dirpath,dirnames,filenames in os.walk(path):
        for file in filenames:
            if re.search(pattern_zip,file):
                fullpath=os.path.join(dirpath,file)
                with zipfile.ZipFile(fullpath, "r") as z:
                    for i in z.namelist():
                        if re.search(pattern_file,i):
                            f = z.open(i)
#                             print('reading: ',fullpath)
                            tmp = read_file(f,**kw)
                            try:
                                print(fullpath,i,'行列数：',tmp.shape)
                                if pattern_date:
                                    try:
                                        tmp['file_date'] = re.findall(string=fullpath,pattern=pattern_date)[0]
                                    except: pass
                                if colnum:
                                    if tmp.shape[1] == colnum:
                                        trx = pd.concat([trx,tmp],axis=0,ignore_index=True)
                                else:
                                    trx = pd.concat([trx,tmp],axis=0,ignore_index=True)
                            except: pass
    return trx

def walk_files(path,pattern_file='.*loan.*csv',colnum=False,pattern_date='201[89].\d{2}.\d{2}',**kw):
    '''
    提取指定path目录下所有符合pattern正则式文件的内容
    参数：
    -- path：文件路径
    -- pattern_file：文件名正则表达式
    -- colnum：列数;int；默认False代表不用指定列数来过滤文件
    -- pattern_date：文件日期正则表达式，从文本文件路径中得到
    -- **kw：文件读取参数
    返回：
    -- 符合条件的所有文件合并后的dataframe
    '''
    import re
    data = None
    for dirpath,dirnames,filenames in os.walk(path):
        for file in filenames:
            if re.search(pattern_file,file):
                fullpath=os.path.join(dirpath,file)
#                 print('reading: ',fullpath)
                tmp = read_file(fullpath,**kw)
                try:
                    print(fullpath,'行列数：',tmp.shape)
                    if pattern_date:
                        ## 一般路径或文件名会含有文件上传时间，匹配pattern_date正则来解析文件日期
                        try:
                            tmp['file_date'] = re.findall(string=fullpath,pattern=pattern_date)[0]
                        except:pass
                    if colnum:
                        if tmp.shape[1] == colnum:
                            data = pd.concat([data,tmp],axis=0,ignore_index=True)
                    else:
                        data = pd.concat([data,tmp],axis=0,ignore_index=True)
                except: pass
    return data



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

def convert_time_col(df,pattern_date='(time|date|ddl)',suffix=''):
    '''
    对df含有时间类型的列，其字符串转换成标准时间
    参数：
    -- df:dataframe
    -- pattern_date:时间列表达式
    -- suffix：时间列添加后缀
    '''
    columns = df.columns
    # 时间类型
    time_cols = columns[columns.str.lower().str.contains(pat=pattern_date)]
    for time_col in time_cols:
        df[str(time_col)+suffix] = df[time_col].apply(convert_time)
    return df