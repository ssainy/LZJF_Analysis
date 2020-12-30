from functools import wraps
import csv
import json
import os

DATA_ATTR = '_DATA_ATTR_'
FILE_ATTR = '_FILE_ATTR_'

def ddt(cls):
    for name, func in cls.__dict__.items():
        if hasattr(func, DATA_ATTR):
            for indx,data in enumerate(getattr(func, DATA_ATTR)):
                test_name = "{}_{}".format(name,indx)
                if isinstance(data, tuple) or isinstance(data, list):
                    add_test(cls, test_name, func, func.__doc__, *data)
                else:
                    add_test(cls, test_name, func, func.__doc__, data)
            delattr(cls, name)
        if hasattr(func, FILE_ATTR):
            file_path = getattr(func, FILE_ATTR)
            if not os.path.exists(file_path):
                raise Exception("{} not found".format(file_path))

            if file_path.endswith('.csv'):
                with open(file_path) as f:
                    reader = csv.DictReader(f)
                    for indx,data in enumerate(reader):
                        test_name = "{}_{}".format(name,indx)
                        add_test(cls, test_name, func, func.__doc__, **data)
            elif file_path.endswith('.json'):
                with open(file_path) as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        for indx,value in enumerate(data):
                            test_name = "{}_{}".format(name,indx)
                            add_test(cls, test_name, func, func.__doc__, **value)
                    else:
                        test_name = "{}_{}".format(name,0)
                        add_test(cls, test_name, func, func.__doc__, **data)
            else:
                raise Exception("{} should be .csv or .json file".format(file_path))

            delattr(cls, name)
    return cls

def add_test(cls, name, func, func_doc, *args, **kwargs):
    setattr(cls, name, feed_data(func, func_doc, *args, **kwargs))

def feed_data(func, func_doc, *args, **kwargs):
    @wraps(func)
    def wrapper(self):
        # print(func_doc)
        return func(self, *args, **kwargs)
    # if not func_doc:
    #     wrapper.__doc__ = "input: {}, params:{} {}".format(func.__name__, args, kwargs)
    # else:
    #     wrapper.__doc__ = "input: {}, params:{} {}\n".format(func.__name__, args, kwargs) + func_doc
    # wrapper.__doc__ = wrapper.__doc__ + "input: {}, params:{} {}".format(func.__name__, args, kwargs)
    return wrapper

def data(*value):
    def wrapper(func):
        setattr(func, DATA_ATTR, value)
        return func
    return wrapper

# csv file must has header
def data_file(value):
    def wrapper(func):
        setattr(func, FILE_ATTR, value)
        return func
    return wrapper

#-------------------------
# test case to verify ddt 
#-------------------------

import unittest

@ddt
class Test(unittest.TestCase):
    @data('a')
    def test_one(self, value):
        '''
        asdfasdfasdf
        '''
        print(value)
    
    @data(['a','b','c'],(1,2,3))
    def test_two(self, *value):
        print(value)

    # @data_file('./ddt.csv')
    # def test_csv(self, a, b):
    #     '''asdfasdfasdf 2222'''
    #     print(a,b)
    #
    # @data_file('./ddt.json')
    # def test_json(self, a, b):
    #     print(a,b)
    #
    # @data_file('./ddt_list.json')
    # def test_json2(self, a, b):
    #     '''test_json list testing'''
    #     print(a,b)
    #
    # def test_json2(self):
    #     '''test_json list testing'''
    #     print("testasdfaf")

if __name__ == '__main__':
    unittest.main(verbosity=0)
    # t = Test()
    # print(dir(Test))
    # t.test_one_0(1)