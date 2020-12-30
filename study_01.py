# coding: UTF-8

#从文件中读取json
#改变某个键值对
#进行http请求（注意本请求会是失败）
#将新的json写到新文件里面



import json
import requests

class Demo:
    def __init__(self,endpoint):
        self.endpoint = endpoint

    def parse_json(self,input_path):
        with open(input_path) as file_object:
            lines = file_object.readlines()
        origin_json_str = ""
        for line in lines:
            origin_json_str += line.rstrip()
        # 将字符串转换为json格式
        gbdt_json = json.loads(origin_json_str)
        return gbdt_json

    def get_new_value(self):
        new_key = [i for i in range(100) if i % 2 == 0]
        print(new_key)
        return new_key

    def http_request(self,param):
        requests.session().keep_alive = False
        result = requests.post(self.endPoint, json=param)
        return result

    def combine_request_json(self,gbdt_json):
        json_data = {"model_name": "default", "data": {"input": gbdt_json, "is_traing": None}}
        return json_data

    def write_new_file(self,gbdt_json,output_path):
        gbdt_str = json.dumps(gbdt_json)
        print(type(gbdt_str))
        print(gbdt_str)
        f = open(output_path, "w")
        f.write(gbdt_str)
        f.close()



if __name__ == '__main__':
    endpoint = "http://172.27.133.70:30746/api/predict"
    demo_class = Demo(endpoint)
    origin_json = demo_class.parse_json("json.txt")
    origin_json['rawInstances'][0]['rawFeatures']["v_30"] = demo_class.get_new_value();
    request_json = demo_class.combine_request_json(origin_json)
    result = demo_class.http_request(request_json)



