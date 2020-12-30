



import json
import requests

host = "http://172.27.128.8:40141"
cycle_id = 167
label_json_path = 'label_1000.json'
json_label_index = {0: '大写金额', 1: '小写金额', 2: '地址'}


def get(url, userToken):
    headers = {"Cookie": "User-Token={}".format(userToken), "Content-Type": "application/json"}
    r = requests.get(url, headers=headers)
    return (r.json())


def post(url, userToken, data):
    headers = {"Cookie": "User-Token={}".format(userToken), "Content-Type": "application/json"}
    rsp = requests.post(url, data=data, headers=headers)
    print(rsp)
    print(rsp.json())


def get_user_token(host):
    url = "{}/keystone/v1/sessions".format(host)
    payload = "{\"captcha\":\"\",\"username\":\"4pdadmin\",\"remember\":false,\"password\":\"admin\"}\n"
    headers = {'content-type': "application/json"}
    response = requests.request("POST", url, data=payload, headers=headers, verify=False)
    return response.cookies['User-Token']


def xywh_to_points(x, y, w, h):
    left_up = [x - int(w / 2.0), y - int(h / 2.0)]
    right_up = [x + int(w / 2.0), y - int(h / 2.0)]
    right_down = [x + int(w / 2.0), y + int(h / 2.0)]
    left_down = [x - int(w / 2.0), y + int(h / 2.0)]
    return left_up, right_up, right_down, left_down


with open(label_json_path) as f:
    tk = get_user_token(host)
    rs = get(host + '/hypc-ocr-backend/v1/learning-cycles/{}/label-tasks?type=locating'.format(cycle_id),
             get_user_token(host))
    print(rs)
    task_id_image_list = rs['data']
    task_id_image_d = {x['name']: x['taskId'] for x in task_id_image_list}
    print(task_id_image_d)
    data = f.readlines()[0]
    jsonf = json.loads(data)
    labels = jsonf['label']
    for x in labels:
        _image = x['image'].split('/')[-1]
        try:
            taskId = task_id_image_d[_image]
        except Exception as e:
            print('{} has already been labeled'.format(_image))
            continue
        req_body = {"taskId": "", "labels": [], "rotation": 0, "centerX": 100, "centerY": 210}
        req_body['taskId'] = taskId
        for k, v in json_label_index.items():
            filed = x['component'][k]['component_detail']
            labelx = {}
            labelx['type'] = 'polygon'
            labelx['content'] = {}
            labelx['content']['name'] = ''
            labelx['content']['value'] = ''
            labelx['bbox'] = list(xywh_to_points(filed['x'], filed['y'], filed['width'], filed['height']))
            req_body['labels'].append(labelx)


        print(json.dumps(req_body))
        post(host + '/hypc-ocr-backend/v1/learning-cycles/{}/label-tasks'.format(cycle_id), get_user_token(host),
             json.dumps(req_body))