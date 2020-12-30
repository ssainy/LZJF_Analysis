import json
import requests
 
 
host = 'https://172.27.128.148'
cycle_id = 24
label_json_path = 'labels1.json'
json_label_index = {0:'大写金额', 1:'小写金额', 2:'地址'} # 旧版标注信息各字段的顺序
#task_type = 'locating'
task_type = 'structuring'
 
def get(url, user_token):
    headers={"cookie":"User-Token={}".format(user_token), 'content-type':'application/json'}
    rsp = requests.get(url, headers=headers, verify=False)
    print(rsp.json())
    return rsp
 
def post(url, user_token, data):
    headers={"cookie":"User-Token={}".format(user_token), 'content-type':'application/json'}
    rsp = requests.post(url, data=data, headers=headers, verify=False)
    print(rsp.json())
 
def get_user_token(host):
    url = "{}/keystone/v1/sessions".format(host)
    payload = "{\"captcha\":\"\",\"username\":\"4pdadmin\",\"remember\":false,\"password\":\"admin\"}\n"
    headers = {'content-type': "application/json"}
    response = requests.request("POST", url, data=payload, headers=headers, verify=False)
    return response.cookies['User-Token']
 
def xywh_to_points(x, y, w, h):
    left_up = [x - int(w/2.0), y - int(h/2.0)]
    right_up = [x + int(w/2.0), y - int(h/2.0)]
    right_down = [x + int(w/2.0), y + int(h/2.0)]
    left_down = [x - int(w/2.0), y + int(h/2.0)]
    return left_up, right_up, right_down, left_down

 
with open(label_json_path) as f:
    tk = get_user_token(host)
    rs = get(host + '/hypc-ocr-backend/v1/learning-cycles/{}/label-tasks?type={}'.format(cycle_id,task_type), get_user_token(host))
    task_id_image_list = rs.json()['data']
    task_id_image_d = {x['name']: x['taskId'] for x in task_id_image_list}
    print(task_id_image_d)
    data = f.readlines()[0]
    jsonf = json.loads(data)
    labels = jsonf['label']
    for x in labels[0:200:1]:
        _image = x['image'].split('/')[-1]
        try:
            taskId = task_id_image_d[_image]
        except Exception as e:
            print('{} has already been labeled'.format(_image))
            continue
 
        req_body = {"taskId":"", "labels":[],"rotation":90,"centerX":1054,"centerY":608}
        req_body['taskId'] = taskId
        for k,v in json_label_index.items():
            filed = x['component'][k]['component_detail']
            labelx = {}
            labelx['type'] = 'rect'
            labelx['content'] = {}
            if task_type == 'structuring':
                labelx['content']['name'] = v
                labelx['content']['value'] = filed['content_value']
            else:
                labelx['content']['name'] = ''
                labelx['content']['value'] = ''
            labelx['bbox'] = {"x": filed['x'] - int(filed['width']/2.0), 
                              "y":filed['y'] - int(filed['height']/2.0), 
                              "width":filed['width'], "height":filed['height'], "rotation": 0}
            req_body['labels'].append(labelx)
 
        print(json.dumps(req_body))
        post(host + '/hypc-ocr-backend/v1/learning-cycles/{}/label-tasks'.format(cycle_id), get_user_token(host), json.dumps(req_body))
