# Time     2021/03/02 11::41
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 调用接口返回各种异常信息

import requests
import json

# 设置请求头
headers = {
    "User - Agent": "Mozilla / 5.0(Windows NT 10.0;Win64;x64;rv: 68.0) Gecko / 20100101 Firefox / 68.0",
    "Content-Type": "application/json;charset=utf-8"
}


# 获取连接
def getRequestContent(url, data):
    try:
        # 设置连接信息
        requestsContent = requests .post(url=url, data=json .dumps(data), headers=headers)
        # 提取接口返回的数据
        requestsTextStr = requestsContent .text
        # 返回的是一个字符串的数据,转换为字典类型
        requestsTextJson = json .loads(requestsTextStr)
        return requestsTextJson
    except Exception as error:
        raise error


if __name__ == '__main__':
    url = "http://10.76.226.222:9016/datashsnc/api/query"
    data = {
        "access_token": "cboss_bussiness_group",
        "metric": "shsnc_logkeyword_pm",
        "start": "2021-02-02 09:00:00",
        "end": "2021-02-02 09:01:00",
        "tags": {
            "bc_info": "person-cboss",  # 对应appid
            "cluster_id": "ALL",
            "inst_range": "*",  # 对应容器，默认传*，取回所有报错容器
            "jf_name": "ALL",
            "keyword": "\\\"java.lang.NullPointerException\\\"",  # 对应关键字，因原始数据加了””，取数时需要反编译
            "keyword_type": "ALL"
        }
    }
    dataJsons = getRequestContent(url, data)
    for dataJson in dataJsons['results']:
        print(dataJson)

