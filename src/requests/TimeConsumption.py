# Time     2021/01/08 10::06
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import requests
import json
import time
import datetime
from ProgramConfigurationOnly import Config
import sys
url = None
headers = {
        "User - Agent": "Mozilla / 5.0(Windows NT 10.0;Win64;x64;rv: 68.0) Gecko / 20100101 Firefox / 68.0",
        "Content-Type": "application/json;charset=utf-8"
}
createTime = datetime.datetime.now()


def getInterface(interfaceUrl, interfaceDate, interfaceUrlHeaders):
    try:
        # 获取请求返回信息的内容 post请求
        requestsContent = requests.post(url=interfaceUrl, data=json.dumps(interfaceDate), headers=interfaceUrlHeaders)
        # 提取接口返回的数据
        requestsText = requestsContent.text
        return requestsText
    except Exception as error:
        print(error)


# 统计数据
def sums(vaules):
    return sum([float(vaule['value']) for vaule in vaules])


def regionExecute(data, interface):
    try:
        # 循环11个地市
        code = None
        for region in range(569, 570):
            successNums = 0.0
            businessFailNums = 0.0
            systemFailNums = 0.0
            successRate = ""
            if region == 569:
                code = "ALL"
            else:
                code = str(region)
            data['tags']['citycode'] = code

            # 耗时
            data['tags']['code'] = "0"
            content = getInterface(url, data, headers)
            content = json.loads(content)['results']
            successNums = sums(content)

            contNums = successNums

            successRate = str(round(successNums/1, 2))
            dictVaule = {
                "interface_code": interface,
                "index_code": "TIME_CONSUMING",
                "link_name": "OSB",
                "city_code": code,
                "start_time": datetime.datetime.strptime(data['start'], '%Y-%m-%d %H:%M:%S'),
                "end_time": datetime.datetime.strptime(data['end'], '%Y-%m-%d %H:%M:%S'),
                "success_nums": 0.0,
                "business_fail_nums": 0.0,
                "system_fail_nums": 0.0,
                "index_vaule": successRate,
                "create_time": createTime
            }
            yield dictVaule
    except Exception as error:
        raise error


# 基础信息获取
def exce():
    try:
        global url
        interval = None
        start = None
        end = None
        dataType = None
        esbs = None
        data = {
            "access_token": "cboss_bussiness_group",
            "tags": {
                "region_id": "ALL",
                "sys_op_id": "20140749"
            }
        }
        if "URL" in conDict:
            url = conDict['URL']
        else:
            url = "http://10.76.226.222:9016/datashsnc/api/query"
        # if "METRIC" in conDict:
        #     dataType = conDict['METRIC']
        # else:
        #     dataType = "ESB_Data_Cost"
        dataType = "ESB_Data_Cost"
        # if "INTERVAL" in conDict:
        #     interval = int(conDict['INTERVAL'])
        # else:
        #     interval = 5
        interval = 1
        if "DATA" in conDict:
            esbs = conDict['DATA']
        else:
            raise Exception("接口信息没有配置")
        esbs = eval(esbs)
        nowData = datetime.datetime.now()
        start = (nowData - datetime.timedelta(minutes=interval)).strftime("%Y-%m-%d %H:%M:00")
        end = nowData.strftime("%Y-%m-%d %H:%M:00")
        data['start'] = start
        data['end'] = end
        data['metric'] = dataType
        for esb in esbs:
            data['tags']['esbname'] = esb['esb']
            yield from regionExecute(data, esb['interface'])
    except Exception as error:
        raise error


# 数据写入数据库
def write():
    try:
        lists = []
        month = datetime.datetime.now().strftime("%Y%m")
        table = "bomc.cboss_interface_data_" + month
        for func in exce():
            lists.append(func)
        sql, vauleList = ora.batchInsertSql(table, lists, "backTable")
        ora.batchExec("backTable", sql, vauleList)
    except Exception as error:
        logging.error(error)
        ora.dataRollback()
    else:
        ora.dataCommit()


if __name__ == '__main__':
    # 程序配置在cboss.python_config_cboss表中
    try:
        config = Config("EsbInterFaceDate")
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()
        write()
    except Exception as error:
        logging.error(error)
        sys.exit(1)
    else:
        sys.exit(0)