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

url = "http://10.76.226.222:9016/datashsnc/api/query"
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
        logging.error(error)


# 统计数据
def sums(vaules):
    if len(vaules) != 0:
        return sum([float(vaule['value']) for vaule in vaules])
    else:
        return 0.0


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

            # ----------------------------------------------调用量--------------------------------------------------
            # ----------------------------------
            data['metric'] = "ESB_Data_count"
            # 成功量
            data['tags']['code'] = "0"
            content = getInterface(url, data, headers)
            content = json.loads(content)['results']
            successNums = sums(content)

            # 业务失败
            data['tags']['code'] = "1"
            content = getInterface(url, data, headers)
            content = json.loads(content)['results']
            businessFailNums = sums(content)

            # 系统失败
            data['tags']['code'] = "2"
            content = getInterface(url, data, headers)
            content = json.loads(content)['results']
            systemFailNums = sums(content)

            contNums = successNums + businessFailNums
            if contNums == 0:
                successRate = '100.0'
            else:
                successRate = str(round((successNums / contNums), 4) * 100)
            # ----------------------------------
            # ----------------------------------------------调用量-------------------------------------------------

            # ----------------------------------------------耗时--------------------------------------------------
            # ----------------------------------
            # 耗时
            data['metric'] = "ESB_Data_Cost"
            data['tags']['code'] = "0"
            content = getInterface(url, data, headers)
            content = json.loads(content)['results']
            timeConsuming = sums(content)
            # ----------------------------------
            # ----------------------------------------------耗时--------------------------------------------------

            dictVaule = {
                "interface_code": interface,
                "city_code": code,
                "start_time": datetime.datetime.strptime(data['start'], '%Y-%m-%d %H:%M:%S'),
                "end_time": datetime.datetime.strptime(data['end'], '%Y-%m-%d %H:%M:%S'),
                "success_nums": successNums,
                "business_fail_nums": businessFailNums,
                "system_fail_nums": systemFailNums,
                "success_rate": successRate,
                "time_consuming": timeConsuming,
                "create_time": createTime
            }
            yield dictVaule
    except Exception as error:
        raise error


# 基础信息获取
def exce():
    try:
        esbs = None
        data = {
            "access_token": "cboss_bussiness_group",
            "tags": {
                "region_id": "ALL",
                "sys_op_id": "20140749"
            }
        }
        if "DATA" in conDict:
            esbs = conDict['DATA']
        else:
            raise Exception("接口信息没有配置")
        esbs = eval(esbs)
        nowData = datetime.datetime.now()
        start = (nowData - datetime.timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:00")
        end = (nowData - datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:00")
        data['start'] = start
        data['end'] = end

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
        table = "bomc.oaoview_data_osb_" + month
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
    config = Config("EsbInterFaceDate")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    write()
