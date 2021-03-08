# Time     2021/02/22 11::09
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 获取实例过载接口返回的信息，并且将状态、活动线程数和实占线程数和并成一条数据插入数据库中

import requests
import json
import datetime
import pandas
import sys
from ProgramConfigurationOnly import Config

# 设置请求头
headers = {
    "User - Agent": "Mozilla / 5.0(Windows NT 10.0;Win64;x64;rv: 68.0) Gecko / 20100101 Firefox / 68.0",
    "Content-Type": "application/json;charset=utf-8"
}

value = {
    "wujian_wls_state": "example_start",
    "wujian_wls_thread_count": "example_thread_count",
    "wujian_wls_hogging_thread": "example_hogging_thread"
}


# 获取连接
def getRequestContent(url, data):
    try:
        # 设置连接信息
        requestsContent = requests.post(url=url, data=json.dumps(data), headers=headers)
        # 提取接口返回的数据
        requestsTextStr = requestsContent.text
        # 返回的是一个字符串的数据,转换为字典类型
        requestsTextJson = json.loads(requestsTextStr)
        return requestsTextJson
    except Exception as error:
        raise error


# 获取数据
def getData(metric):
    url = "http://10.76.226.222:9016/datashsnc/api/query"
    nowTime = datetime .datetime .now()
    endTime = (nowTime - datetime .timedelta(minutes=1)) .strftime('%Y-%m-%d %H:%M:00')
    startTime = (nowTime - datetime .timedelta(minutes=2)) .strftime('%Y-%m-%d %H:%M:00')

    # wujian_wls_state               weblgic实例状态（0：down  1：notice  2： 3：ok）
    # wujian_wls_thread_count        weblgic实例活动线程数
    # wujian_wls_hogging_thread      weblgic实例独占线程数
    # metric = ['wujian_wls_state', 'wujian_wls_thread_count', 'wujian_wls_hogging_thread']
    data = {
        "access_token": "cboss_bussiness_group",
        "metric": metric,
        "start": startTime,
        "end": endTime,
        "tags": {
            "systemName": "*",
            "moduleName": "*",
            "groupName": "*",
            "ip": "*",  # ip
            "name": "*",  # 实例名
            "port": "*"  # 端口
        }
    }

    # 调用接口
    jsonText = getRequestContent(url, data)

    # 对返回的数据重新处理
    dataList = []
    for resultsJson in jsonText['results']:
        tagJson = json .loads(resultsJson['host_tag'])
        # 与表字段保持一致，方便插表
        dictJson = {
            "system_name": tagJson['systemName'],
            "group_name": tagJson['groupName'],
            "module_name": tagJson['moduleName'],
            "example_name": tagJson['name'],
            "ip": tagJson['ip'],
            "port": tagJson['port'],
            value[metric]: int(resultsJson['value']),
            "create_time": datetime .datetime .strptime(resultsJson['time'], '%Y-%m-%d %H:%M:%S'),
        }
        dataList .append(dictJson)
    return dataList


# 数据合并
def dataFrameMerge(data1, data2):
    # 转换为数据表
    dataFrame1 = pandas .DataFrame(data1)
    dataFrame2 = pandas .DataFrame(data2)

    # 数据合并
    dataFrame = pandas .merge(dataFrame1, dataFrame2, on=["system_name", "group_name", "module_name", "example_name",
                                                          "ip", "port", "create_time"])

    # 重新转换为dict类型
    dataFrame = dataFrame .to_dict("records")
    return dataFrame


# 接口数据入库
def dataWarehousing():
    try:
        # 接口返回的数据
        # wujian_wls_state               weblgic实例状态（0：down  1：notice  2： 3：ok）
        # wujian_wls_thread_count        weblgic实例活动线程数
        # datas = getData()
        datas1 = getData("wujian_wls_state")
        datas2 = getData("wujian_wls_thread_count")
        datas3 = getData("wujian_wls_hogging_thread")

        if len(datas1) == 0 or len(datas2) == 0 or len(datas3) == 0:
            raise Exception("当前数据集为空，退出本次执行！")

        datas = dataFrameMerge(datas1, datas2)
        datas = dataFrameMerge(datas, datas3)

        if len(datas) == 0:
            raise Exception("当前接口返回的数据为空！")

        # 批量获取序列
        seqDict = {
            "seq": str(len(datas))
        }
        seqSql = """
                 select cboss.seq_sc_example.nextval as seq
                 from (select 1 from all_objects where rownum <= [:seq])
                 """
        seqSql = ora .strFormat(seqSql, seqDict)
        sqlIndex = ora .selectSqlExecute("cboss", seqSql)

        # 接口数据赋值唯一的seq
        index = 0
        for data in datas:
            data['seq_index'] = sqlIndex[index]['seq']
            index += 1

        # 接口数据入库
        tableName = "cboss.example_overload_" + datetime .datetime .now() .strftime("%Y%m")
        insertSql, insertDate = ora .batchInsertSql(tableName, datas)
        ora .batchInsert("cboss", insertSql, insertDate)
    except Exception as error:
        ora .dataRollback()
        raise error
    else:
        ora .dataCommit()


if __name__ == '__main__':

    try:
        # 程序配置在cboss.python_config_cboss表中
        config = Config("DefaultDataSourceCongfig")
        staticConfig = config .getStaticConfig()
        logging = config .getLogging()
        ora = config .getDataSource()
        conDict = staticConfig .getKeyDict()

        # 调用主程序
        dataWarehousing()
    except Exception as error:
        logging .error(error)
        sys.exit(1)
    else:
        logging .info("程序结束")
        sys .exit(0)
