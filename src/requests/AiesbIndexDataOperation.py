# Time     2021/01/20 16::56
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 默认
from ProgramConfigurationOnly import Config
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
import pandas
import numpy
import datetime
import copy


# 线性回归
def rateFuncs(data, business, businessRate):
    data = pandas .DataFrame(data)
    # 第二步，画出散点图，求x和y的相关系数
    plt.scatter(data.NUMS, data.SUCCESS_RATE)

    data.corr()

    # 第三步，估计模型参数，建立回归模型
    lrModel = LinearRegression()

    x = data[['NUMS']]
    y = data[['SUCCESS_RATE']]

    # 训练模型
    lrModel.fit(x, y)

    # 第四步、对回归模型进行检验
    lrModel.score(x, y)

    # 第五步、利用回归模型进行预测
    lrModel.predict([[int(business)]])

    # 查看截距
    alpha = lrModel.intercept_[0]

    # 查看参数
    beta = lrModel.coef_[0][0]

    # plt.show()
    rate = alpha + beta * numpy.array([int(business)])
    rate = rate if rate <= 100 else 100.0

    # 设置浮动值
    k = 0.0
    if business > 10:
        k = 2.5
    else:
        k = 5
    state = 0

    # 判断数据状态
    if rate - k > businessRate:
        state = 1
    return state, rate - k


def reatFunc(interfaceCode):
    for i in range(569, 581):
        try:
            state = 0
            sqlDict = {
                "interfaceCode": interfaceCode,
                "yyyymm": datetime.datetime.now().strftime("%Y%m"),
                "cityCode": "ALL" if i == 569 else str(i)
            }
            sql = """
                  select * from bomc.cboss_interface_data_[:yyyymm] t
                  where t.interface_code = '[:interfaceCode]'
                        and t.link_name = 'AIESB'
                        and t.index_code = '成功率'
                        and t.city_code = '[:cityCode]'
                        and t.create_time < trunc(sysdate,'mi')
                        and t.create_time >= trunc(sysdate) - 1
                  order by t.create_time desc
                  """
            listVaule = ora.selectSqlExecute("backTable", sql, sqlDict)

            if len(listVaule) == 0:
                continue
            datas = copy.deepcopy(listVaule[0])

            # 成功率100%的为正常  直接返回
            if float(datas['INDEX_VAULE']) >= 100:
                dictData = datas
                dictData['INDEX_LINK'] = dictData['LINK_NAME']
                dictData['INDEX_CODE'] = '成功率'
                dictData['INTERFACE_VAULE'] = dictData['INDEX_VAULE']
                dictData['INDEX_VAULE'] = 100
                dictData['INDEX_LABEL'] = state
                dictData.pop('success_nums')
                dictData.pop('BUSINESS_FAIL_NUMS')
                dictData.pop('SYSTEM_FAIL_NUMS')
                dictData.pop('LINK_NAME')
                yield dictData
                continue

            x = []
            # 剔除业务量为0的数据
            for vaule in listVaule[::-1]:
                if int(vaule['SUCCESS_NUMS']) == 0 and int(vaule['BUSINESS_FAIL_NUMS']) == 0:
                    listVaule.remove(vaule)
                else:
                    dictVaule = {
                        "SUCCESS_RATE": float(vaule['INDEX_VAULE']),
                        "NUMS": int(vaule['SUCCESS_NUMS'])+int(vaule['BUSINESS_FAIL_NUMS'])
                    }
                    x.append(dictVaule)

            if len(listVaule) == 0:
                dictData = datas
                dictData['INDEX_LINK'] = dictData['LINK_NAME']
                dictData['INDEX_CODE'] = '成功率'
                dictData['INTERFACE_VAULE'] = dictData['INDEX_VAULE']
                dictData['INDEX_VAULE'] = 100
                dictData['INDEX_LABEL'] = state
                dictData.pop('success_nums')
                dictData.pop('BUSINESS_FAIL_NUMS')
                dictData.pop('SYSTEM_FAIL_NUMS')
                dictData.pop('LINK_NAME')
                yield dictData
                continue

            # 判断当前成功率是否高于指标
            business = int(datas['SUCCESS_NUMS']) + int(datas['BUSINESS_FAIL_NUMS'])
            states, indexVaule = rateFuncs(x, business, float(datas['INDEX_VAULE']))

            if states == 0:
                dictData = datas
                dictData['INDEX_LINK'] = dictData['LINK_NAME']
                dictData['INDEX_CODE'] = '成功率'
                dictData['INTERFACE_VAULE'] = dictData['INDEX_VAULE']
                dictData['INDEX_VAULE'] = round(indexVaule[0], 2)
                dictData['INDEX_LABEL'] = state
                dictData.pop('success_nums')
                dictData.pop('BUSINESS_FAIL_NUMS')
                dictData.pop('SYSTEM_FAIL_NUMS')
                dictData.pop('LINK_NAME')
                yield dictData
                continue

            # 判断是否连续异常
            index = 0


            for vaule in listVaule[0:5]:
                business = int(vaule['SUCCESS_NUMS']) + int(vaule['BUSINESS_FAIL_NUMS'])
                businessRate = float(vaule['INDEX_VAULE'])

                states = rateFuncs(x, business, businessRate)[0]

                if states == 0:
                    break
                else:
                    index += 1

            # 判断展示状态
            if index < 3:
                state = 0
            elif index >= 3 and index < 5:
                state = 1
            else:
                state = 2
            dictData = datas
            dictData['INDEX_LINK'] = dictData['LINK_NAME']
            dictData['INDEX_CODE'] = '成功率'
            dictData['INTERFACE_VAULE'] = dictData['INDEX_VAULE']
            dictData['INDEX_VAULE'] = round(indexVaule[0], 2)
            dictData['INDEX_LABEL'] = state
            dictData.pop('success_nums')
            dictData.pop('BUSINESS_FAIL_NUMS')
            dictData.pop('SYSTEM_FAIL_NUMS')
            dictData.pop('LINK_NAME')

            yield dictData
        except Exception as error:
            logging.error(error)
            dictData = datas
            dictData['INDEX_LINK'] = 'AIESB'
            dictData['INDEX_CODE'] = '成功率'
            dictData['INTERFACE_VAULE'] = "100"
            dictData['INDEX_VAULE'] = 100
            dictData['INDEX_LABEL'] = 0
            dictData.pop('success_nums')
            dictData.pop('BUSINESS_FAIL_NUMS')
            dictData.pop('SYSTEM_FAIL_NUMS')
            dictData.pop('LINK_NAME')
            yield dictData


def backlogFunc():
    sqlDict = {
        "yyyymm": datetime.datetime.now().strftime("%Y%m")
    }
    sql = """
          select * from bomc.cboss_interface_data_[:yyyymm] t
          where t.index_code in ('积压量','异常量')
                and t.create_time < trunc(sysdate,'mi')
                and t.create_time >= trunc(sysdate-1/1440,'mi')
          order by t.create_time desc
          """
    listVaule = ora.selectSqlExecute("backTable", sql, sqlDict)
    if len(listVaule) == 0:
        return

    for vaule in listVaule:

        try:
            state = 0
            if float(vaule['INDEX_VAULE']) == 0.0:
                state = 0
            else:
                dict = {
                    "link_name": vaule['link_name'],
                    "city_code": vaule['city_code'],
                    "index_code": vaule['index_code'],
                    "interface_code": vaule['interface_code'],
                    "yyyymm":datetime.datetime.now().strftime("%Y%m")
                }
                sql = """
                      select * from bomc.cboss_interface_data_[:yyyymm] t
                      where t.link_name = '[:link_name]'
                            and t.interface_code = '[:interface_code]'
                            and t.city_code = '[:city_code]'
                            and t.index_code = '[:index_code]'
                            and t.index_vaule <> '0'
                            and t.create_time < trunc(sysdate,'mi')
                            and t.create_time >= trunc(sysdate-5/1440,'mi')
                      order by t.create_time desc
                      """
                dataList = ora.selectSqlExecute("backTable", sql, dict)
                num = len(dataList)
                if num <= 3:
                    state = 1
                else:
                    state = 2

            dictData = copy.deepcopy(vaule)
            dictData['INDEX_CODE'] = vaule['index_code']
            dictData['INDEX_LINK'] = dictData['LINK_NAME']
            dictData['INTERFACE_VAULE'] = dictData['INDEX_VAULE']
            dictData['INDEX_VAULE'] = 0
            dictData['INDEX_LABEL'] = state
            dictData.pop('success_nums')
            dictData.pop('BUSINESS_FAIL_NUMS')
            dictData.pop('SYSTEM_FAIL_NUMS')
            dictData.pop('LINK_NAME')
            yield dictData
        except Exception as error:
            logging.error(error)
            dictData = copy.deepcopy(vaule)
            dictData['INDEX_CODE'] = vaule['index_code']
            dictData['INDEX_LINK'] = dictData['LINK_NAME']
            dictData['INTERFACE_VAULE'] = "0"
            dictData['INDEX_VAULE'] = 0
            dictData['INDEX_LABEL'] = 0
            dictData.pop('success_nums')
            dictData.pop('BUSINESS_FAIL_NUMS')
            dictData.pop('SYSTEM_FAIL_NUMS')
            dictData.pop('LINK_NAME')
            yield dictData


def reatOperation(interfaceCode):
    aa = []
    for fun in reatFunc(interfaceCode):
        a = fun
        aa.append(a)
    return aa


def backlogOperation():
    aa = []
    for fun in backlogFunc():
        a = fun
        aa.append(a)
    return aa


# 数据写入数据库
def write():
    try:
        esbs = None
        if "DATA" in conDict:
            esbs = conDict['DATA']
        else:
            raise Exception("接口信息没有配置")
        esbs = eval(esbs)
        aa = []
        for esb in esbs:
            a = reatOperation(esb['interface'])
            aa.extend(a)
        backlog = backlogOperation()
        aa.extend(backlog)
        table = "bomc.OAOVIEW_DATA_LABEL_" + datetime.datetime.now().strftime("%Y%m")
        sql, vauleList = ora.batchInsertSql(table, aa, "backTable")
        ora.batchExec("backTable", sql, vauleList)
    except Exception as error:
        logging.error(error)
        ora.dataRollback()
    else:
        ora.dataCommit()


if __name__ == '__main__':
    config = Config("EsbInterFaceDate")
    staticConfig = config.getStaticConfig()
    logging = config.getLogging()
    ora = config.getDataSource()
    conDict = staticConfig.getKeyDict()
    write()
