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


# 线性回归
def rateFunc(data, business, businessRate):
    data = pandas.DataFrame(data)
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


def timeFunc(data, business, consuming):
    data = pandas.DataFrame(data)
    # 第二步，画出散点图，求x和y的相关系数
    plt.scatter(data.NUMS, data.TIME_CONSUMING)

    data.corr()

    # 第三步，估计模型参数，建立回归模型
    lrModel = LinearRegression()

    x = data[['NUMS']]
    y = data[['TIME_CONSUMING']]

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

    # 设置浮动值
    k = 100.0
    state = 0
    rate = (abs(rate) + k) if (abs(rate) + k) < 5000 else 5000

    # 判断数据状态
    if abs(rate) + k < consuming:
        state = 1
    return state, abs(rate) + k


# 异常数据分析
def rateOperation(interfaceCode):
    # 提取数据
    sqlDict = {
        "interfaceCode": interfaceCode,
        "yyyymm": datetime.datetime.now().strftime("%Y%m"),
    }
    sql = """
          select * from bomc.oaoview_data_osb_[:yyyymm] t
          where t.interface_code = '[:interfaceCode]'
                and t.create_time < trunc(sysdate,'mi')
                and t.create_time >= trunc(sysdate) - 2
          order by t.create_time desc
          """

    vauleList = ora.selectSqlExecute("backTable", sql, sqlDict)

    # 成功率100%的为正常  直接返回
    if float(vauleList[0]['success_rate']) >= 100:
        dictData = vauleList[0]
        dictData['INDEX_VAULE'] = 100
        dictData['INDEX_CODE'] = '成功率'
        dictData['INDEX_LINK'] = 'OSB'
        dictData['INDEX_LABEL'] = 0
        dictData['INTERFACE_VAULE'] = dictData['SUCCESS_RATE']
        dictData.pop('SUCCESS_NUMS')
        dictData.pop('BUSINESS_FAIL_NUMS')
        dictData.pop('SYSTEM_FAIL_NUMS')
        dictData.pop('TIME_CONSUMING')
        dictData.pop('SUCCESS_RATE')
        return dictData

    # 成功率低于100%时分析是否远远低于以往的数值
    # 由于业务量的不同导致趋势不一致，根据业务量分为两部分
    # x为业务量0~10的数据   y为业务量10以上的数据
    x, y = [], []
    for vaule in vauleList:
        if float(vaule['success_rate']) >= 100:
            continue
        business = int(vaule['SUCCESS_NUMS'] + vaule['BUSINESS_FAIL_NUMS'])
        dictVaule = {
            "NUMS": business,
            "SUCCESS_RATE": vaule['success_rate']
        }
        if business <= 10:
            x.append(dictVaule)
        else:
            y.append(dictVaule)

    # 指标低于阈值是判断是否是连续的
    index = 0
    state = 0
    indexVaule = 0.0
    for vaule in vauleList[0:6]:
        states = 0
        business = int(vaule['SUCCESS_NUMS'] + vaule['BUSINESS_FAIL_NUMS'])
        businessRate = vaule['success_rate']
        parameter = None

        # 根据业务量采用不同的训练数据
        if business <= 10:
            parameter = x
        else:
            parameter = y

        # 返回当前数据的状态和阈值
        if index == 0:
            states, indexVaule = rateFunc(parameter, business, businessRate)
        else:
            states = rateFunc(y, business, businessRate)[0]

        # 当连续的数据集中有正常的数据时退出循环
        if states == 0:
            break
        else:
            index += 1

    # 根据异常数据的数量 来判断当前是否异常
    if index < 3:
        state = 0
    elif index >= 3 and index < 5:
        state = 1
    else:
        state = 2

    dictData = vauleList[0]
    dictData['INDEX_LINK'] = 'OSB'
    dictData['INDEX_CODE'] = '成功率'
    dictData['INDEX_VAULE'] = round(indexVaule[0], 2)
    dictData['INDEX_LABEL'] = state
    dictData['INTERFACE_VAULE'] = dictData['SUCCESS_RATE']
    dictData.pop('success_nums')
    dictData.pop('BUSINESS_FAIL_NUMS')
    dictData.pop('SYSTEM_FAIL_NUMS')
    dictData.pop('TIME_CONSUMING')
    dictData.pop('SUCCESS_RATE')
    return dictData


def timeOperation(interfaceCode):
    sqlDict = {
        "interfaceCode": interfaceCode,
        "yyyymm": datetime.datetime.now().strftime("%Y%m")
    }
    sql = """
          select * from bomc.oaoview_data_osb_[:yyyymm] t
          where t.interface_code = '[:interfaceCode]'
                and t.create_time >= trunc(sysdate) - 2
          order by t.create_time desc
          """

    vauleList = ora.selectSqlExecute("backTable", sql, sqlDict)

    x = []
    for vaule in vauleList:
        # if float(vaule['success_rate']) >= 100:
        #     continue
        business = int(vaule['SUCCESS_NUMS']+vaule['BUSINESS_FAIL_NUMS'])
        dictVaule = {
            "NUMS": business,
            "TIME_CONSUMING": vaule['TIME_CONSUMING']
        }
        x.append(dictVaule)

    index = 0
    state = 0
    indexVaule = 0.0
    for vaule in vauleList[0:6]:
        states = 0
        business = int(vaule['SUCCESS_NUMS']+vaule['BUSINESS_FAIL_NUMS'])
        businessRate = vaule['TIME_CONSUMING']

        if index == 0:
            states, indexVaule = timeFunc(x, business, businessRate)
        else:
            states = timeFunc(x, business, businessRate)[0]

        if states == 0:
            break
        else:
            index += 1
    if index < 3:
        state = 0
    elif index >= 3 and index < 5:
        state = 1
    else:
        state = 2
    dictData = vauleList[0]
    dictData['INDEX_LINK'] = 'OSB'
    dictData['INDEX_CODE'] = '耗时'
    dictData['INDEX_VAULE'] = round(indexVaule[0], 2)
    dictData['INDEX_LABEL'] = state
    dictData['INTERFACE_VAULE'] = dictData['TIME_CONSUMING']
    dictData.pop('success_nums')
    dictData.pop('BUSINESS_FAIL_NUMS')
    dictData.pop('SYSTEM_FAIL_NUMS')
    dictData.pop('TIME_CONSUMING')
    dictData.pop('SUCCESS_RATE')
    return dictData


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
            a = rateOperation(esb['interface'])
            b = timeOperation(esb['interface'])
            aa.append(a)
            aa.append(b)
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
