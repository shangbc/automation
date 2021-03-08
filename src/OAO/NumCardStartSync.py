# Time     2021/02/02 08::48
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 号码回收错误表重处理
from ProgramConfigurationOnly import Config
from pandas._libs.tslibs.timestamps import Timestamp as timestamp
import sys
import datetime


# 获取需要重处理的数据
def reprocessing():
    try:
        # 获取错误表中的需要重处理的数据
        selectSql = "select a.* from party.num_card_state_sync_err a"
        syncErrDatas = ora.selectSqlExecute("party", selectSql)
        if len(syncErrDatas) == 0:
            logging.info("当前没有需要重处理的数据")
            return []

        # 获取当前时间
        time = timestamp.now()

        # 创建时间修改为当前
        for syncErrData in syncErrDatas:
            syncErrData['CREATE_DATE'] = time
            syncErrData.pop('ERR_DATE')
            syncErrData.pop('ERR_MSG')

        return syncErrDatas

    except Exception as errors:
        raise errors


# 数据转移到当前表
def reprocessingNow(datas):
    try:
        # 数据转移当前表
        insertSql, insertDates = ora.batchInsertSql("party.num_card_state_sync", datas, "party")
        ora.batchInsert("party", insertSql, insertDates)

        # 备份数据
        insertSql, insertDates = ora.batchInsertSql("party.num_card_state_sync_err_back", datas, "party")
        ora.batchInsert("party", insertSql, insertDates)

        lists = []
        # 删除失败表中的数据
        for data in datas:
            deleteDict = {
                "billId": data['BILL_ID']
            }
            lists.append(deleteDict)

        deleteSql = "delete from party.num_card_state_sync_err t where t.bill_id = :billId"
        ora.batchExec("party", deleteSql, lists)

    except Exception as errors:
        ora.dataRollback()
        raise errors
    else:
        # ora.dataRollback()
        ora.dataCommit()
        logging.info("错误表数据删除成功")


def reprocessingHis():
    try:
        # 获取备份表中的数据
        selectSql = "select a.* from party.num_card_state_sync_err_back a"
        syncErrBackDatas = ora.selectSqlExecute("party", selectSql)
        if len(syncErrBackDatas) == 0:
            logging.info("当前没有需要重处理的数据")
            return

        #
        moth = datetime.datetime.now().strftime("%Y%m")
        for syncErrBackData in syncErrBackDatas:
            hisDict = {
                "billId": syncErrBackData['bill_id'],
                "moth": moth
            }
            selectSql = """
                        select a.*
                        from party.num_card_state_sync_f_[:moth] a
                        where a.bill_id = '[:billId]'
                              and a.upload_flag = '0'
                        """
            data = ora.selectSqlExecute("party", selectSql, hisDict)

            # 处理成功，转移到历史表之后
            if len(data) > 0:
                try:
                    updateSql = """
                                update party.num_card_state_sync_f_[:moth] t
                                set t.ord_type = '1'
                                where t.bill_id = '[:billId]'
                                      and t.upload_flag = '0'
                                """
                    ora.sqlExecute("party", updateSql, hisDict)

                    deleteSql = """
                                delete from party.num_card_state_sync_err_back t
                                where t.bill_id = '[:billId]'
                                """
                    ora.sqlExecute("party", deleteSql, hisDict)
                except Exception as errors:
                    ora.dataRollback()
                    raise errors
                else:
                    ora.dataCommit()

            # 处理失败回到错误表之后
            selectSql = """
                        select a.*
                        from party.num_card_state_sync_err a
                        where a.bill_id = '[:billId]'
                        """
            data = ora.selectSqlExecute("party", selectSql, hisDict)
            if len(data) > 0:
                try:
                    deleteSql = """
                                delete from party.num_card_state_sync_err_back t
                                where t.bill_id = '[:billId]'
                                """
                    ora.sqlExecute("party", deleteSql, hisDict)
                except Exception as errors:
                    ora.dataRollback()
                    raise errors
                else:
                    ora.dataCommit()

    except Exception as errors:
        raise errors


def exceFunc():
    try:
        reprocessingHis()
        syncErrData = reprocessing()
        if len(syncErrData) > 0:
            reprocessingNow(syncErrData)
    except Exception as errors:
        raise errors


if __name__ == '__main__':
    try:
        # NumCardStartSync
        config = Config("NumCardStartSync")
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()
        exceFunc()
    except Exception as error:
        logging.error(error)
        sys.exit(1)
    else:
        logging.info("程序结束")
        sys.exit(0)
