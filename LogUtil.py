import DbutilOnly
import time
import logging


# @Time    2020/09/10 11:33:00
# Author   shangxb
# Version   1.0
# function 记录执行的信息
class LogUtil:
    def __init__(self):
        # 创建需要用到的数据源实例
        pdb = Dbutil.OraclePolls({"aisr": "crmkfdb_prod"})
        logTime = time.strftime("%Y%m%d%H%M%S", time.localtime())
        self.pdb = pdb
        self.logTime = logTime
        self.num = 0

    # @Time    2020/09/10 11:33:00
    # Author   shangxb
    # Version   1.1
    # function 记录执行的信息
    #           jobName    存储过程的名称
    #           stepName   任务执行的位置
    #           taskStatus 0 中间 ，1 开始，2 结束 3异常的中间过程
    #           taskLog    正在执行的步骤
    # update    2020/10/09 17:00:00     新增done_date字段记录执行的时间
    def logInsert(self, jobName, taskPos, taskStatus, errMsg='', taskLog='', doneDate='', rowCount=''):
        if self.num < 10:
            logTimenum = self.logTime + "-0" + str(self.num)
        else:
            logTimenum = self.logTime + "-" + str(self.num)
        v_sql = "insert into aisr.t_run_log_sc(logid ,task_date, task_name, start_time, task_pos, task_status," \
                "err_msg, task_log, done_date, row_count) " \
                "values('" + self.logTime + "','" + logTimenum + "','" + jobName + "',sysdate,'" + taskPos + "','" \
                                                                                                             "" + taskStatus + "','" + errMsg + "','" + taskLog + "','" + doneDate + "','" + str(
            rowCount) + "')"
        self.num += 1
        self.pdb.sqlExecute("aisr", v_sql)
        self.pdb.dataCommit()


def resetLogLevel():
    root_logger = logging.getLogger()
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s \n - %(message)s',
                        level=logging.WARNING)
