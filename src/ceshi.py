import re
import datetime
if __name__ == '__main__':
    time = nowDate = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")
    print(time)