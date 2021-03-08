import os #line:1
import threading #line:3
import time #line:5
import configparser #line:7
import cx_Oracle as Oracle #line:8
import pymysql #line:10
import pandas as pd #line:12
from DBUtils .PooledDB import PooledDB #line:14
from multidict import CIMultiDict #line:16
lock =threading .Lock ()#line:18
user_flag =''#line:20
data_flag =''#line:22
DBNAME =''#line:24
import logging #line:26
import sys #line:28
os .chdir (sys .path [0 ])#line:30
time =time .strftime ("%Y-%m-%d",time .localtime ())#line:32
logger =logging .getLogger ()#line:36
logger .setLevel (logging .INFO )#line:40
logfile ='.\log\\'+str (time )+'.log'#line:44
fh =logging .FileHandler (logfile ,mode ='a',encoding ="utf-8")#line:46
fh .setLevel (logging .DEBUG )#line:48
ch =logging .StreamHandler ()#line:52
ch .setLevel (logging .DEBUG )#line:54
formatter =logging .Formatter ('%(asctime)s - %(name)s - %(levelname)s - %(message)s')#line:60
fh .setFormatter (formatter )#line:62
ch .setFormatter (formatter )#line:64
logger .addHandler (fh )#line:68
logger .addHandler (ch )#line:70
def MAPP (OOO0O0000OO0OOO0O ):#line:76
    O0OOOOOOO000O0O00 ='20.26.90.22'#line:77
    OOOOO00000OOO0OO0 =1521 #line:79
    OOO0O0OOO00000O0O ='mapp'#line:81
    OO0OOO0OO00O0O0OO ='OOoo0000'#line:83
    OO0000O0OO0OO0O00 ='PDB_ZB'#line:85
    O0000OO0O0OO0OOO0 =O0OOOOOOO000O0O00 +":"+str (OOOOO00000OOO0OO0 )+"/"+OO0000O0OO0OO0O00 #line:87
    OO00OOO0OOO0O0OOO =PooledDB (creator =Oracle ,user =OOO0O0OOO00000O0O ,password =OO0OOO0OO00O0O0OO ,dsn =O0000OO0O0OO0OOO0 )#line:97
    OO000O00O000OOOOO ='select * from aiselfhealing_db_config where DB_NAME ='+"'"+str (OOO0O0000OO0OOO0O )+"'"#line:101
    OOOO00OOOOOO00O0O =OO00OOO0OOO0O0OOO .connection ()#line:103
    O00O00O000O0OO00O =pd .read_sql (OO000O00O000OOOOO ,OOOO00OOOOOO00O0O )#line:105
    OOO000OO0O0OOOOO0 =O00O00O000O0OO00O .to_dict (orient ="records")#line:107
    O0OOOOOOO000O0O00 =OOO000OO0O0OOOOO0 [0 ]['DB_HOST']#line:109
    OOOOO00000OOO0OO0 =OOO000OO0O0OOOOO0 [0 ]['DB_PORT']#line:111
    OOO0O0OOO00000O0O =OOO000OO0O0OOOOO0 [0 ]['DB_USER']#line:113
    OO0OOO0OO00O0O0OO =OOO000OO0O0OOOOO0 [0 ]['DB_PASSWORD']#line:115
    O0O0OO00O0OO00O0O =OOO000OO0O0OOOOO0 [0 ]['MAXCONN']#line:117
    O0OOOOOOO0O000OO0 =OOO000OO0O0OOOOO0 [0 ]['SERVICE_DATABASE']#line:119
    return O0OOOOOOO000O0O00 ,OOOOO00000OOO0OO0 ,OOO0O0OOO00000O0O ,OO0OOO0OO00O0O0OO ,O0OOOOOOO0O000OO0 ,O0O0OO00O0OO00O0O #line:121
class DadbPool (object ):#line:125
    try :#line:126
        def __init__ (OOOOO0O0O0OO00OOO ,OO000000O0OOO0OO0 ):#line:128
            OOOO00OOO0OOOOOOO ,OO000OO0OO0OO00OO ,O00000OOO0000O0OO ,OOO0O0OO0OOOO0000 ,OO000O0O00OOO00OO ,O0OOOO0000OOO000O =MAPP (OO000000O0OOO0OO0 )#line:132
            O00OOOO00O00000O0 =OOOO00OOO0OOOOOOO +":"+str (OO000OO0OO0OO00OO )+"/"+OO000O0O00OOO00OO #line:144
            O0OO0000OO000O000 =OO000000O0OOO0OO0 #line:146
            OOOOO0O0O0OO00OOO .pool =PooledDB (creator =pymysql ,maxconnections =None ,mincached =2 ,maxcached =5 ,maxshared =3 ,blocking =True ,maxusage =None ,setsession =[],ping =0 ,host =OOOO00OOO0OOOOOOO ,port =int (OO000OO0OO0OO00OO ),user =decrypt (O00000OOO0000O0OO ),password =decrypt (OOO0O0OO0OOOO0000 ),database =OO000O0O00OOO00OO ,charset ='UTF8')#line:184
            OOOOO0O0O0OO00OOO .conn =None #line:188
    except Exception as e :#line:192
        print (e )#line:194
    def get_conn (OOO0000OO0O00OO00 ):#line:196
        ""#line:204
        OOO0000OO0O00OO00 .conn =OOO0000OO0O00OO00 .pool .connection ()#line:206
        OOO0000OO0O00OO00 .cursor =OOO0000OO0O00OO00 .conn .cursor (pymysql .cursors .DictCursor )#line:208
    @staticmethod #line:210
    def __OO00O000OOO00O00O (O0O0OOO0OO0OO00O0 ):#line:211
        ""#line:219
        O0O0OOO0OO0OO00O0 .cursor .close ()#line:221
        O0O0OOO0OO0OO00O0 .conn .close ()#line:223
    def execute (O0O0O0OOOOO0OOOOO ,O0O00O000000O0OOO ,*O0OOOO0OOOO0OO000 ):#line:225
        if not O0O0O0OOOOO0OOOOO .conn :#line:227
            O0O0O0OOOOO0OOOOO .get_conn ()#line:228
        if O0OOOO0OOOO0OO000 :#line:230
            O0O0O0OOOOO0OOOOO .cursor .execute (O0O00O000000O0OOO ,O0OOOO0OOOO0OO000 )#line:234
        else :#line:238
            O0O0O0OOOOO0OOOOO .cursor .execute (O0O00O000000O0OOO )#line:242
    def executemany (OO000OOO0OO0O0000 ,OO0O0000000OOOO00 ,O0O00OOOOOO000OOO ):#line:246
        if not OO000OOO0OO0O0000 .conn :#line:248
            OO000OOO0OO0O0000 .get_conn ()#line:249
        if O0O00OOOOOO000OOO :#line:251
            OO000OOO0OO0O0000 .cursor .executemany (OO0O0000000OOOO00 ,O0O00OOOOOO000OOO )#line:255
        else :#line:259
            OO000OOO0OO0O0000 .cursor .executemany (OO0O0000000OOOO00 )#line:263
    def back_up (OOOO0O000O00000OO ,OOO000O0OOO00000O ,*O0O000O0O00OO000O ):#line:267
        ""#line:279
        O0OO00O0OO00OOO0O =OOO000O0OOO00000O .strip (' ').split (' ',1 )[0 ]#line:281
        try :#line:283
            logging .info (OOO000O0OOO00000O +'执行中')#line:285
            if O0OO00O0OO00OOO0O =='create':#line:287
                OOOO0O000O00000OO .execute (OOO000O0OOO00000O ,*O0O000O0O00OO000O )#line:289
            elif O0OO00O0OO00OOO0O =='insert':#line:291
                OOOO0O000O00000OO .execute (OOO000O0OOO00000O ,*O0O000O0O00OO000O )#line:293
            else :#line:295
                logging .info (str (OOO000O0OOO00000O )+"-----非备份语句，请确认语句是否正确")#line:297
                exit (1 )#line:299
            logging .info (OOO000O0OOO00000O +'执行成功')#line:301
            return OOO000O0OOO00000O +'执行成功'#line:303
        except Exception as OO0O0O0OOO000OO00 :#line:305
            logging .warning (str (OOO000O0OOO00000O )+"执行失败，错误原因:"+str (OO0O0O0OOO000OO00 ))#line:307
            return False #line:309
    def select (OOOOOOO000O0OOO0O ,OO0O0OO00O0OOOOOO ,*OO0OOO0OO0OOOOO0O ):#line:311
        ""#line:323
        global df #line:325
        try :#line:327
            logging .info (OO0O0OO00O0OOOOOO +'执行中')#line:329
            OO00O000O00O00O00 =OO0O0OO00O0OOOOOO .strip (' ').split (' ',1 )[0 ]#line:331
            if OO00O000O00O00O00 =='select':#line:333
                OOOOOOO000O0OOO0O .execute (OO0O0OO00O0OOOOOO ,*OO0OOO0OO0OOOOO0O )#line:335
                O00OO00O0OO0O0000 =OOOOOOO000O0OOO0O .cursor .fetchall ()#line:337
                OOOOO000OO0O00OO0 =[CIMultiDict (O0OO0OOOO00O00O00 )for O0OO0OOOO00O00O00 in O00OO00O0OO0O0000 ]#line:339
            else :#line:343
                logging .warning (OO0O0OO00O0OOOOOO +":非select语句，请确认语句是否正确")#line:345
                exit (1 )#line:347
            logging .info (OO0O0OO00O0OOOOOO +'执行成功')#line:349
            return OOOOO000OO0O00OO0 #line:351
        except Exception as O00OO0OO0O000000O :#line:355
            logging .warning (str (OO0O0OO00O0OOOOOO )+"执行失败，错误原因:"+str (O00OO0OO0O000000O ))#line:357
            return False #line:359
    def execute_sql (O00O0O0O00O0O00O0 ,OO000O0O0OO0O0OOO ,*O0OOOO0OOO000OOO0 ):#line:361
        ""#line:373
        try :#line:375
            logging .info (OO000O0O0OO0O0OOO +'执行中')#line:377
            O0O0OOOO00O0000OO =OO000O0O0OO0O0OOO .strip (' ').split (' ',1 )[0 ]#line:379
            if O0O0OOOO00O0000OO =='insert':#line:381
                O00O0O0O00O0O00O0 .execute (OO000O0O0OO0O0OOO ,*O0OOOO0OOO000OOO0 )#line:383
            elif O0O0OOOO00O0000OO =='update':#line:385
                O00O0O0O00O0O00O0 .execute (OO000O0O0OO0O0OOO ,*O0OOOO0OOO000OOO0 )#line:387
            elif O0O0OOOO00O0000OO =='delete':#line:389
                O00O0O0O00O0O00O0 .execute (OO000O0O0OO0O0OOO ,*O0OOOO0OOO000OOO0 )#line:391
            elif O0O0OOOO00O0000OO =='drop':#line:393
                O00O0O0O00O0O00O0 .execute (OO000O0O0OO0O0OOO ,*O0OOOO0OOO000OOO0 )#line:395
            elif O0O0OOOO00O0000OO =='create':#line:397
                O00O0O0O00O0O00O0 .execute (OO000O0O0OO0O0OOO ,*O0OOOO0OOO000OOO0 )#line:399
            elif O0O0OOOO00O0000OO =='truncate':#line:401
                O00O0O0O00O0O00O0 .execute (OO000O0O0OO0O0OOO ,*O0OOOO0OOO000OOO0 )#line:403
            elif O0O0OOOO00O0000OO =='alter':#line:404
                O00O0O0O00O0O00O0 .execute (OO000O0O0OO0O0OOO ,*O0OOOO0OOO000OOO0 )#line:406
            else :#line:407
                logging .warning (OO000O0O0OO0O0OOO +":非修数据语句，请核对后重试")#line:409
                exit (1 )#line:411
            logging .info (OO000O0O0OO0O0OOO +'执行成功')#line:413
            return OO000O0O0OO0O0OOO +'执行成功'#line:415
        except Exception as O000OOO0OOO0OO0O0 :#line:417
            logging .warning (str (OO000O0O0OO0O0OOO )+"执行失败，错误原因:"+str (O000OOO0OOO0OO0O0 ))#line:419
            return False #line:421
    def execute_sql_many (OO000O0OO0OOOO00O ,O0OOOOO00000OO00O ,O000O0O00O0OOO000 ):#line:423
        ""#line:435
        try :#line:437
            logging .info (O0OOOOO00000OO00O +'执行中')#line:439
            O0O0OO0OOOO00OOOO =O0OOOOO00000OO00O .strip (' ').split (' ',1 )[0 ]#line:441
            if O0O0OO0OOOO00OOOO =='insert':#line:443
                OO000O0OO0OOOO00O .executemany (O0OOOOO00000OO00O ,O000O0O00O0OOO000 )#line:445
            elif O0O0OO0OOOO00OOOO =='update':#line:447
                OO000O0OO0OOOO00O .executemany (O0OOOOO00000OO00O ,O000O0O00O0OOO000 )#line:449
            elif O0O0OO0OOOO00OOOO =='delete':#line:451
                OO000O0OO0OOOO00O .executemany (O0OOOOO00000OO00O ,O000O0O00O0OOO000 )#line:453
            elif O0O0OO0OOOO00OOOO =='drop':#line:455
                OO000O0OO0OOOO00O .executemany (O0OOOOO00000OO00O ,O000O0O00O0OOO000 )#line:457
            elif O0O0OO0OOOO00OOOO =='create':#line:459
                OO000O0OO0OOOO00O .executemany (O0OOOOO00000OO00O ,O000O0O00O0OOO000 )#line:461
            elif O0O0OO0OOOO00OOOO =='truncate':#line:463
                OO000O0OO0OOOO00O .executemany (O0OOOOO00000OO00O ,O000O0O00O0OOO000 )#line:465
            elif O0O0OO0OOOO00OOOO =='alter':#line:466
                OO000O0OO0OOOO00O .executemany (O0OOOOO00000OO00O ,O000O0O00O0OOO000 )#line:468
            else :#line:469
                logging .warning (O0OOOOO00000OO00O +":非修数据语句，请核对后重试")#line:471
                exit (1 )#line:473
            logging .info (O0OOOOO00000OO00O +'执行成功')#line:475
            return O0OOOOO00000OO00O +'执行成功'#line:477
        except Exception as OO0O0000O000O0000 :#line:479
            logging .warning (str (O0OOOOO00000OO00O )+"执行失败，错误原因:"+str (OO0O0000O000O0000 ))#line:481
            return False #line:483
    def close (O0OOOOO0OO0O0OO00 ):#line:484
        ""#line:490
        O0OOOOO0OO0O0OO00 .pool .close ()#line:492
    def commit (O0O000O00000O000O ):#line:494
        ""#line:500
        if not O0O000O00000O000O .conn ==None :#line:502
            O0O000O00000O000O .conn .commit ()#line:504
        else :#line:506
            logging .warning ('未执行DML数据库操作，commit失败')#line:508
            return False #line:510
    def rollback (O0OO000000OOO0000 ):#line:512
        ""#line:518
        if not O0OO000000OOO0000 .conn ==None :#line:519
            O0OO000000OOO0000 .conn .rollback ()#line:521
        else :#line:523
            logging .warning ('未执行DML数据库操作，commit失败')#line:525
            return False #line:527
class OraclePool (object ):#line:533
    try :#line:534
        def __init__ (O000OO0O0OO000O00 ,OOOO0O0O00OO0OO0O ):#line:536
            O000OO0OOOO0O0OOO ,OO0O0OO0O0000OOOO ,OO00000O000OOO000 ,OO000O000OO000O00 ,O000OO000O0OO0000 ,OOO00000O0OOO0OOO =MAPP (OOOO0O0O00OO0OO0O )#line:540
            OO00OO0O0O0OO00OO =O000OO0OOOO0O0OOO +":"+str (OO0O0OO0O0000OOOO )+"/"+O000OO000O0OO0000 #line:542
            O0O0000O0O000OO0O =OOOO0O0O00OO0OO0O #line:544
            O000OO0O0OO000O00 .pool =PooledDB (creator =Oracle ,user =decrypt (OO00000O000OOO000 ),password =decrypt (OO000O000OO000O00 ),dsn =OO00OO0O0O0OO00OO ,)#line:556
            O000OO0O0OO000O00 .conn =None #line:558
    except Exception as e :#line:562
        print (e )#line:564
    def get_conn (OOO00OO0O0O000000 ):#line:566
        ""#line:574
        OOO00OO0O0O000000 .conn =OOO00OO0O0O000000 .pool .connection ()#line:576
        OOO00OO0O0O000000 .cursor =OOO00OO0O0O000000 .conn .cursor ()#line:578
    @staticmethod #line:580
    def __O0OOOOOOO0OO0O0OO (OOOOOO0OO0O0OO000 ):#line:581
        ""#line:589
        OOOOOO0OO0O0OO000 .cursor .close ()#line:591
        OOOOOO0OO0O0OO000 .conn .close ()#line:593
    def execute (OO0O00O0OO0O00O00 ,O0O000OOOOOO000O0 ,*O00OOOOO0OOO00O0O ):#line:595
        if not OO0O00O0OO0O00O00 .conn :#line:597
            OO0O00O0OO0O00O00 .get_conn ()#line:598
        if O00OOOOO0OOO00O0O :#line:600
            OO0O00O0OO0O00O00 .cursor .execute (O0O000OOOOOO000O0 ,O00OOOOO0OOO00O0O )#line:602
        else :#line:604
            OO0O00O0OO0O00O00 .cursor .execute (O0O000OOOOOO000O0 )#line:606
    def executemany (OOOO000O0O0000OOO ,O000OO0000000OOO0 ,O0OO00O000OOOO0O0 ):#line:610
        if not OOOO000O0O0000OOO .conn :#line:612
            OOOO000O0O0000OOO .get_conn ()#line:613
        if O0OO00O000OOOO0O0 :#line:615
            OOOO000O0O0000OOO .cursor .executemany (O000OO0000000OOO0 ,O0OO00O000OOOO0O0 )#line:619
        else :#line:623
            OOOO000O0O0000OOO .cursor .executemany (O000OO0000000OOO0 )#line:627
    def back_up (OO0OOOO00OO000O0O ,O00O000OO0O0O00O0 ,*OOO0O0OOO0OO00000 ):#line:631
        ""#line:643
        OO0O00OOOO0000O0O =O00O000OO0O0O00O0 .strip (' ').split (' ',1 )[0 ]#line:645
        try :#line:647
            logging .info (O00O000OO0O0O00O0 +'执行中')#line:649
            if OO0O00OOOO0000O0O =='create':#line:651
                OO0OOOO00OO000O0O .execute (O00O000OO0O0O00O0 ,*OOO0O0OOO0OO00000 )#line:653
            elif OO0O00OOOO0000O0O =='insert':#line:655
                OO0OOOO00OO000O0O .execute (O00O000OO0O0O00O0 ,*OOO0O0OOO0OO00000 )#line:657
            else :#line:659
                logging .info (str (O00O000OO0O0O00O0 )+"-----非备份语句，请确认语句是否正确")#line:661
                exit (1 )#line:663
            logging .info (O00O000OO0O0O00O0 +'执行成功')#line:665
            return O00O000OO0O0O00O0 +'执行成功'#line:667
        except Exception as OO0000OOO000O0000 :#line:669
            logging .warning (str (O00O000OO0O0O00O0 )+"执行失败，错误原因:"+str (OO0000OOO000O0000 ))#line:671
            return False #line:673
    def select (OO00OOO0OOO0OOOO0 ,OO00000O0O00OOO00 ,*O0OO0000O0OOOOOO0 ):#line:675
        ""#line:687
        global df ,list #line:689
        try :#line:691
            logging .info (OO00000O0O00OOO00 +'执行中')#line:693
            O0O0000OOO000O0O0 =OO00000O0O00OOO00 .strip (' ').split (' ',1 )[0 ]#line:695
            if O0O0000OOO000O0O0 =='select':#line:697
                OO00OOO0OOO0OOOO0 .execute (OO00000O0O00OOO00 ,*O0OO0000O0OOOOOO0 )#line:699
                df =pd .read_sql (OO00000O0O00OOO00 ,OO00OOO0OOO0OOOO0 .conn )#line:701
                O0O00OO0O0O000OO0 =df .to_dict (orient ="records")#line:703
                list =[CIMultiDict (OOO00OO00OOOOO000 )for OOO00OO00OOOOO000 in O0O00OO0O0O000OO0 ]#line:705
            else :#line:709
                logging .warning (OO00000O0O00OOO00 +":非select语句，请确认语句是否正确")#line:711
                exit (1 )#line:713
            logging .info (OO00000O0O00OOO00 +'执行成功')#line:715
            return list #line:717
        except Exception as O0O00OOOO00O0OO00 :#line:721
            logging .warning (str (OO00000O0O00OOO00 )+"执行失败，错误原因:"+str (O0O00OOOO00O0OO00 ))#line:723
            return False #line:725
    def execute_sql (O0O0OOO00OO0O000O ,O0OO00OO0000O0OOO ,*O000O00O00OOO00O0 ):#line:727
        ""#line:739
        try :#line:741
            logging .info (O0OO00OO0000O0OOO +'执行中')#line:743
            OO00OO0O00000OOO0 =O0OO00OO0000O0OOO .strip (' ').split (' ',1 )[0 ]#line:745
            if OO00OO0O00000OOO0 =='insert':#line:747
                O0O0OOO00OO0O000O .execute (O0OO00OO0000O0OOO ,*O000O00O00OOO00O0 )#line:749
            elif OO00OO0O00000OOO0 =='update':#line:751
                O0O0OOO00OO0O000O .execute (O0OO00OO0000O0OOO ,*O000O00O00OOO00O0 )#line:753
            elif OO00OO0O00000OOO0 =='delete':#line:755
                O0O0OOO00OO0O000O .execute (O0OO00OO0000O0OOO ,*O000O00O00OOO00O0 )#line:757
            elif OO00OO0O00000OOO0 =='drop':#line:759
                O0O0OOO00OO0O000O .execute (O0OO00OO0000O0OOO ,*O000O00O00OOO00O0 )#line:761
            elif OO00OO0O00000OOO0 =='create':#line:763
                O0O0OOO00OO0O000O .execute (O0OO00OO0000O0OOO ,*O000O00O00OOO00O0 )#line:765
            elif OO00OO0O00000OOO0 =='truncate':#line:767
                O0O0OOO00OO0O000O .execute (O0OO00OO0000O0OOO ,*O000O00O00OOO00O0 )#line:769
            elif OO00OO0O00000OOO0 =='alter':#line:770
                O0O0OOO00OO0O000O .execute (O0OO00OO0000O0OOO ,*O000O00O00OOO00O0 )#line:772
            else :#line:773
                logging .warning (O0OO00OO0000O0OOO +":非修数据语句，请核对后重试")#line:775
                exit (1 )#line:777
            logging .info (O0OO00OO0000O0OOO +'执行成功')#line:779
            return O0OO00OO0000O0OOO +'执行成功'#line:781
        except Exception as O0O0O00O0OO0O0000 :#line:783
            logging .warning (str (O0OO00OO0000O0OOO )+"执行失败，错误原因:"+str (O0O0O00O0OO0O0000 ))#line:785
            return False #line:787
    def execute_sql_many (O0O0000OOO0O0OO00 ,O000OOOOOOO00O0O0 ,O0O00O000O0OOO000 ):#line:789
        ""#line:801
        try :#line:803
            logging .info (O000OOOOOOO00O0O0 +'执行中')#line:805
            OO0OOO00OOO00O00O =O000OOOOOOO00O0O0 .strip (' ').split (' ',1 )[0 ]#line:807
            if OO0OOO00OOO00O00O =='insert':#line:809
                O0O0000OOO0O0OO00 .executemany (O000OOOOOOO00O0O0 ,O0O00O000O0OOO000 )#line:811
            elif OO0OOO00OOO00O00O =='update':#line:813
                O0O0000OOO0O0OO00 .executemany (O000OOOOOOO00O0O0 ,O0O00O000O0OOO000 )#line:815
            elif OO0OOO00OOO00O00O =='delete':#line:817
                O0O0000OOO0O0OO00 .executemany (O000OOOOOOO00O0O0 ,O0O00O000O0OOO000 )#line:819
            elif OO0OOO00OOO00O00O =='drop':#line:821
                O0O0000OOO0O0OO00 .executemany (O000OOOOOOO00O0O0 ,O0O00O000O0OOO000 )#line:823
            elif OO0OOO00OOO00O00O =='create':#line:825
                O0O0000OOO0O0OO00 .executemany (O000OOOOOOO00O0O0 ,O0O00O000O0OOO000 )#line:827
            elif OO0OOO00OOO00O00O =='truncate':#line:829
                O0O0000OOO0O0OO00 .executemany (O000OOOOOOO00O0O0 ,O0O00O000O0OOO000 )#line:831
            elif OO0OOO00OOO00O00O =='alter':#line:832
                O0O0000OOO0O0OO00 .executemany (O000OOOOOOO00O0O0 ,O0O00O000O0OOO000 )#line:834
            else :#line:835
                logging .warning (O000OOOOOOO00O0O0 +":非修数据语句，请核对后重试")#line:837
                exit (1 )#line:839
            logging .info (O000OOOOOOO00O0O0 +'执行成功')#line:841
            return O000OOOOOOO00O0O0 +'执行成功'#line:843
        except Exception as OOO0OOO000O0O00O0 :#line:845
            logging .warning (str (O000OOOOOOO00O0O0 )+"执行失败，错误原因:"+str (OOO0OOO000O0O00O0 ))#line:847
            return False #line:849
    def close (O0OOO0O000O0O0OOO ):#line:851
        ""#line:857
        O0OOO0O000O0O0OOO .pool .close ()#line:859
    def commit (OOO0OOO000OOO0OOO ):#line:861
        ""#line:867
        if not OOO0OOO000OOO0OOO .conn ==None :#line:869
            OOO0OOO000OOO0OOO .conn .commit ()#line:871
        else :#line:873
            logging .warning ('未执行DML数据库操作，commit失败')#line:875
            return False #line:877
    def rollback (O0OOO00O00O00OOOO ):#line:879
        ""#line:885
        if not O0OOO00O00O00OOOO .conn ==None :#line:886
            O0OOO00O00O00OOOO .conn .rollback ()#line:888
        else :#line:890
            logging .warning ('未执行DML数据库操作，rollback失败')#line:892
            return False #line:894
def encrypt (OO0OO00OOOOOOO00O ):#line:899
    OOO00000OO00O0O0O =bytearray (str (OO0OO00OOOOOOO00O ).encode ("gbk"))#line:900
    O0OOOOO0OOOOOOO00 =len (OOO00000OO00O0O0O )#line:902
    O000OO000O00O00OO =bytearray (O0OOOOO0OOOOOOO00 *2 )#line:904
    O0O0OO00O00000O00 =0 #line:906
    for O0O0O000OO00OO0OO in range (0 ,O0OOOOO0OOOOOOO00 ):#line:908
        OOO0OO0O0O000OOO0 =OOO00000OO00O0O0O [O0O0O000OO00OO0OO ]#line:909
        OOO0O0O00O00O00OO =OOO0OO0O0O000OOO0 ^8 #line:911
        O0O0000O0OO0OOOOO =OOO0O0O00O00O00OO %16 #line:913
        O0OO0OOO0O0O00O00 =OOO0O0O00O00O00OO //16 #line:915
        O0O0000O0OO0OOOOO =O0O0000O0OO0OOOOO +65 #line:917
        O0OO0OOO0O0O00O00 =O0OO0OOO0O0O00O00 +65 #line:919
        O000OO000O00O00OO [O0O0OO00O00000O00 ]=O0O0000O0OO0OOOOO #line:921
        O000OO000O00O00OO [O0O0OO00O00000O00 +1 ]=O0OO0OOO0O0O00O00 #line:923
        O0O0OO00O00000O00 =O0O0OO00O00000O00 +2 #line:925
    return O000OO000O00O00OO .decode ("gbk")#line:927
def decrypt (O000O0OO00OO0OOO0 ):#line:932
    O00O00OOO0OO0O0O0 =bytearray (str (O000O0OO00OO0OOO0 ).encode ("gbk"))#line:933
    O00OOOO0O00OOOO0O =len (O00O00OOO0OO0O0O0 )#line:935
    if O00OOOO0O00OOOO0O %2 !=0 :#line:937
        return ""#line:938
    O00OOOO0O00OOOO0O =O00OOOO0O00OOOO0O //2 #line:940
    OO00O0O000000OO0O =bytearray (O00OOOO0O00OOOO0O )#line:942
    OO0OOOO0O0OOOOO00 =0 #line:944
    for O00OO0OO0O00OOOOO in range (0 ,O00OOOO0O00OOOO0O ):#line:946
        O00OOO00000OOO00O =O00O00OOO0OO0O0O0 [OO0OOOO0O0OOOOO00 ]#line:947
        O00OO0O0O0O0OO000 =O00O00OOO0OO0O0O0 [OO0OOOO0O0OOOOO00 +1 ]#line:949
        OO0OOOO0O0OOOOO00 =OO0OOOO0O0OOOOO00 +2 #line:951
        O00OOO00000OOO00O =O00OOO00000OOO00O -65 #line:953
        O00OO0O0O0O0OO000 =O00OO0O0O0O0OO000 -65 #line:955
        O0O00O0OO0O0OO0O0 =O00OO0O0O0O0OO000 *16 +O00OOO00000OOO00O #line:957
        OO000OO0000OO0O00 =O0O00O0OO0O0OO0O0 ^8 #line:959
        OO00O0O000000OO0O [O00OO0OO0O00OOOOO ]=OO000OO0000OO0O00 #line:961
    try :#line:963
        return OO00O0O000000OO0O .decode ("gbk")#line:965
    except :#line:967
        return "failed"#line:969
