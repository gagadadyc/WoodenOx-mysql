import os
import time
import threading
import mysql.connector
from mysql.connector import errorcode

import reConfig

# 获取链接参数字典
ConnParDict = reConfig.reConnParameter()

try:
    cnx = mysql.connector.connect(**ConnParDict)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
        exit(1)
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
        exit(1)
    else:
        print(err)
        exit(1)
else:
    print("Connected")

# dictionary=True将返回结果集的每一行都以字典形式返回，key为列名
cursor = cnx.cursor(dictionary=True)
# 检查用户是否具有Event_priv权限
# 由于有时host与登录时输入的host不同，如输入“127.0.0.1”实际登录的是“locallhost”那行的用户（如果“localhost”存在的话），所以要再次获取一次用户+主机名
cursor.execute("select user();")
user_hostStr = cursor.fetchone()['user()']
(user, host) = user_hostStr.strip().split('@')
cursor.execute(
    "select event_priv from mysql.user where user=%s and host=%s", [user, host])
if cursor.fetchone()['event_priv'] != 'Y':
    # 当前用户没有event_priv权限，提示并退出
    print("The current user has no privileges to event(event_priv is not 'Y'). Please contact the Database Administrator")
    exit(0)

# 检查事件调度器参数
cursor.execute("show variables like 'event_scheduler';")
row = cursor.fetchone()
if row['Value'] == "OFF":
    # 打开依赖的“事件调度器”功能
    cursor.execute("SET GLOBAL event_scheduler = ON;")
    print("'event_scheduler'is OFF,we sets it to ON")
elif row['Value'] == "DISABLED":
    # 由于DISABLED情况下无法在mysql启动该的情况下更改成ON，提示修改配置文件打开event_scheduler功能
    print(
        "Error:'event-scheduler' is DISABLED, please In the server configuration file (my.cnf, or my.ini on Windows systems), include the line where it will be read by the server (for example, in a [mysqld] section: event_scheduler=ON) Finally restart MySQL")
    exit(1)

# 读取时间
EXtime = reConfig.reTime()
# 读取SQL语句
SQL = reConfig.reSQL()

print("At " + EXtime)
print("The SQL \n" + SQL + "\nwill be submitted to Mysql")

print("If yes, enter Y/y in ten seconds, Enter other values to exit")
cfStr = input()
if cfStr != 'Y' and cfStr != 'y':
    print("The submission was canceled,nothing to do,exit")
    exit()

# 使用OXevent+当前时间来作为事件名
enentName = "OXEvent" + time.strftime("%Y%m%d%H%M%S%p", time.localtime())
# 构造SQL语句，包装成计划事件并提交
sqlStr = "CREATE EVENT `" + enentName + "` " + "ON SCHEDULE AT '" + \
    EXtime + "' " + "ON COMPLETION PRESERVE " + "DO " + SQL
print(sqlStr)
try:
    cursor.execute(sqlStr)
except mysql.connector.ProgrammingError as err:
    print(err)
    cursor.close()
    cnx.close()
    exit(1)

cursor.close()
cnx.close()
