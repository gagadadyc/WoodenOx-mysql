import os
import time
import threading
import mysql.connector
from mysql.connector import errorcode

import reConfig
import wrLog

print("This is "+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+" for localhost's time")
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
    print("MySQL is Connected")
# dictionary=True将返回结果集的每一行都以字典形式返回，key为列名
cursor = cnx.cursor(dictionary=True)

# 显示服务器时间
cursor.execute("select now();")
serverTime = cursor.fetchone()['now()']
print("This is", serverTime ,"for MysqlServer's time")

# 检查用户是否具有Event_priv权限
# 由于有时host与登录时输入的host不同，如输入“127.0.0.1”实际登录的是“locallhost”那行的用户（如果“localhost”存在的话），所以要再次获取一次用户+主机名
cursor.execute("select user();")
user_hostStr = cursor.fetchone()['user()']
(user,host) = user_hostStr.strip().split('@')
cursor.execute("select event_priv from mysql.user where user=%s and host=%s",[user,host])
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
    print("Error:'event-scheduler' is DISABLED, please In the server configuration file (my.cnf, or my.ini on Windows systems), include the line where it will be read by the server (for example, in a [mysqld] section: event_scheduler=ON) Finally restart MySQL")
    exit(1)

# 读取时间
EXtime = reConfig.reTime()
# 读取SQL语句
SQL = reConfig.reSQL()
print("At " + EXtime)
print("The SQL \n"+ SQL +"\nwill be submittal to Mysql")

print("If yes, enter Y/y , Enter other values to exit")
cfStr = input()
if cfStr != 'Y' and cfStr != 'y':
    print("The submission was canceled,nothing to do,exit") 
    exit(0)

# 检查是否有同一EXECUTE_AT（计划执行时间）的事件，如有则提醒用户
cursor.execute("select `EXECUTE_AT` from `information_schema`.`EVENTS` where EXECUTE_AT = '"+ EXtime +"'")
# fetchall()方法返回的是一个“元组列表”，当列表里没有值时，等同于False；而且此方法访问了结果集中所有的行，所以之后不会抛出结果集未读的异常
if cursor.fetchall():
    print("We find exist event will execute at "+ EXtime + ", continue to add? If yes, enter Y/y , Enter other values to exit")
    cfStr = input()
    if cfStr != 'Y' and cfStr != 'y':
        print("The submission was canceled,nothing to do,exit") 
        exit(0)


# 使用OXevent+当前时间来作为事件名【时间最小精度为秒，如有在一秒内可能执行多次，需要注意此处】
eventName = "OXEvent"+time.strftime("%Y%m%d%H%M%S", time.localtime())
# 构造SQL语句，包装成计划事件并提交
sqlStr = "CREATE EVENT `"+ eventName +"` "+"ON SCHEDULE AT '"+ EXtime + "' "+"ON COMPLETION PRESERVE "+"DO "+ SQL
try:
    # 在提交操作之前填写日志；日志内容有：本地时间、时间名称、user@host、执行时间、执行的SQL语句
    localTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    logSQL =  SQL.replace("\n"," ")
    wrLog.wrEvent(localTime,eventName,user_hostStr,EXtime,logSQL)
    cursor.execute(sqlStr)
except mysql.connector.ProgrammingError as err:
    print(err)
    cursor.close()
    cnx.close()
    exit(1)
# 通过检测是否返回空结果集，查询是否写入成功
cursor.execute("select EVENT_DEFINITION from `information_schema`.`EVENTS` where EVENT_NAME = '"+ eventName+ "'")
# fetchall()方法返回的是一个“元组列表”，当列表里没有值时，等同于False；而且此方法访问了结果集中所有的行，所以之后不会抛出结果集未读的异常
if not cursor.fetchall():
    print("select EVENT_DEFINITION from `information_schema`.`EVENTS` where EVENT_NAME = '"+ eventName+ "'")
    print("Event create failure.")

cursor.close()
cnx.close()

