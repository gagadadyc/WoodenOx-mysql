
def wrEvent(localTime,eventName,user_hostStr,EXtime,SQL):
    f = open('eventLog.wo', 'a')
    # 一行一条记录，用两个空格隔开变量；顺序依次是：本地时间、时间名称、user@host、执行时间、执行的SQL语句
    f.write(localTime+"  "+eventName+"  "+user_hostStr+"  "+EXtime+"  "+SQL+"\n")