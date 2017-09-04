import time


# 链接参数字典
ConnParDict = {
    'user': "",
    'password': "password",
    'host': "127.0.0.1",
    'port': "3306",
    'database': "DB",
    'raise_on_warnings': True,
    'get_warnings': True
}
EXtime = "1970-01-01 00:00:00"

with open('config.wo', 'r') as f:
    # 用来标识文档块开始与结束的提取标志位
    list = f.readlines()


def reConnParameter():
    # 读取开关
    draw = False
    # 判断[config_begin]错误标志位
    CPMark = False
    # 行号
    num = 0
    for line in list:
        num = num + 1
        # line.strip()去除末尾换行符、空格、制表符
        if line.strip() == "[connection_parameter]":
            draw = True
            CPMark = True
            continue
        # 检查到下一模块的起始符代表这一模块已经结束
        if line.strip().startswith('['):
            draw = False
            break
        # 跳过注释
        if line.strip().startswith('#'):
            print("跳过注释")
            continue

        if draw:
            if line.strip() == "":
                continue
            configArr = line.strip().split('=')
            # 检查格式、将配置填入字典，填入时使用strip去除首尾空格
            if ConnParDict.get(configArr[0].strip()) == None:
                print("near", configArr[0].strip(), "at line", num,
                      ",You have an error in your Config syntax; you can check the manual for the https://imdyc.com 配置文档地址mark")
                exit(1)
            else:
                ConnParDict[configArr[0].strip()] = configArr[1].strip()
    # for end
    # 若读不到[config_begin]，python可能会抛出账号密码错误的异常，用户难以理解。系统进一步判断，如有错误，提示并退出
    if not CPMark:
        print("You have an error in your Config syntax;The '[connection_parameter]' cannot be found in the config.wo")
        exit(1)
    return ConnParDict

# 读取并格式化时间,如果有非法输入则提示并exit程序
# 返回的是一个2017-8-28 20:07格式的字符串


def reTime():
    draw = False
    ETMark = False
    num = 0
    for line in list:
        num = num + 1
        # line.strip()去除末尾换行符
        if line.strip() == "[execution_time]":
            draw = True
            ETMark = True
            continue
        # 检查到下一模块的起始符代表这一模块已经结束
        if ETMark == True and line.strip().startswith('['):
            draw = False
            break
        # 跳过注释
        if line.strip().startswith('#'):
            continue
        if draw:
            # 跳过空行
            if line.strip() == "":
                continue
            timeArr = line.strip().split('=')
            if timeArr[0].strip() != "EXtime":
                print("near", timeArr[0].strip(),
                      "at line", num, ",You have an error in your Config syntax; you can check the manual for the https://imdyc.com 配置文档地址mark")
                exit(1)
            else:
                EXtime = timeArr[1].strip()
                break
    # for end
    # 如果没读到时间则向用户报错
    if not ETMark:
        print("You have an error in your Config syntax;The '[execution_time]' cannot be found in the config.wo")
        exit(1)
    # 控制时间格式，如果提交超过限制的时间，比如 2017-13-28 24:88:10,或者 2017-8-28 12:08:-15，python会捕捉异常，我们将这个异常捕捉并提示给用户
    try:
        timeTuple = (time.strptime(EXtime, "%Y-%m-%d %H:%M:%S"))
    except ValueError as err:
        print("You have an error in your Config syntax;",err)
        exit(1)

    # 将从配置中读到的时间转换为时间戳 便于比较
    timesta = time.mktime(timeTuple)
    # 不能执行“过去的任务”
    if timesta < time.time():
        print("at line", num, "You have an error in your time value; note: You can't set the time value in the past one")
        exit(1)
    # 表示只能执行十秒以后的任务
    elif timesta < time.time() + 10:
        print("You have an error in your time value; note: Delayed execution time should be more than 10 seconds")
        exit(1)

    return EXtime


def reSQL():
    draw = False
    sqlMark = False
    num = 0
    SQL = ""
    for line in list:
        num = num + 1
        # line.strip()去除末尾换行符
        if line.strip() == "[SQL]":
            draw = True
            sqlMark = True
            continue
        # 检查到下一模块的起始符代表这一模块已经结束
        if sqlMark == True and line.strip().startswith('['):
            draw = False
            break
        # 跳过注释
        if line.strip().startswith('#'):
            continue
        if draw:
            # 跳过空行
            if line.strip() == "":
                continue
            SQL = SQL+line
    # for end
    # 读不到SQL语句则向用户报错
    if not sqlMark:
        print("You have an error in your Config syntax;The '[SQL]' cannot be found in the config.wo")
        exit(1)
    return SQL