import requests
import datetime
import time
import xml.etree.cElementTree as ET

TIMEOUT_REQ = 10
TIMEZONE_DELTA = 0 #4
SECONDS_TO_WARN = 1
DELETE_TIME = 1

tree = ET.parse('serverVideo3.0.xml')
ourstr = tree.findall('data_base')[0].text
if ourstr == "MariaDb":
    import MySQLdb as dbToConn
else:
    from hdbcli import dbapi as dbToConn


port = tree.findall('port')[0].text

if ourstr == "MariaDb":
    db_port = tree.findall('db_port')[0].text
    conn = dbToConn.connect(host=db_port.split(":")[0], port=int(db_port.split(":")[1]),
                           user='loaduser', passwd='password', db='tfone', charset='utf8')
else:
    db_port = tree.findall('db_port')[0].text
    conn = dbToConn.connect(db_port.split(":")[0], db_port.split(":")[1],
                           user="SYSTEM", password="111111*zZ", autocommit=False)
cursor = conn.cursor()

adress = tree.findall('alarm_adress')[0].text
signal_time = int(tree.findall('signal_time')[0].text)  #172.16.234.2

while True:

    ourtime = datetime.datetime.now() + datetime.timedelta(hours=TIMEZONE_DELTA)
    if ourstr == "MariaDb":
        sign = '''select * from alarm
    where ts_time < (%s) and ts_time > (%s)'''
    else:
        sign = '''select * from tfone.alarm
    where ts_time < (?) and ts_time > (?)'''
    cursor.execute(sign, (str(ourtime), str(ourtime - datetime.timedelta(seconds=SECONDS_TO_WARN))))
    conn.commit()
    if cursor.fetchone():
        try:
            requests.get(r'http://' + adress + r'/cmd.cgi?cmd=REL,2,1', timeout=TIMEOUT_REQ)
            time.sleep(signal_time)
            requests.get(r'http://' + adress + r'/cmd.cgi?cmd=REL,2,0', timeout=TIMEOUT_REQ)

            if ourstr == "MariaDb":
                sign = '''delete from alarm
            where ts_time < (%s) and ts_time > (%s)'''
            else:
                sign = '''delete from tfone.alarm
            where ts_time < (?) and ts_time > (?)'''

            cursor.execute(sign, (str(ourtime), str(ourtime - datetime.timedelta(seconds=SECONDS_TO_WARN))))
            conn.commit()

            if ourstr == "MariaDb":
                sign = '''delete from alarm
            where ts_time < (%s)'''
            else:
                sign = '''delete from tfone.alarm
            where ts_time < (?)'''
            cursor.execute(sign, [str(datetime.datetime.now()+datetime.timedelta(hours=TIMEZONE_DELTA, minutes=-DELETE_TIME))])
            conn.commit()
        except Exception as e:
            if ourstr == "MariaDb":
                sign = "INSERT INTO system_log(sys_date, type, message, service_name) VALUES(%s,%s,%s,%s)"
            else:
                sign = "INSERT INTO tfone.system_log(sys_date, type, message, service_name) VALUES(?,?,?,?)"
            cursor.execute(sign,
                           (str(datetime.datetime.now()), "ERROR", str(e)[0:254],
                            "signal_laurent"))
            conn.commit()
            time.sleep(60)
    else:
        time.sleep(0.4)
