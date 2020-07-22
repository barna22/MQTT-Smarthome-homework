import numpy
import matplotlib.pyplot as plt
import math
import paho.mqtt.client as mqtt
import time
import mysql.connector
from mysql.connector import Error

#connect to dbms
try:
    connection = mysql.connector.connect(host='localhost',
                                         database='heading',
                                         user='barna',
                                         password='mqtt')
    if connection.is_connected():
        print("Connected to database")
except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        print("No errors")


#return to default values after averaging
def rst():
    global x
    global y
    global z
    global cnt
    global start
    x = []
    y = []
    z = []
    cnt = 0
    start = time.time()


#calculate average
def getaverage():
    global heading
    return sum(heading) / len(heading)


#write data to database
def db_write(average):
    global connection
    global idcnt
    try:
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO headingdata (id, value) 
                                    VALUES (%s, %s) """

        recordTuple = (idcnt, average)
        cursor.execute(mySql_insert_query, recordTuple)
        connection.commit()
        print("Record inserted successfully into table")
        idcnt = idcnt + 1

    except mysql.connector.Error as error:
        print("Failed to insert into MySQL table {}".format(error))

    finally:
        print("No errors during insertion")



#publish data on new mqtt topic
def sendmqttdata(average):
    client.publish("headingdata", average)


#plot the data so far
def showplot():
    global connection
    global headingidx
    global headingvalue
    try:
        sql_select_Query = "select * from headingdata"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        for row in records:
            headingidx.append(row[0])
            headingvalue.append(row[1])

    except Error as e:
        print("Error reading data from MySQL table", e)
    finally:
            print("No errors during query")
    print(headingvalue)
    print(headingidx)
    plt.plot(headingvalue)
    plt.show()



#called every time a new message is received
def on_message(client, userdata, msg):
    global cnt
    if "x" in msg.topic:
        xvalue = str(msg.payload)[2:-1]
        x.append(float(xvalue))
    if "y" in msg.topic:
        yvalue = str(msg.payload)[2:-1]
        y.append(float(yvalue))
    if "z" in msg.topic:
        zvalue = str(msg.payload)[2:-1]
        z.append(float(zvalue))
        heading.append(math.atan2(y[cnt], x[cnt]) * 180 / math.pi)
        if time.time() - start > 600:
            average = getaverage()
            db_write(average)
            sendmqttdata(average)
            rst()
        else:
            cnt = cnt + 1
    if time.time() - programstart > 6000:
        showplot()



#setting up global variables and MQTT connection
client = mqtt.Client()
client.on_message = on_message

client.connect("vm.smallville.cloud.bme.hu", 18211, 60)
client.subscribe('phone/Magnetometer/x')
client.subscribe('phone/Magnetometer/y')
client.subscribe('phone/Magnetometer/z')

x = []
y = []
z = []
heading = []
cnt = 0
idcnt = 0
programstart = time.time()
start = time.time()
headingidx = []
headingvalue = []

client.loop_forever()