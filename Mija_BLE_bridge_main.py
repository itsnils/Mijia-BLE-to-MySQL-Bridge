import time
import re
import pigpio
import pymysql
import datetime
import threading
from btlewrap.bluepy import BluepyBackend
from mitemp_bt.mitemp_bt_poller import MiTempBtPoller, \
    MI_TEMPERATURE, MI_HUMIDITY, MI_BATTERY


class ble_to_mysql():
    def __init__(self):
        self.hardware_watchdog = None
        self.pi = pigpio()
        self.led_read = 16
        self.led_run = 20
        self.pi.set_mode(self.led_run, pigpio.OUTPUT)
        self.pi.set_mode(self.led_read, pigpio.OUTPUT)
        self.host = None
        self.port = None
        self.db = None
        self.passwd = None
        self.user = None
        self.my_db = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd, db=self.db)
        self.sql_cursor = self.my_db.cursor()

    def read_settings_data(self):
        """
        The settings from the settings.txt file are read and transferred here.
        """
        try:
            data = open('/home/pi/settings.txt', 'r')
            for i in data:
                x = re.split('=|"|\n', i)
                if x[0] == "hardware_watchdog":
                    if x[2] == "True":
                        self.hardware_watchdog = True
                        print("WG on")
                    if x[2] == "Fasle":
                        self.hardware_watchdog = False
                        print("WG off")

                if x[0] == "host":
                    self.host = int(x[2])

                if x[0] == "port":
                    self.port = int(x[2])

                if x[0] == "user":
                    self.user = str(x[2])

                if x[0] == "db":
                    self.db = str(x[2])

                if x[0] == "passwd":
                    self.passwd = str(x[2])



        except:
            print("Error no settings.txt")

    def sensor_output_to_db(self, *args):
        """
         Leest gegevens van de sensor zendt de gegevens naar een SQL-server.
        """
        for i in args:
            self.pi.write(self.led_read,1)
            poller = MiTempBtPoller(i, BluepyBackend)
            humidity = poller.parameter_value(MI_HUMIDITY)
            battery = poller.parameter_value(MI_BATTERY)
            temprature = poller.parameter_value(MI_TEMPERATURE)
            self.pi.write(self.led_read,0)
            for i in range(0,5):
                self.pi.write(self.led_read,1)
                time.sleep(0.1)
                self.pi.write(self.led_read,0)
                time.sleep(0.1)

            if i == "4c:65:a8:d0:8e:35":
                sql = """INSERT INTO Sesnor_35(
                timestamp,
                temprature,
                humidity,
                battery
                )
                VALUES (%s, %s, %s, %s )
                """
            if i == "58:2d:34:36:a8:16":
                sql = """INSERT INTO Sesnor_16(
                timestamp,
                temprature,
                humidity,
                battery
                )
                VALUES (%s, %s, %s, %s )
                 """
            if i == "4c:65:a8:d0:96:f5":
                sql = """INSERT INTO Sesnor_f5(
                timestamp,
                temprature,
                humidity,
                battery
                )
                VALUES (%s, %s, %s, %s )
                """

            ts = time.time()
            timestamp = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            recordTuple = (
                timestamp, temprature, humidity, battery)
            try:
                self.sql_cursor.execute(sql, recordTuple)
                self.my_db.commit()
                print(i)
                print("Gesendet")
            except:
                self.my_db.rollback()
                print("Error")

    def hardware_watchdog_petting(self,):
        """
        Here the Software watchdog is stroked. if /dev/watchdog is not written for 15 sec. the system will reboot.
        """
        f = open('/dev/watchdog', 'w')
        f.write("S")
        f.close()
        print("Watchdog Reset")

    def read_loop(self):
        """
        Here the loop is active to read the sensors and the data is sent to the SQL server.
        """
        while True:
            try:
                self.sensor_output_to_db("4c:65:a8:d0:8e:35", "58:2d:34:36:a8:16", "4c:65:a8:d0:96:f5")
                time.sleep(5*60-20)
            except:
                print("Error End")
                time.sleep(2)

    def main(self):
        """
        Everything is activated here. The sensors are read out, the watchdog is stimulated, the LEDs are updated.
        """
        t1 = threading.Thread(target=self.read_loop)
        t1.start()
        self.hardware_watchdog = False
        start = 0
        while True:
            if start == 120:
                start = start + 1
                self.hardware_watchdog = True
            self.pi.write(self.led_run, 1)
            if self.hardware_watchdog == True:
                self.hardware_watchdog_petting()
            time.sleep(1)
            self.pi.write(self.led_run, 0)
            time.sleep(0.5)

run = ble_to_mysql()
run.main()