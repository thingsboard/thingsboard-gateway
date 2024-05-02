import time
import Adafruit_DHT

# 设置DHT11传感器类型和GPIO引脚号
sensor = Adafruit_DHT.DHT11
gpio_pin = 17

try:
    while True:
        # 读取DHT11传感器数据
        humidity, temperature = Adafruit_DHT.read_retry(sensor, gpio_pin)

        if humidity is not None and temperature is not None:
            print(f"Temperature: {temperature:.1f} °C, Humidity: {humidity:.1f} %")
        else:
            print("Failed to retrieve data from DHT11 sensor.")

        # 每2秒读取一次数据
        time.sleep(2)

except KeyboardInterrupt:
    print("Measurement stopped by user.")