import time
import board
import adafruit_dht

# 创建DHT11传感器对象,连接到GPIO引脚4
dht11 = adafruit_dht.DHT11(board.D4)

while True:
    try:
        # 读取温湿度数据
        temperature = dht11.temperature
        humidity = dht11.humidity

        # 打印温湿度数据
        print(f"Temperature: {temperature:.1f} °C")
        print(f"Humidity: {humidity:.1f} %")

    except RuntimeError as error:
        # 读取数据失败,打印错误信息
        print(f"Failed to read data from DHT11: {error.args[0]}")

    except Exception as error:
        # 发生其他异常,关闭连接并抛出错误
        dht11.exit()
        raise error

    # 等待2秒后进行下一次读取
    time.sleep(2)