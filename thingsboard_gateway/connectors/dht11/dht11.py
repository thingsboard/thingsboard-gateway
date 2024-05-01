import Adafruit_DHT

DHT_SENSOR = Adafruit_DHT.DHT11

def read_data(gpio):
    """Read humidity and temperature from DHT11 sensor"""
    humidity, temperature = Adafruit_DHT.read_retry(DHT_SENSOR, gpio)   
    if humidity is not None and temperature is not None:
        print(f'Temp={temperature:0.1f}*C Humidity={humidity:0.1f}%')
        return temperature, humidity
    else:
        raise RuntimeError('Failed to get reading from DHT11')