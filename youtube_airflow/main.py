import json
from datetime import datetime, timezone
import pandas as pd
import requests

city_name = "Kumasi"
api_key = "d1d125186b2bb28c3c2c4828f0cb7375"
full_url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"

with open("credentials.txt", "r") as f:
    api_key = f.read()

res = requests.get(full_url)
data = res.json()

def kelvin_to_fahrenheit(kelvin):
    celcius = (kelvin - 273.15) * (9/5) + 32
    celcius = round(celcius, 3)
    return celcius


def daily_weather_update_kumasi(full_url):

    weather_data = {}

    weather_data["City"] = data['name']
    weather_data["Weather_description"] = data["weather"][0]["description"]
    weather_data["Temp_fahrenheit"] = kelvin_to_fahrenheit(data["main"]["temp"])
    weather_data["Feels_like_fahrenheit"] = kelvin_to_fahrenheit(data["main"]["feels_like"])
    weather_data["Min_temp_fahrenheit"] = kelvin_to_fahrenheit(data["main"]["temp_min"])
    weather_data["Max_temp_fahrenheit"] = kelvin_to_fahrenheit(data["main"]["temp_max"])
    weather_data["Pressure"] = data["main"]["pressure"]
    weather_data["Humidity"] = data["main"]["humidity"]
    weather_data["Wind_speed"] = data["wind"]["speed"]
    weather_data["Time_of_record"] = datetime.fromtimestamp(data["dt"])
    weather_data["Sunrise_time"] = datetime.fromtimestamp(data["sys"]["sunrise"])
    weather_data["Sunset_time"] = datetime.fromtimestamp(data["sys"]["sunset"])

    weather_data_df = pd.DataFrame([weather_data])
    
    weather_data_df.to_csv("daily_weather_update_kumasi.csv", index=False)

if __name__ == "__main__":
    daily_weather_update_kumasi(full_url)