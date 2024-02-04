import json
import os
import logging
import pandas as pd
import subprocess
from external.client import YandexWeatherAPI
from utils import get_url_by_city_name, CITIES

class DataFetchingTask:
    def __init__(self, city_name: str, cities_dict: dict) -> None:
        self.city_name = city_name
        self.cities_dict = cities_dict

    @staticmethod
    def get_json(city_name: str):
        url_with_data = get_url_by_city_name(city_name)
        try:
            resp = YandexWeatherAPI.get_forecasting(url_with_data)
            return resp
        except Exception as e:
            logging.info(f"Error fetching data for {city_name}: {e}")
            return None

    def create_json(self) -> None:
        os.makedirs("json_cities", exist_ok=True)
        
        for city in self.cities_dict:
            data = self.get_json(city)
            if data:
                with open(f"json_cities/{city}.json", 'w') as file:
                    json.dump(data, file, indent=2)
                    logging.info(f"File json_cities/{city}.json has been created.")


# task = DataFetchingTask(city_name='Moscow', cities_dict=CITIES)
# task.create_json()


class DataCalculationTask:
    def __init__(self) -> None:
        self.script_path = "external/analyzer.py"
        self.input_folder = "json_cities"
        self.output_folder = "analyzed_cities"
    
    def analyze_cities(self):
        os.makedirs(self.output_folder, exist_ok=True)

        json_files = [f for f in os.listdir(self.input_folder) if f.endswith(".json")]

        for input_file in json_files:
            input_file_path = os.path.join(self.input_folder, input_file)
            output_file_path = os.path.join(self.output_folder, f"{input_file}")

            command = ["python", self.script_path, "-i", input_file_path, "-o", output_file_path]

            try:
                subprocess.run(command, check=True)
                logging.info(f"Script {input_file}. Successful execution.")
            except Exception as e:
                logging.info(f"Script {input_file}. An error has occurred: {e}")



class DataAggregationTask:
    def __init__(self) -> None:
        self.analyzed_cities_folder = "analyzed_cities"

    def create_xls(self): 
        analyzed_cities = [f for f in os.listdir(self.analyzed_cities_folder) if f.endswith(".json")]

        all_tables = []

        for analyze_city in analyzed_cities:
            with open(os.path.join(self.analyzed_cities_folder, analyze_city), "r") as city_json:
                city = json.load(city_json)
                df = pd.DataFrame(city['days'])
                original_data = df[['date', 'temp_avg', 'relevant_cond_hours']].copy()
                original_data.columns = ['Дата', 'Средняя температура', 'Время без осадков']
                original_data.insert(0, "Город", str(analyze_city)[:-5])
                grouped_data = original_data.groupby('Город', as_index=False).aggregate({"Средняя температура": "mean", "Время без осадков": "mean"})
                grouped_data['Рейтинг'] = (grouped_data['Средняя температура'].mul(grouped_data['Время без осадков']))/100
                grouped_data = grouped_data.replace(str(analyze_city)[:-5],'Среднее значение')
                result_table = pd.concat([original_data, grouped_data], ignore_index = True)
                # result_table.set_index(['Город', 'Дата'], inplace=True)
                all_tables.append(result_table)

        result_table = pd.concat(all_tables)

        result_table.to_excel('average_temperature_table.xls', engine='openpyxl', index=True)
        logging.info(f"File average_temperature_table.xls has been created.")


class DataAnalyzingTask:
    def __init__(self) -> None:
        self.cities_xls = "average_temperature_table.xls"

    def get_favorable_city(self):
        cities_xls = pd.read_excel(self.cities_xls)
        arrived_city = cities_xls['Рейтинг'].idxmax()
        favorable_city = cities_xls["Город"].iloc[arrived_city-1]
        temp_avg = cities_xls["Средняя температура"].iloc[arrived_city]
        relevant_cond_hours = cities_xls["Время без осадков"].iloc[arrived_city]

        print(f"{favorable_city} is the most favorable for travel")
        logging.info(f"The most favorable city was determined - {favorable_city}."  \
                     f" Average temperature is {temp_avg} and time without precipitation is {relevant_cond_hours}.")

if __name__ == '__main__':
    format = '%(asctime)s: %(message)s'
    logging.basicConfig(format=format, level=logging.INFO, datefmt='%H:%M:%S')

    new = DataAnalyzingTask()
    new.get_favorable_city()
