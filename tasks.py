import json
import logging
import os
import subprocess
import time
from multiprocessing import Process, Queue

import pandas as pd

from external.client import YandexWeatherAPI
from utils import get_url_by_city_name



class DataFetchingTask:

    @staticmethod
    def get_json(city_name: str) -> json:
        url_with_data = get_url_by_city_name(city_name)
        try:
            resp = YandexWeatherAPI.get_forecasting(url_with_data)
            return resp
        except Exception as e:
            logging.info(f'Error fetching data for {city_name}: {e}')
            return None

    def create_json(self, city_name: str) -> None:
        os.makedirs('json_cities', exist_ok=True)
        data = self.get_json(city_name)
        if data:
            with open(f'json_cities/{city_name}.json', 'w') as file:
                json.dump(data, file, indent=2)
                logging.info(f'File json_cities/{city_name}.json has been created.')


class DataCalculationTask(Process):
    def __init__(self, script_path, queue: Queue) -> None:
        super().__init__()
        self.queue = queue
        self.script_path = script_path
        self.input_folder = 'json_cities'
        self.output_folder = 'analyzed_cities'
    
    def run(self):
        os.makedirs(self.output_folder, exist_ok=True)

        json_files = [f for f in os.listdir(self.input_folder) if f.endswith('.json')]

        for input_file in json_files:
            input_file_path = os.path.join(self.input_folder, input_file)
            output_file_path = os.path.join(self.output_folder, f'{input_file}')

            command = ['python', self.script_path, '-i', input_file_path, '-o', output_file_path]

            try:
                subprocess.run(command, check=True)
                logging.info(f'Script {input_file}. Successful execution.')
                self.queue.put(input_file)
                print(f'{input_file} city has been added to the queue.') # Логирование не работает

            except Exception as e:
                logging.info(f'Script {input_file}. An error has occurred: {e}')


class DataAggregationTask(Process):
    def __init__(self, queue) -> None:
        super().__init__()
        self.queue = queue
        self.analyzed_cities_folder = 'analyzed_cities'

    def run(self) -> None: 

        all_tables = []

        while True:
            time.sleep(0.3) # Ждем пока очередь заполнится
            analyze_city = self.queue.get()
            print(f'{analyze_city} city received from the queue.')

            with open(os.path.join(self.analyzed_cities_folder, analyze_city), 'r') as city_json:
                city = json.load(city_json)
                df = pd.DataFrame(city['days'])
                original_data = df[['date', 'temp_avg', 'relevant_cond_hours']].copy()
                original_data.columns = ['Дата', 'Средняя температура', 'Время без осадков']
                original_data.insert(0, 'Город', str(analyze_city)[:-5])
                grouped_data = original_data.groupby('Город', as_index=False).aggregate({'Средняя температура': 'mean', 'Время без осадков': 'mean'})
                # Симпровизированный коэффициент для вычисления наилучшего города по погодным условиям
                grouped_data['Рейтинг'] = (grouped_data['Средняя температура'].mul(grouped_data['Время без осадков']))/100
                grouped_data = grouped_data.replace(str(analyze_city)[:-5],'Среднее значение')
                result_table = pd.concat([original_data, grouped_data], ignore_index = True)
                all_tables.append(result_table)
                
            result_table = pd.concat(all_tables)

            if self.queue.empty():
                print('Queue is empty')
                result_table.to_excel('average_temperature_table.xls', engine='openpyxl', index=True)
                logging.info(f'File average_temperature_table.xls has been created.')
                break


class DataAnalyzingTask:
    def __init__(self) -> None:
        self.cities_xls = 'average_temperature_table.xls'

    def get_favorable_city(self) -> None:
        cities_xls = pd.read_excel(self.cities_xls)
        arrived_city = cities_xls['Рейтинг'].idxmax()
        favorable_city = cities_xls['Город'].iloc[arrived_city-1]
        temp_avg = cities_xls['Средняя температура'].iloc[arrived_city]
        relevant_cond_hours = cities_xls['Время без осадков'].iloc[arrived_city]

        print(f'{favorable_city} is the most favorable for travel')
        logging.info(f'The most favorable city was determined - {favorable_city}.'  \
                     f' Average temperature is {temp_avg} and time without precipitation is {relevant_cond_hours}.')