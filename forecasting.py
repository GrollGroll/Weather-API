import logging
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES

if __name__ == '__main__':
    format = '%(asctime)s: %(message)s'
    logging.basicConfig(format=format, level=logging.INFO, datefmt='%H:%M:%S')

    with ThreadPoolExecutor() as pool:
        results = pool.map(DataFetchingTask().create_json, CITIES)

    queue = Queue()
    script_path = 'external/analyzer.py'
    process_producer = DataCalculationTask(script_path, queue)
    process_consumer = DataAggregationTask(queue)
    process_producer.start()
    process_consumer.start()
    process_producer.join()
    process_consumer.join() 

    DataAnalyzingTask().get_favorable_city()