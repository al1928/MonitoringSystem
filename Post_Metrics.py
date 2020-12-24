# coding=utf-8
import prometheus_client
import time
import psutil

UPDATE_PERIOD = 15
PIDMemory = prometheus_client.Gauge('PIDMemory',
                                    'Hold current system resource usage',
                                    ['resource_type'])
if __name__ == '__main__':
    # запуск сервера для отправки мектрик/ должен совпадать с localhost указанным в конфиге прометеуса - файл prometheus.yml
    prometheus_client.start_http_server(9099)

while True:
    count = 0
    for pid in psutil.pids():
        try:
            p = psutil.Process(pid)
            PIDMemory.labels(f"{p.name()}_"
                             f"CPU_persent").set(p.cpu_percent(interval=1))
            named_tuple_memory = p.memory_info()  # возвращает именнованный кортеж метрик memory_info()
            # rss - физ память (без подкачки)
            PIDMemory.labels(f"{p.name()}_"
                             f"{named_tuple_memory._fields[0]}").set(p.memory_info()[0])
            # vsm - виртуальная память (с подкачкой)
            PIDMemory.labels(f"{p.name()}_"
                             f"{named_tuple_memory._fields[1]}").set(p.memory_info()[1])
            # for i in range(12):
            #     named_tuple_memory = psutil.Process(pid).memory_info() #возвращает именнованный кортеж метрик memory_info()
            #     # PIDMemory.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
            #     #                  str(psutil.Process(pid).create_time()) + "_" + str(i)).set(psutil.Process(pid).memory_info_ex()[i])
            #     PIDMemory.labels(f"{psutil.Process(pid).name()}_{str(pid)}_{str(psutil.Process(pid).create_time())}_{named_tuple_memory._fields[i]}").set(psutil.Process(pid).memory_info()[i])

        except psutil.NoSuchProcess:
            print("процесса " + str(pid) + " не существует")
            count += 1
    print ("Количество пропавших id: " + str(count))
    time.sleep(UPDATE_PERIOD)