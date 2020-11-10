# coding=utf-8
import prometheus_client
import time
import psutil

UPDATE_PERIOD = 15
# Зачем нужно всё закоменченное ниже??
# NETWORK_bytes = prometheus_client.Gauge('NETWORK_bytes',
#                                         'Hold current system resource usage',
#                                         ['resource_type'])

# NETWORK_packets = prometheus_client.Gauge('NETWORK_packets',
#                                           'Hold current system resource usage',
#                                           ['resource_type'])

# NETWORK_errors = prometheus_client.Gauge('NETWORK_errors',
#                                          'Hold current system resource usage',
#                                          ['resource_type'])

# NETWORK_drops = prometheus_client.Gauge('NETWORK_drops',
#                                         'Hold current system resource usage',
#                                         ['resource_type'])

# MEMORY = prometheus_client.Gauge('MEMORY',
#                                  'Hold current system resource usage',
#                                  ['resource_type'])

# DISK_usage = prometheus_client.Gauge('DISK_usage',
#                                      'Hold current system resource usage',
#                                      ['resource_type'])

# DISK_RW_count = prometheus_client.Gauge('DISK_RW_count',
#                                         'Hold current system resource usage',
#                                         ['resource_type'])

# DISK_RW_bytes = prometheus_client.Gauge('DISK_RW_bytes',
#                                         'Hold current system resource usage',
#                                         ['resource_type'])

# DISK_RW_time = prometheus_client.Gauge('DISK_RW_time',
#                                        'Hold current system resource usage',
#                                        ['resource_type'])

# SYSTEM = prometheus_client.Gauge('SYSTEM_percent',
#                                  'Hold current system resource usage',
#                                  ['resource_type'])

PIDMemory = prometheus_client.Gauge('PIDMemory',
                                    'Hold current system resource usage',
                                    ['resource_type'])
if __name__ == '__main__':
    # запуск сервера для отправки мектрик/ должен совпадать с localhost указанным в конфиге прометеуса - файл prometheus.yml
    prometheus_client.start_http_server(9099)

while True:
    # и это тоже зачем?

    # NETWORK_bytes.labels('bytes_sent').set(psutil.net_io_counters(pernic=False)[0])  # количество отправленных байтов
    # NETWORK_bytes.labels('bytes_recv').set(psutil.net_io_counters(pernic=False)[1])  # количество принятых байтов

    # NETWORK_packets.labels('packets_sent').set(
    #     psutil.net_io_counters(pernic=False)[2])  # количество отправленных байтов
    # NETWORK_packets.labels('packets_recv').set(psutil.net_io_counters(pernic=False)[3])  # количество принятых байтов

    # NETWORK_errors.labels('errors_sent').set(psutil.net_io_counters(pernic=False)[4])  # количество ошибок при отправке
    # NETWORK_errors.labels('errors_recv').set(psutil.net_io_counters(pernic=False)[5])  # количество ошибок при получении

    # NETWORK_drops.labels('drop_in').set(
    #     psutil.net_io_counters(pernic=False)[6])  # количесвто пришедших пакетов, которые были отброшены
    # NETWORK_drops.labels('drop_out').set(
    #     psutil.net_io_counters(pernic=False)[7])  # количество отправленных пакетов, которые были отброшены

    # MEMORY.labels('total_memory').set(psutil.virtual_memory()[0])  # общее количество физической памяти
    # MEMORY.labels('available_memory').set(psutil.virtual_memory()[1])  # доступная память для процессов
    # MEMORY.labels('used_memory').set(psutil.virtual_memory()[3])  # количество используемой памяти
    # MEMORY.labels('free_memory').set(psutil.virtual_memory()[4])  # свободная память

    # DISK_usage.labels('disk_total').set(psutil.disk_usage('/')[0])  # Общий объем диска
    # DISK_usage.labels('disk_used').set(psutil.disk_usage('/')[1])  # Используемый объем диска
    # DISK_usage.labels('disk_free').set(psutil.disk_usage('/')[2])  # Свободный объем диска

    # DISK_RW_count.labels('read_count').set(psutil.disk_io_counters()[0])  # количество чтений
    # DISK_RW_count.labels('write_count').set(psutil.disk_io_counters()[1])  # количество записей

    # DISK_RW_bytes.labels('read_bytes').set(psutil.disk_io_counters()[2])  # количество прочитанных байт
    # DISK_RW_bytes.labels('write_bytes').set(psutil.disk_io_counters()[3])  # количество записанных байт

    # DISK_RW_time.labels('read_time').set(psutil.disk_io_counters()[4])  # время чтения с диска в мс
    # DISK_RW_time.labels('write_time').set(psutil.disk_io_counters()[5])  # время записи с диска в мс

    # SYSTEM.labels('used_CPU_percent').set(psutil.cpu_percent())  # цпу_пёрсент
    # SYSTEM.labels('used_disk_percent').set(psutil.disk_usage('/')[3])  # процент использования диска
    # SYSTEM.labels('used_memory_percent').set(
    #     psutil.virtual_memory()[2])  # процент использования памяти, (total - available) / total * 100.

    # print len(psutil.Process(12196).memory_info_ex())
    # print len(psutil.pids())
    # print psutil.Process("browser.exe").name()

    # print psutil.pids()
    count = 0
    for pid in psutil.pids():
        try:
            for i in range(12):
                named_tuple = psutil.Process(pid).memory_info() #возвращает именнованный кортеж метрик memory_info()
                # PIDMemory.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                #                  str(psutil.Process(pid).create_time()) + "_" + str(i)).set(psutil.Process(pid).memory_info_ex()[i])
                PIDMemory.labels(f"{psutil.Process(pid).name()}_{str(pid)}_{str(psutil.Process(pid).create_time())}_{named_tuple._fields[i]}").set(psutil.Process(pid).memory_info()[i])

        except psutil.NoSuchProcess:
            print("процесса " + str(pid) + " не существует")
            count += 1
    print ("Количество пропавших id: " + str(count))
    time.sleep(UPDATE_PERIOD)
