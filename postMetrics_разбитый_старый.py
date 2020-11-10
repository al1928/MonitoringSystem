# coding=utf-8

import prometheus_client
import time
import psutil

UPDATE_PERIOD = 15

NETWORK_bytes = prometheus_client.Gauge('NETWORK_bytes',
                                        'Hold current system resource usage',
                                        ['resource_type'])

NETWORK_packets = prometheus_client.Gauge('NETWORK_packets',
                                          'Hold current system resource usage',
                                          ['resource_type'])

NETWORK_errors = prometheus_client.Gauge('NETWORK_errors',
                                         'Hold current system resource usage',
                                         ['resource_type'])

NETWORK_drops = prometheus_client.Gauge('NETWORK_drops',
                                        'Hold current system resource usage',
                                        ['resource_type'])

MEMORY = prometheus_client.Gauge('MEMORY',
                                 'Hold current system resource usage',
                                 ['resource_type'])

DISK_usage = prometheus_client.Gauge('DISK_usage',
                                     'Hold current system resource usage',
                                     ['resource_type'])

DISK_RW_count = prometheus_client.Gauge('DISK_RW_count',
                                        'Hold current system resource usage',
                                        ['resource_type'])

DISK_RW_bytes = prometheus_client.Gauge('DISK_RW_bytes',
                                        'Hold current system resource usage',
                                        ['resource_type'])

DISK_RW_time = prometheus_client.Gauge('DISK_RW_time',
                                       'Hold current system resource usage',
                                       ['resource_type'])

SYSTEM = prometheus_client.Gauge('SYSTEM_percent',
                                 'Hold current system resource usage',
                                 ['resource_type'])

PIDMemory = prometheus_client.Gauge('PIDMemory',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_rss= prometheus_client.Gauge('ProcessesMemory_rss',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_vms= prometheus_client.Gauge('ProcessesMemory_vms',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_num_page_faults= prometheus_client.Gauge('ProcessesMemory_num_page_faults',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_peak_wset= prometheus_client.Gauge('ProcessesMemory_peak_wset',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_wset= prometheus_client.Gauge('ProcessesMemory_wset',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_peak_paged_pool= prometheus_client.Gauge('ProcessesMemory_peak_paged_pool',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_paged_pool= prometheus_client.Gauge('ProcessesMemory_paged_pool',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_peak_nonpaged_pool= prometheus_client.Gauge('ProcessesMemory_peak_nonpaged_pool',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_nonpaged_pool= prometheus_client.Gauge('ProcessesMemory_nonpaged_pool',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_pagefile= prometheus_client.Gauge('ProcessesMemory_pagefile',
                                    'Hold current system resource usage',
                                    ['resource_type'])

ProcessesMemory_peak_pagefile= prometheus_client.Gauge('ProcessesMemory_peak_pagefile',
                                    'Hold current system resource usage',
                                    ['resource_type'])


ProcessesMemory_private= prometheus_client.Gauge('ProcessesMemory_private',
                                    'Hold current system resource usage',
                                    ['resource_type'])

if __name__ == '__main__':
    prometheus_client.start_http_server(9099) # localhost который указан в файле prometheus.yml

while True:
    NETWORK_bytes.labels('bytes_sent').set(psutil.net_io_counters(pernic=False)[0])  # количество отправленных байтов
    NETWORK_bytes.labels('bytes_recv').set(psutil.net_io_counters(pernic=False)[1])  # количество принятых байтов

    NETWORK_packets.labels('packets_sent').set(
        psutil.net_io_counters(pernic=False)[2])  # количество отправленных байтов
    NETWORK_packets.labels('packets_recv').set(psutil.net_io_counters(pernic=False)[3])  # количество принятых байтов

    NETWORK_errors.labels('errors_sent').set(psutil.net_io_counters(pernic=False)[4])  # количество ошибок при отправке
    NETWORK_errors.labels('errors_recv').set(psutil.net_io_counters(pernic=False)[5])  # количество ошибок при получении

    NETWORK_drops.labels('drop_in').set(
        psutil.net_io_counters(pernic=False)[6])  # количесвто пришедших пакетов, которые были отброшены
    NETWORK_drops.labels('drop_out').set(
        psutil.net_io_counters(pernic=False)[7])  # количество отправленных пакетов, которые были отброшены

    MEMORY.labels('total_memory').set(psutil.virtual_memory()[0])  # общее количество физической памяти
    MEMORY.labels('available_memory').set(psutil.virtual_memory()[1])  # доступная память для процессов
    MEMORY.labels('used_memory').set(psutil.virtual_memory()[3])  # количество используемой памяти
    MEMORY.labels('free_memory').set(psutil.virtual_memory()[4])  # свободная память

    DISK_usage.labels('disk_total').set(psutil.disk_usage('/')[0])  # Общий объем диска
    DISK_usage.labels('disk_used').set(psutil.disk_usage('/')[1])  # Используемый объем диска
    DISK_usage.labels('disk_free').set(psutil.disk_usage('/')[2])  # Свободный объем диска

    DISK_RW_count.labels('read_count').set(psutil.disk_io_counters()[0])  # количество чтений
    DISK_RW_count.labels('write_count').set(psutil.disk_io_counters()[1])  # количество записей

    DISK_RW_bytes.labels('read_bytes').set(psutil.disk_io_counters()[2])  # количество прочитанных байт
    DISK_RW_bytes.labels('write_bytes').set(psutil.disk_io_counters()[3])  # количество записанных байт

    DISK_RW_time.labels('read_time').set(psutil.disk_io_counters()[4])  # время чтения с диска в мс
    DISK_RW_time.labels('write_time').set(psutil.disk_io_counters()[5])  # время записи с диска в мс

    SYSTEM.labels('used_CPU_percent').set(psutil.cpu_percent())  # цпу_пёрсент
    SYSTEM.labels('used_disk_percent').set(psutil.disk_usage('/')[3])  # процент использования диска
    SYSTEM.labels('used_memory_percent').set(
        psutil.virtual_memory()[2])  # процент использования памяти, (total - available) / total * 100.


    count = 0
    # pid_list = psutil.pids()
    # print(type(pid_list), pid_list)
    for pid in psutil.pids():
        try:
            ProcessesMemory_rss.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_rss").set(psutil.Process(pid).memory_info()[0])

            ProcessesMemory_vms.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_vms").set(psutil.Process(pid).memory_info()[1])

            ProcessesMemory_num_page_faults.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_num_page_faults").set(psutil.Process(pid).memory_info()[2])

            ProcessesMemory_peak_wset.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_peak_wset").set(psutil.Process(pid).memory_info()[3])

            ProcessesMemory_wset.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_wset").set(psutil.Process(pid).memory_info()[4])

            ProcessesMemory_peak_paged_pool.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_peak_paged_pool").set(psutil.Process(pid).memory_info()[5])

            ProcessesMemory_paged_pool.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_paged_pool").set(psutil.Process(pid).memory_info()[6])

            ProcessesMemory_peak_nonpaged_pool.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_peak_nonpaged_pool").set(psutil.Process(pid).memory_info()[7])

            ProcessesMemory_nonpaged_pool.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_nonpaged_pool").set(psutil.Process(pid).memory_info()[8])

            ProcessesMemory_pagefile.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_pagefile").set(psutil.Process(pid).memory_info_ex()[9])

            ProcessesMemory_peak_pagefile.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_peak_pagefile").set(psutil.Process(pid).memory_info_ex()[10])

            ProcessesMemory_private.labels(psutil.Process(pid).name() + "_" + str(pid) + "_" +
                                 str(psutil.Process(pid).create_time()) + "_ private").set(psutil.Process(pid).memory_info_ex()[11])

        except psutil.NoSuchProcess:
            print("процесса " + str(pid) + " не существует")
            count += 1
    print ("Количество пропавших (или пропавших частично) id: " + str(count))
    time.sleep(UPDATE_PERIOD)
