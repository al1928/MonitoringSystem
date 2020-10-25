# MonitoringSystem

Запуск с нуля

1. Склонировать
2. Запустить prometeus/prometheus.exe
3. Запустить файл с метриками postMetrics.py
4. Запустить spark/bin/pyspark (возможно и без этого заработает)
5. Запустить exportData.py



! Если стоит уже prometheus:
	Заменить файл prometheus.yml на тот что в репозитории, либо поменять в своем файле порт приема метрик на "9099"
