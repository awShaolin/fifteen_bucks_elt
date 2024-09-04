# Fifteen Bucks ELT Pipline

Пайплайн по загрузке данных из API в сырой слой аналитического хранилища.

## Installation

### Создание Docker образов:

В корневой директории проекта запускаем docker-compose в фоновом режиме:
```bash
docker-compose up -d
```
Проверка запущенных контейнеров (все сервисы должны иметь статус healthy):
```bash
docker ps
```
При возникновении ошибок во время создания образов, можно воспользоваться командой для просмотра логов: 
 ```bash
docker logs <containerID>
```

### Создание таблицы в DWH:

Подключиться к сервису dwh-postgres через `psql`, dbeaver или любой другой клиент.

Запустить ddl из файла `sql/ddl.sql`

## Prerequisites

- Docker ([download link](https://www.docker.com/products/docker-desktop/)) 
- docker-compose ([download link](https://docs.docker.com/compose/install/)) 

## Services

### Airflow
`Airflow` нужен для оркестрации задач по загрузке данных в DWH и включает в себя следующие сервисы: 
- `postgres` - используется для хранения метаданных Airflow
- `redis` - обработка очередей задач 
- `airflow-webserver` - отвечает за веб-интерфейс
- `airflow-scheduler` - планировщик задач
- `airflow-worker` - исполнитель задач 

> Airflow WEB - http://localhost:8080

### DWH (PostgreSQL)
Отдельная бд, которая играет роль аналитического хранилища данных. Данные пишутся из Airflow на сырой слой `raw`.

## DAGs
 Краткое описание работы `dag` по загрузке данных из API.

`dag` состоит из следующих тасок:

| task_id | Порядок выполнения | Описание |
|---------|--------------------|----------|
| wait_for_cannabis_api | t1 | `HttpSensor` для проверки доступности API |
| fetch_data | t2 | `PythonOperator`, который вызывает функцию `fetch_data` из файла `dags/scripts/fifteen_bucks_elt.py`. Функция делает GET запрос к API и возвращает json с данными|
| parse_data_to_df | t3 | `PythonOperator` который вызывает функцию `fetch_data` из файла `dags/scripts/fifteen_bucks_elt.py`. Cоставляет из json - pandas DataFrame |
| insert_data_into_dwh | t4 | `PythonOperator`, который вызывает функцию `fetch_data` из файла `dags/scripts/fifteen_bucks_elt.py`. Отфильтровывает DataFrame, оставляя только id, которых нет в бд и вставляет данные в таблицу при помощи `sqlalchemy`