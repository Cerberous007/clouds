# Проект облачные вычисления
## Сбор данных из helpdesk Usedesk, прогнозирование нагрузки и поиск аномалий.

Основной код проекта разложен по нескольким директориям:
- ETL
- bash
- creds
- splunk_scripts

В папке ETL располагается надстройка над trino для подключения к БД (connectors.py)
Также располагается ряд скриптов для выгрузки данных, а также pipeline для прогнозирования (make_forecast_monthly.py)
В папке splunk_scripts располагается код репорта, который собирает график с доверительными интервалами по методу "2-х сигм"

Для настройки и запуска - необходимо следующее:
- Установить необходимые зависимости
```sh
pip install -r requirements.txt
```
- Получить логин и пароль для подключения к БД и внести в файл 
```sh
creds/dbcreds.py
```
- Получить Access token для api Usedesk и внести его в соответствующий файл
```sh
creds/usedesk_creds.py
```

Далее для отработки скриптов необходимо выполнять соответствующие bash-файлы из папки bash

Для автоматизации в данном случае используется простейщий крон-демон.

Для его настройки необходимо:
- Скопировать содержимое файла 
```sh
bash/cron_config.txt
```
- Открыть крон едитор:
```sh
crontab -e
```
- Вставить текст из буфера обмена
- При необходимости, можно перестроить время запуска скриптов.