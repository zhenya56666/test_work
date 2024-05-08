import json
import pymongo
import bson
import telebot
import asyncio
from datetime import datetime, timedelta



def mongodb_crete(database_name, collection_name, path_to_dump_database=None):
    client = pymongo.MongoClient("mongodb://localhost:27017/")  #подключение к mongodb

    database = client[database_name]  #создание базы данных и коллекции
    collection = database[collection_name]

    if path_to_dump_database is not None:
        with open(path_to_dump_database, "rb") as dump_database:  #копирование данных в базу из дампа
            dump_data = bson.decode_file_iter(dump_database)
            for data in dump_data:
                del data['_id'] #удаление id из дампа
                if not collection.find(data): #добавление в базу данных уникальных данных из дампа
                    collection.insert_one(data)
        dump_database.close()
    client.close()

def dict_to_fill(request): #функция создает словарь который в дальнейшем будет заполнятся данными из базы данных
    date_from = request['dt_from']
    date_to = request['dt_upto']
    group = request['group_type']
    result = {}
    options = {
        'hour': timedelta(hours=1),
        'day': timedelta(days=1)
    }
    while date_from <= date_to:
        if group == 'month':
            date_from = datetime(date_from.year, date_from.month, 1, date_from.hour, date_from.minute, date_from.second) #меняем дату на начало месяца
            result[date_from] = 0

            new_month = date_from.month + 1 #Определяем новый месяц и год если требуется
            new_year = date_from.year
            if new_month > 12:
                new_month = 1
                new_year += 1
            date_from = datetime(new_year, new_month, 1, date_from.hour, date_from.minute, date_from.second) #записываем результат

        elif group == 'year':
            date_from = datetime(date_from.year, 1, 1, date_from.hour, date_from.minute, date_from.second)
            result[date_from] = 0
            new_year = date_from.year + 1
            date_from = datetime(new_year, 1, 1, date_from.hour, date_from.minute, date_from.second)
        else:
            result[date_from] = 0
            date_from = date_from + options[group]

    return result

def response(request, database_name, collection_name): #функция формирования ответа
    raw_data = dict_to_fill(request)

    client = pymongo.MongoClient("mongodb://localhost:27017/")  # подключение к mongodb
    database = client[database_name]
    collection = database[collection_name]

    date_from = request['dt_from']
    date_to = request['dt_upto']
    group = request['group_type']
    request_tobd = {"dt": {"$gte": date_from, "$lte": date_to}} #запрос к mongodb
    cursor_bd = collection.find(request_tobd)

    # заполняем данные
    for data in cursor_bd:  # цикл прохождения по выданным данным из бд
        if date_from <= data['dt'] <= date_to:  # дополнительная проверка, коректные ли данные пришли из бд
            for it in raw_data:  # цикл для определения куда именно положить данные
                options = {  # опции сравнения в зависимости от указанной группы агригации данных
                    'year': it.year == data['dt'].year,
                    'month': it.year == data['dt'].year and it.month == data['dt'].month,
                    'day': it.year == data['dt'].year and it.month == data['dt'].month and it.day == data['dt'].day,
                    'hour': it.year == data['dt'].year and it.month == data['dt'].month and it.day == data[
                        'dt'].day and it.hour == data['dt'].hour,
                }
                if options[group]:
                    raw_data[it] += int(data['value'])
    client.close()

    #формирование ответа
    dataset = []
    for it in raw_data:
        dataset.append(raw_data[it])

    result = {'dataset': [], 'labels': []}
    for it in raw_data.keys():
        result['labels'].append(datetime.strftime(it, '%Y-%m-%dT%H:%M:%S'))
    result['dataset'] = dataset.copy()

    return result

async def start_bot():
    # Запуск бота в асинхронном режиме
    bot.polling(none_stop=True)

if __name__ == '__main__':
    with open('parametrs.json') as parametrs_file: #чтение файла параметров запуска
        parametrs = json.load(parametrs_file)
    parametrs_file.close()
    if parametrs['create_database']: #если необходимо то создать базу данных и скопировать туда данные из дампа
        mongodb_crete(parametrs['database_name'], parametrs['collection_name'], parametrs['path_to_dump_database'])

    bot = telebot.TeleBot(parametrs['Telegram_Bot_Api_Key'])
    @bot.message_handler(commands=['start']) #вывод приветсвенного сообщения
    def start_message_bot(message):
        bot.send_message(message.chat.id, 'Привет! отправь мне сообщение в следующем формате: \n{\n"dt_from":"2022-9-01T00:00:00",\n"dt_upto":"2022-12-31T23:59:00",\n"group_type":"month"\n}')

    @bot.message_handler(content_types=['text']) #функция обработки запроса с проверкой корректности запроса
    def response_bot(message):
        try:
            request = json.loads(message.text)
            request['dt_from'] = datetime.strptime(request['dt_from'], '%Y-%m-%dT%H:%M:%S')
            request['dt_upto'] = datetime.strptime(request['dt_upto'], '%Y-%m-%dT%H:%M:%S')
            mes = json.dumps(response(request, parametrs['database_name'], parametrs['collection_name']))
            bot.send_message(message.chat.id, mes)
        except:
            bot.send_message(message.chat.id, 'Не верный запрос!')

    loop = asyncio.get_event_loop()
    loop.create_task(start_bot())
    loop.run_forever()
