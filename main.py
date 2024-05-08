import json
import pymongo
import bson
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
            result[date_from] = []

            new_month = date_from.month + 1 #Определяем новый месяц и год если требуется
            new_year = date_from.year
            if new_month > 12:
                new_month = 1
                new_year += 1
            date_from = datetime(new_year, new_month, 1, date_from.hour, date_from.minute, date_from.second) #записываем результат

        elif group == 'year':
            date_from = datetime(date_from.year, 1, 1, date_from.hour, date_from.minute, date_from.second)
            result[date_from] = []
            new_year = date_from.year + 1
            date_from = datetime(new_year, 1, 1, date_from.hour, date_from.minute, date_from.second)
        else:
            result[date_from] = []
            date_from = date_from + options[group]

    return result


if __name__ == '__main__':
    with open('parametrs.json') as parametrs_file: #чтение файла параметров запуска
        parametrs = json.load(parametrs_file)
    parametrs_file.close()
    if parametrs['create_database']: #если необходимо то создать базу данных и скопировать туда данные из дампа
        mongodb_crete(parametrs['database_name'], parametrs['collection_name'], parametrs['path_to_dump_database'])

    with open('request.json', 'r') as request_json: #временная затычка для запросов
        request = json.load(request_json)
        request['dt_from'] = datetime.strptime(request['dt_from'], '%Y-%m-%dT%H:%M:%S')
        request['dt_upto'] = datetime.strptime(request['dt_upto'], '%Y-%m-%dT%H:%M:%S')
    request_json.close()
    raw_data = dict_to_fill(request)


    print("start")
