import json
import pymongo
import bson



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


if __name__ == '__main__':
    with open('parametrs.json') as parametrs_file: #чтение файла параметров запуска
        parametrs = json.load(parametrs_file)
    parametrs_file.close()
    if parametrs['create_database']: #если необходимо то создать базу данных и скопировать туда данные из дампа
        mongodb_crete(parametrs['database_name'], parametrs['collection_name'], parametrs['path_to_dump_database'])


    print("start")
