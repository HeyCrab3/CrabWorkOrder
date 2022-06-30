import pymongo, os, json, datetime, random
from bson import ObjectId

ip = '127.0.0.1'
port = 27017
database = 'peanutio'

client = pymongo.MongoClient(ip, port)
db = None


def get_db():
    global db
    if not db:
        db = client[database]
    return db


db = get_db()

data_path = './data'

def data_import():
    coll_list = db.list_collection_names()
    for collection in coll_list:
        # 删除集合
        db[collection].drop()
    for maindir, subdir, file_list in os.walk(data_path):
        for file_name in file_list:
            if file_name[file_name.rindex('.'):] == '.json':
                coll = file_name[:file_name.rindex('.')]

                with open(data_path + '/' + file_name, encoding='utf-8') as file:
                    str = file.read()
                    if str is '' or str is None:
                        continue
                    else:
                        data = []
                        data.extend(json.loads(str))
                        if coll == 'user':
                            for d in data:
                                d['_id'] = ObjectId(d['_id'])
                                d['userName'] = data['userName']
                                d['passWord'] = data['passWord']
                                d['perm'] = data['perm']
                        if coll == 'workorder':
                            for d in data:
                                d['_id'] = ObjectId(d['_id'])
                                d['author']['_id'] = ObjectId(d['author']['_id'])
                                d['content'] = d['content']
                                d['create_time'] = d['create_time']
                        db[coll].insert_many(data)
