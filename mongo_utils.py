from pymongo import MongoClient
from bson import ObjectId
import configparser
config = configparser.ConfigParser()
config.read('mongo.config')
hostname=config.get("DEFAULT","hostname")
username=config.get("DEFAULT","username")
password=config.get("DEFAULT","password")
db=config.get("DEFAULT","db")
try:
    host_base_url = 'mongodb://{user_name}:{password}@{ip}/?authSource={db}'.format(user_name=username,password=password,ip=hostname, db=db)
    mongodb = MongoClient(host=host_base_url)[db]
    print('Connected to MongoDB...')

except Exception as e:
    print('Failed to connect to MongoDB...')
    traceback.print_exc()
    raise RuntimeError('Failed to connect to MongoDB...')