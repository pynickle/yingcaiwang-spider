from pymongo import MongoClient
import pymysql

# a class for connect
class spider:
    # some things for mongodb and mysql
    def __init__(self, data=''):
        self.host = 'localhost'
        self.port = 27017
        self.user = 'root'
        self.passwd = 'passwd'
        self.db = 'db'
        self.charset = 'utf8'
        self.data = data

    # connect to mongodb and remove all
    def connect_to_mongodb(self):
        # connect the mongodb and remove all
        client = MongoClient(host=self.host, port=self.port)
        db = client.blog_database
        collection = db.blog
        collection.remove({})
        return collection

    # connect the mysql and remove all
    def connect_to_mysql(self):
        conn = pymysql.connect(host="localhost", user="root", passwd="nick2005", db="scraping", charset='utf8')
        cur = conn.cursor()
        cur.execute('truncate table yingcaiwang;')
        return cur, conn

    # collection get
    def get_data(self, __data):
        self.data = __data
