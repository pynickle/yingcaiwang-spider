from pymongo import MongoClient
import pymysql

# a class for connect
class spider:
    # some things for mongodb and mysql
    def __init__(self, data=''):
        self.host = 'localhost'
        self.port = 27017
        self.user = 'root'
        self.passwd = 'nick2005'
        self.db = 'scraping'
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
        conn = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, charset=self.charset)
        cur = conn.cursor()
        cur.execute('truncate table yingcaiwang;')
        return cur, conn
