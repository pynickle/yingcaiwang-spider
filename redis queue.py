'''
author:nick
date:2019/2/21
MIT license
'''

#import what we need
import requests
from bs4 import BeautifulSoup
import time
import re
import class_connect
import threading
import queue as Queue
from redis import Redis
import os

#get the link
link_list=[]
for i in range(1,208):
    url='http://campus.chinahr.com/qz/P'+str(i)+'/?job_type=10&'
    link_list.append(url)

#push the link to slaves
def push_redis_list():
    #connect to redis
    r = Redis(host='127.0.0.1', port=6379)

    #push the url
    for url in link_list:
        r.lpush('imformation',url)
        re_url=re.search(r'http://campus.chinahr.com/qz/P(.*?)/?job_type=10&',url)
        re_url=re_url.group(1)
        print('append : '+re_url)
    return

#a class for more threads
class myThread(threading.Thread):
    def __init__(self,name,q):
        threading.Thread.__init__(self)
        self.name=name
        self.q=q

    #rewrite run method
    def run(self):
        print('Starting '+self.name)
        while True:
            try:
                crawler(self.name,self.q)
            except:
                break
        print('Exiting'+self.name)

# instantiation
a = class_connect.spider()
collection = a.connect_to_mongodb()
cur, conn = a.connect_to_mysql()

# headers for url
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

#the most important def
def crawler(threadName,q):

    #a time for beginning
    scrapy_time = time.time()

    #connect to redis and mysql
    r = Redis(host='127.0.0.1', port=6379)
    cur, conn = a.connect_to_mysql()

    #try to get the url
    while True:
        try:
            # get the url
            url = r.lpop('imformation')
            url = url.decode('ascii')
            print(url)
            if url==None:
                return

            try:
                r = requests.get(url, headers=headers, timeout=20)

                # lxml and beautifulsoup
                soup = BeautifulSoup(r.text, "lxml")

                # find what we need
                salary_list = soup.find_all('strong', class_='job-salary')
                city_list = soup.find_all('span', class_="job-city Fellip")
                top_list = soup.find_all('div', class_="top-area")
                job_info = soup.find_all('div', class_='job-info')
                type_list = soup.find_all('span', class_='industry-name')

                # every information for recruit
                for x in range(len(top_list)):

                    # get the text by strip
                    salary = salary_list[x].text.strip()
                    city = city_list[x].text.strip()
                    top = top_list[x].text.strip()
                    job_and_company = top.split('\n', 1)
                    job_information = job_info[x].text.strip()
                    city_to_people = job_information.split('\n')
                    type = type_list[x].text.strip()

                    # the dictionary for mongodb and the list for csv
                    all = {"job": job_and_company[0],
                           "company": job_and_company[1],
                           "salary": salary,
                           "city": city,
                           "type": type}

                    # get the background and people
                    for each in range(0, 5):

                        #with re's help
                        first = re.compile(r'  ')
                        time_for_sub = first.sub('', city_to_people[each])
                        another = re.compile(r'/')
                        the_final_info = another.sub('', time_for_sub)

                        # get the background and people
                        if each == 3:
                            all['background'] = the_final_info
                            back=the_final_info
                        if each == 4:
                            all['people'] = the_final_info
                            peo=the_final_info

                    #insert into mongodb and mysql
                    collection.insert_one(all)
                    cur.execute(
                        "INSERT INTO yingcaiwang(job,company,salary,city,type,background,people) VALUES(%s,%s,%s,%s,%s,%s,%s);",
                        (job_and_company[0], job_and_company[1], salary, city, type, back, peo))
                    conn.commit()

                k=re.search('P(.*)/',url).group(1)

                #rest for 3 seconds every 5 pages
                if i % 5 == 0:
                    print(threadName+" : 第%s页爬取完毕，休息三秒" % (k))
                    print('the %s page is finished,rest for three seconds' % (k))
                    time.sleep(3)

                # rest for 1 second else
                else:
                    print(threadName+" : 第%s页爬取完毕，休息一秒" % (k))
                    print('the %s page is finished,rest for one second' % (k))
                    time.sleep(1)

            except Exception as e:
                print(e)
                print('problem!')

        #url is finished break
        except:
            break
    return

def get_url():

    #connect to redis
    r = Redis(host='127.0.0.1', port=6379)

    #our threads and queue
    threadList=['Thread-1','Thread-2','Thread-3','Thread-4','Thread-5']
    workQueue=Queue.Queue(1000)
    threads=[]

    #start the threads
    for tName in threadList:
        thread=myThread(tName,workQueue)
        thread.start()
        threads.append(thread)

    #put the url
    for url in link_list:
        workQueue.put(url)

    #wait for end
    for t in threads:
        t.join()

    #take the time
    scrapy_end = time.time()
    scrapy_time_whole = scrapy_end - scrapy_time
    print('it takes {}'.format(scrapy_time_whole))

    # commit the change to mysql
    cur.close()
    conn.commit()
    conn.close()

if __name__=='__main__':
    this_machine = 'master'
    print('start redis spider')
    if this_machine == 'master':
        push_redis_list()
    else:
        get_url()