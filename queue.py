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

count=1

#get the link
link_list=[]
for i in range(1,208):
    url='http://campus.chinahr.com/qz/P'+str(i)+'/?job_type=10&'
    link_list.append(url)

# instantiation
a = class_connect.spider()
collection = a.connect_to_mongodb()
cur, conn = a.connect_to_mysql()

#a class for more threads
class myThread(threading.Thread):
    def __init__(self,name,q):
        threading.Thread.__init__(self)
        self.name=name
        self.q=q

    #run
    def run(self):
        print('Starting '+self.name)
        while True:
            try:
                crawler(self.name,self.q)
            except:
                break
        print('Exiting'+self.name)

# headers for url
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

#a time for beginning
scrapy_time = time.time()

#the most important def
def crawler(threadName,q):
    cur, conn = a.connect_to_mysql()
    # get the url
    url=q.get(timeout=2)
    link = url
    r = requests.get(link, headers=headers, timeout=20)

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
                back = the_final_info
                csvall.append(the_final_info)
            if each == 4:
                all['people'] = the_final_info
                peo = the_final_info

        # insert into mongodb and mysql

        collection.insert_one(all)
        cur.execute(
            "INSERT INTO yingcaiwang(job,company,salary,city,type,background,people) VALUES(%s,%s,%s,%s,%s,%s,%s);",
            (job_and_company[0], job_and_company[1], salary, city, type, back, peo))

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