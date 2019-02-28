'''
author:nick
date:2019/2/19
MIT license
'''

#import what we need
import requests
from bs4 import BeautifulSoup
import time
import re
import class_connect
import threading

#append the link
link_list=[]
for i in range(1,208):
    url='http://campus.chinahr.com/qz/P'+str(i)+'/?job_type=10&'
    link_list.append(url)

# instantiation
a = class_connect.spider()
collection = a.connect_to_mongodb()
cur, conn = a.connect_to_mysql()

#a class for thread
class myThread(threading.Thread):
    def __init__(self,name,link_range):
        threading.Thread.__init__(self)
        self.name=name
        self.link_range=link_range
    def run(self):
        print('Starting '+self.name)
        crawler(self.name,self.link_range)
        print('Exiting '+self.name)

# headers for url
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}

#the time for beginning
scrapy_time = time.time()

#our most important def
def crawler(threadName,link_range):
    cur, conn = a.connect_to_mysql()
    for i in range(link_range[0],link_range[1]+1):
        # get the url
        link = link_list[i-1]
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
            csvall=[job_and_company[0],job_and_company[1],salary,city,type]

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
                    csvall.append(the_final_info)

            #insert into mongodb and mysql
            collection.insert_one(all)
            cur.execute(
                "INSERT INTO yingcaiwang(job,company,salary,city,type,background,people) VALUES(%s,%s,%s,%s,%s,%s,%s);",
                (job_and_company[0], job_and_company[1], salary, city, type, back, peo))
            conn.commit()

        # rest for 3 seconds every 5 pages
        if i % 5 == 0:
            print(threadName+" : 第%s页爬取完毕，休息三秒" % (i))
            print('the %s page is finished,rest for three seconds' % (i))
            time.sleep(3)

        # rest for 1 second else
        else:
            print(threadName+" : 第%s页爬取完毕，休息一秒" % (i))
            print('the %s page is finished,rest for one second' % (i))
            time.sleep(1)

#out thread range list
thread_list=[]
link_range_list=[(1,40),(41,80),(81,120),(121,160),(161,207)]

#start the threads
for i in range(1,6):
    thread=myThread('Thread-'+str(i),link_range_list[i-1])
    thread.start()
    thread_list.append(thread)

#wait for ending
for thread in thread_list:
    thread.join()

#take the time
scrapy_end = time.time()
scrapy_time_whole = scrapy_end - scrapy_time
print('it takes {}'.format(scrapy_time_whole))

# commit the change to mysql
cur.close()
conn.commit()
conn.close()