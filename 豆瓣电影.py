# -*- coding: utf-8 -*-
"""
Created on Thu Dec 26 14:08:40 2019
前150电影信息
详情链接 图片链接 影片中文名 外国名 评分 评价数 概况 导演 主演 年份 地区 类别
按评分排名 top10评分柱状图
评论数第一 评论内容词云
@author: Mario
"""
import time
import requests
from bs4 import BeautifulSoup
import urllib.request
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

def get_urls():
    urls = []
    url = 'https://movie.douban.com/top250?start=0&filter='
    urls.append(url)
    headers ={
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'       
            }
    r = requests.get(url, headers=headers, timeout=300)
    soup = BeautifulSoup(r.text,'lxml')
    lis = soup.find('div',attrs={'class':'paginator'})
    for i in lis.find_all('a')[:9]:
        urls.append('https://movie.douban.com/top250'+i['href'])
    return urls
        
def get_moves(url):
    headers ={
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'       
            }
    detail_urls = []
    china_names = []
    pic_urls = []
    english_names = []
    move_ranks = []
    inqs = []
    r = requests.get(url, headers=headers, timeout=300)
    soup = BeautifulSoup(r.text,'lxml')
    lis = soup.find_all('div',attrs={'class':'item'})
    for i in lis:
        detail_url = list(i)[1].a['href']
        china_name = list(i)[1].a.img['alt']
        pic_url = list(i)[1].a.img['src']
        english_name = list(list(i)[3].div.a)[3].string.strip('\xa0/\xa0')
        detail_urls.append(detail_url)
        china_names.append(china_name)
        pic_urls.append(pic_url)
        english_names.append(english_name)
    ranks = soup.find_all('em')
    for rank in ranks:
        move_ranks.append(int(rank.text))
    for inq in soup.find_all('span',attrs={'class':'inq'}):
        inqs.append(inq.text)
    moves = [china_names,english_names,detail_urls,pic_urls,move_ranks,inqs]
    return moves

def download_picture(p_name,p_url):
    urllib.request.urlretrieve(p_url, '%s.jpg' % p_name)

print('*' * 50)
t1 = time.time()
urls = get_urls()
# 利用并发获取电影信息
executor = ThreadPoolExecutor(max_workers=10)  # 可以自己调整max_workers,即线程的个数
# submit()的参数： 第一个为函数， 之后为该函数的传入参数，允许有多个
future_tasks = [executor.submit(get_moves, url) for url in urls]
# 等待所有的线程完成，才进入后续的执行
wait(future_tasks, return_when=ALL_COMPLETED)
m0 = m1 = m2 = m3 = m4 = m5 = []
for future_task in future_tasks:
    t = future_task.result()
    moves = [t[0]+m0,t[1]+m1,t[2]+m2,t[3]+m3,t[4]+m4,t[5]+m5]
    m0 = moves[0]
    m1 = moves[1]
    m2 = moves[2]
    m3 = moves[3]
    m4 = moves[4]
    m5 = moves[5]
# 利用并发下载电影图片
executor = ThreadPoolExecutor(max_workers=10)
future_tasks = [executor.submit(download_picture, p_name,p_url) for p_name,p_url in zip(moves[0],moves[3])]
wait(future_tasks, return_when=ALL_COMPLETED)
df = pd.DataFrame(moves)
df = df.T
df.columns = ['电影中文名','电影英文名','电影详情链接','电影图片链接','电影排名','短评']
df = df[['电影排名','电影中文名','电影英文名','短评','电影详情链接','电影图片链接']]
df.sort_values('电影排名',inplace = True)
df.to_csv('豆瓣电影.csv',encoding = 'utf-8-sig',index = False)

t2 = time.time()
print(t2-t1)
print('*' * 50)

    
    
