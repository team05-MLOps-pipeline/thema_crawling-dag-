from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import pyarrow as pa 
from datetime import datetime, timedelta
from confluent_kafka import Consumer, TopicPartition
from hdfs import InsecureClient
import json
import pandas as pd
import logging
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import random


# 로그 생성 및 설정 
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)




def cleand_text(text):
    # 별 제거
    text = text.replace('*', '')
    # 공백 삭제
    text = text.strip()

    return text



def make_urllist():
    page_num = 0
    first_url = ""
    urllist= []
    txtlist= []
    first_flag = 0
    last_flag = 0
    
    for i in range(100):
        url = 'https://finance.naver.com/sise/theme.naver?field=name&ordering=asc&page='+str(i+1)
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36'}
        news = requests.get(url, headers=headers)

        # BeautifulSoup의 인스턴스 생성합니다. 파서는 html.parser를 사용합니다.
        soup = BeautifulSoup(news.content, 'html.parser')

        # CASE 1
        thema = soup.findAll(class_="col_type1")
        
        tma_list = []
        for i in range(1, len(thema)):
            #print(thema[i].text)
            tma_list.append(thema[i])
  
        #print(len(tma_list))

        
        
        # 각 뉴스로부터 a 태그인 <a href ='주소'> 에서 '주소'만을 가져옵니다.
        for idx, line in enumerate(tma_list):
            tmp_url = line.a.get('href')
            tmp_txt = line.text
            #바뀐페이지의 젓번째 url의 변동이없을때 탈출
            if idx == 0:
                if first_url == tmp_url:
                    first_flag = 1
                    break
                else:
                    first_url = tmp_url
            
            urllist.append(tmp_url)
            txtlist.append(tmp_txt)
            


        if first_flag == 1:
            break
        #뉴스 개수가 20개가 안되면 탈출
        if len(tma_list) < 40:
            break

        time.sleep(random.uniform(1.6,3.2))

    return urllist, txtlist





def find_thema(url):

    
    for i in range(1):
        url = 'https://finance.naver.com/'+url
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36'}
        news = requests.get(url, headers=headers)

        # BeautifulSoup의 인스턴스 생성합니다. 파서는 html.parser를 사용합니다.
        soup = BeautifulSoup(news.content, 'html.parser')

        # CASE 1
        thema = soup.findAll(class_="name_area")
        
        tma_list = []
        for  i  in range(len(thema)):
            tma_list.append(cleand_text(thema[i].text))

        time.sleep(random.uniform(1.6,3.2))

    return tma_list


def thema_crawling():

    url, txt = make_urllist()


    thema_lists = {}
    for idx, line in tqdm(enumerate(txt)):
        thema_lists[line]  =  find_thema(url[idx])


    # HDFS 클라이언트 생성
    client = InsecureClient('http://namenode:9870', user='root')

    # 딕셔너리 데이터를 JSON 문자열로 변환
    json_str = json.dumps(thema_lists, ensure_ascii=False, indent=4)

    # HDFS에 파일을 작성하고 JSON 문자열을 씁니다.
    with client.write('/thema/themaju.json', encoding='utf-8') as writer:
        writer.write(json_str)






# DAG 설정 
default_args = {
    'owner': 'thema_crawling_task',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 29),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),  
}

dag = DAG('thema_crawling_dag', 
          default_args=default_args,
          catchup=False, 
          schedule_interval='0 9 * * MON')



task = PythonOperator(
   task_id='thema_crawling_task',
   python_callable=thema_crawling,
   dag=dag
)
