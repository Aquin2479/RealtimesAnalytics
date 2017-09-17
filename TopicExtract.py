%pyspark
## 1. 사용할 패키지 import
import pyspark
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from konlpy.tag import Komoran
import numpy as np
from gensim import corpora
from gensim import models
from gensim import matutils
from sklearn.cluster import KMeans
from collections import Counter
import csv
from datetime import datetime, timedelta

## 2. 사용할 함수 정의
## -1). 시간정의
# 필요 함수 정의: timeGo() :: 현재 시스템 시간을 저장해주는 함수
def timeGo():
    startData = datetime.now()
    timeMemoryVar = int('{year:0=4}{month:0=2}{day:0=2}{hour:0=2}00'.format(year=startData.year, month=startData.month,
                                                                            day=startData.day, hour=startData.hour))
    return timeMemoryVar


# 필요 함수 정의: timeWent() :: 어제 시스템 시간을 저장해주는 함수
def timeWent():
    startData = datetime.now() - timedelta(days=1)
    timeMemoryVar = int('{year:0=4}{month:0=2}{day:0=2}{hour:0=2}00'.format(year=startData.year, month=startData.month,
                                                                            day=startData.day, hour=startData.hour))
    return timeMemoryVar


## -2). Tokenization
# 필요 함수 정의 1: pos(doc) :: 텍스트 파일 doc에서 nouns(명사)를 추출하여 list형태로 반환
def pos(d):
    return t.nouns(d)


# 필요 함수 정의 2: extract(newsList, resultList) :: 여러개 뉴스를 한번에 불러와서
#                               각각 뉴스에 대해 pos(doc)을 호출, 명사를 추출하여
#                               결과를 resultList에 저장
def extract(testNews, texts_ko):
    for news in testNews:
        texts_ko.append(pos(news))


## -3). Functions for Topic info arrange
# 필요 함수 정의 1: mk_topic(topic_table, topic_brief) :: 토픽 30개 키워드 정리 함수
def mk_topic(topic_table, topic_brief):
    # 토픽i번에 소속된 기사들의 상위 5 키워드 정리한 리스트 테이블 : ex
    for i in range(0, 30):
        ex = list(topic_table['keyword'][topic_table['topic'] == i])
        # 한 토픽 내에 있는 키워드들 개별로 카운팅
        ex_key_iter = []
        for ex2 in ex:
            for ex3 in ex2:
                ex_key_iter.append(ex3[0])
        # 키워드 등장횟수로 합산해서 상위5개 뽑기
        ex_result = []
        count = Counter(ex_key_iter)
        ex_result.append(sorted(count, reverse=True)[:5])
        # 결과 저장
        topic_brief.append(ex_result)


# 필요 함수 정의 2: word_topic(topic_brief,topic_word) :: 한글 키워드로 토픽 상위 30개 키워드 정리
def word_topic(topic_brief, topic_word):
    for topic1 in topic_brief:
        topic_sub = []
        for topic2 in topic1[0]:
            topic_sub.append(dictionary_ko.get(topic2))
        topic_word.append(topic_sub)

## 3. hive sc컨텍스트 정의
hiveContext = HiveContext(sc)
# -1). section 정의
section = ["politics","entertainments","social","it", "economy", "sports"]
# -2). 형태소분석기 선언
t = Komoran()

## 4. 토픽분석 내용 저장할 저장 리스트/변수 선언
# - 1). 토픽 내용 저장할 데이터
# topic: 토픽내용 저장할 리스트
topic= []
# sectionList: section 내용 저장할 리스트
sectionList = []
# keyword: 토픽 설명할 키워드(1~5개)
keyword1 = []
keyword2 = []
keyword3 = []
keyword4 = []
keyword5 = []
# startdate1: 분석 시작한 시간 기록
startdate1 = []
# enddate1: 분석 끝난 시간 기록
enddate1 = []
# topic_count: 해당 토픽에 속하는 뉴스기사 개수
topic_count = []

# - 2). 뉴스기사에 토픽내용 저장할 데이터
# topics: 토픽 정보 저장할 리스트
topics = []
# newscoding: 뉴스코드(primary key) 저장할 리스트
newscoding = []
# addtime: 분석 시작한 시간 기록할 리스트
addtime =[]

## 5. section(6가지) 별 토픽 분석 코드

for i in range(0, len(section)):
    # 분석 시작시간 및 하루 전 시간 확보
    present_time = timeGo()
    yesterday_time = timeWent()
    rawData = hiveContext.sql(
        "select news_code, content from realtimes_db.realtimes_tb where site = '{sect}' and writing_time >= {yesterday} and writing_time <= {present} ".format(
            sect=section[i], yesterday=yesterday_time, present=present_time)).collect()

    # 뉴스 본문과 뉴스 코드를 저장하기
    testNews = []
    newsCode = []
    for j in range(0, len(rawData)):
        testNews.append(rawData[j].content)
        newsCode.append(rawData[j].news_code)

    # 뉴스 본문을 형태소 분석하여 명사만 추출하기
    texts_ko = []
    extract(testNews, texts_ko)

    # 각 뉴스 기사들의 형태소들을 모아 단어사전 생성
    dictionary_ko = corpora.Dictionary(texts_ko)

    # 단어사전을 바탕으로 각 뉴스 기사간 tf-idf 분석 실시
    tf_ko = [dictionary_ko.doc2bow(text) for text in texts_ko]
    tfidf_model_ko = models.TfidfModel(tf_ko)
    tfidf_ko = tfidf_model_ko[tf_ko]

    # tf-idf 분석 결과 값을 바탕으로 각 뉴스기사를 대표하는 상위 30개 키워드 추출
    keyword = []
    for j in range(0, len(tfidf_ko)):
        keyword.append(sorted(tfidf_ko[j], key=lambda x: x[1], reverse=True)[:30])

    # kmeans 클러스터링 분석을 실시하기 위해 sparse matrix및 전치행렬로 만들어 준다
    corpus_tfidf = matutils.corpus2csc(tfidf_ko).transpose()

    # kmeans 클러스터링 분석 실시 : 클러스터 개수는 10개로 지정
    kmeans = KMeans(n_clusters=10)
    kmeans.fit_predict(corpus_tfidf)
    # 클러스터링 결과 라벨값(토픽값) 저장
    label = kmeans.labels_

    # 클러스터링 결과 토픽 별 속한 뉴스기사 개수 종합
    label_article = []
    for j in range(0, 10):
        label_article.append(sum(label == j))

    # 중간 결과 저장
    topic_table = pd.DataFrame({'topic': label, 'keyword': keyword, 'newsCode': newsCode})

    # 토픽분석 결과와 뉴스코드, 현재 시간 기록...- 2). 뉴스기사에 토픽내용 저장할 데이터
    for j in range(0, len(newsCode)):
        topics.append(label[j])
        newscoding.append(newsCode[j])
        addtime.append(present_time)

    # 토픽분석 결과 토픽 대표하는 상위 5개 키워드 종합
    topic_brief = []
    mk_topic(topic_table, topic_brief)

    # 토픽분석 결과 토픽 대표하는 상위 5개 키워드 한글화
    topic_word = []
    word_topic(topic_brief, topic_word)

    # 분석종료 시간 기록
    end_time = timeGo()

    # 해당 섹션 분석 결과 저장
    for j in range(0, len(label_article)):
        topic.append(str(present_time) + section[i] + str(j))
        sectionList.append(section[i])
        keyword1.append(topic_word[j][0])
        keyword2.append(topic_word[j][1])
        keyword3.append(topic_word[j][2])
        keyword4.append(topic_word[j][3])
        keyword5.append(topic_word[j][4])
        startdate1.append(present_time)
        enddate1.append(end_time)
        topic_count.append(label_article[j])

# - 1). 토픽 내용 저장할 데이터
topic2 ={'topic_name': topic,
        'section':sectionList,
        'keyword1':keyword1,
        'keyword2':keyword2,
        'keyword3':keyword3,
        'keyword4':keyword4,
        'keyword5':keyword5,
        'analysis_start_time':startdate1,
        'analysis_end_time':enddate1,
        'article_count':topic_count
}

#- 1). 토픽 내용 저장할 데이터 아웃 : topic3
topic3 = pd.DataFrame(topic2)[['topic_name','section','keyword1','keyword2','keyword3','keyword4','keyword5','analysis_start_time','analysis_end_time','article_count']]
topic3.to_csv('/home/newbotcloud/topicSave/topic_{startTime}.csv'.format(startTime=present_time), index=False, encoding='utf-8')

# - 2). 뉴스기사에 토픽내용 저장할 데이터
topic_master = pd.DataFrame({'news_code':newscoding,'topic':topics,'time':addtime})

# 정해진 기간의 데이터 추출
rawData = hiveContext.sql("select news_code, site, title, writing_time, company, img, content from realtimes_db.realtimes_tb where writing_time >= {yesterday} and writing_time <= {present} ".format(yesterday = yesterday_time, present = present_time))
allData = rawData.toPandas()

# 뉴스기사 토픽분석 결과와 정해진 기간 데이터 조인을 통해 필요한 값 종합
mergeData = pd.merge(topic_master, allData, on='news_code', how='inner')

# 토픽 네임 속성을 완성하기 위해 3가지 변수 조합(분석시간 + 섹션이름 + 분류된 토픽번호)
temp_topic = mergeData['time'].astype(str)+mergeData['site']+mergeData['topic'].astype(str)

# 새로 만들어진 토픽 속성을 토픽 네임으로 컬럼 저장
mergeData['topic'] = temp_topic
mergeData = mergeData.rename(columns={'topic': 'topic_name'})

#결과값 저장
mergeData.drop('time', axis=1).to_csv('/home/newbotcloud/topicSave/news_{startTime}.csv'.format(startTime=present_time), index=False, encoding='utf-8')
