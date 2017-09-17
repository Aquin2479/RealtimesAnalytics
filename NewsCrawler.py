%pyspark
#import packages & set date if we have no news data
import pyspark
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext

startDate = 20170910
endDate = 20170910


#the function to collect urls and etc when we have news data
def getNewsUrlReqular():
    sid1List = [100, 101, 102, 105, 106, 107]
    urlL1 = []
    title = []
    writeTime = []
    company = []
    newsId = []
    hiveContext = HiveContext(sc)
    finalCollectedTime = hiveContext.sql("select max(writing_time) from realtimes_db.realtimes_tb").collect()[0][0]
    now = datetime.now()
    nowDate = int('{year:0=4}{month:0=2}{day:0=2}'.format(year=now.year, month=now.month, day=now.day))
    nowTime = int(
        '{year:0=4}{month:0=2}{day:0=2}{hour:0=2}{min:0=2}'.format(year=now.year, month=now.month, day=now.day,
                                                                   hour=now.hour, min=now.minute))
    dateList = range(int(str(finalCollectedTime)[0:8]), nowDate + 1)
    aidFinder = re.compile(r'aid=[\d]+')
    aidSub = re.compile(r'[\d]+')
    writeTimeR = re.compile(r'[\s:-]')

    if finalCollectedTime != None:
        for sid1 in sid1List:
            for date in dateList:
                presentPage = 0
                pageNum = 1
                while True:
                    url = "http://news.naver.com/main/list.nhn?mode=LSD&mid=sec&listType=summary&sid1={sid1}&date={date}&page={pageNum}".format(
                        sid1=sid1, date=date, pageNum=pageNum)
                    soup = BeautifulSoup(requests.get(url).text, 'lxml')
                    if soup.select('div.paging > strong')[0].text == presentPage:
                        break
                    urlList = list(set([x.get('href') for x in soup.select(
                        'div.content > div.list_body.newsflash_body > ul > li > dl > dt > a')]))
                    tmpTitle = []
                    tmpTime = []
                    tmpNewsId = []
                    tmpcompany = []
                    tmpUrl = list()

                    for j in urlList:
                        for k in soup.select('div.content > div.list_body.newsflash_body > ul > li > dl'):
                            if k.select('dt > a')[0].get('href') == j:
                                if (len(k.select('dt')) == 1):
                                    tmpWriteTime = int(re.sub(writeTimeR, '', k.select('dd > span.date')[0].text))
                                    if tmpWriteTime > finalCollectedTime:
                                        tmpTitle.append(k.select('dt')[0].select('a')[0].text.strip())
                                        tmpTime.append(tmpWriteTime)
                                        tmpNewsId.append(aidSub.findall(aidFinder.findall(j)[0])[0])
                                        tmpcompany.append(k.select('dd > span.writing')[0].text)
                                        tmpUrl.append(j)
                                else:
                                    tmpWriteTime = int(re.sub(writeTimeR, '', k.select('dd > span.date')[0].text))
                                    if tmpWriteTime > finalCollectedTime:
                                        tmpTitle.append(k.select('dt')[1].select('a')[0].text.strip())
                                        tmpTime.append(tmpWriteTime)
                                        tmpNewsId.append(aidSub.findall(aidFinder.findall(j)[0])[0])
                                        tmpcompany.append(k.select('dd > span.writing')[0].text)
                                        tmpUrl.append(j)

                    newsId = newsId + tmpNewsId
                    title = title + tmpTitle
                    writeTime = writeTime + tmpTime
                    company = company + tmpcompany
                    urlL1 = urlL1 + tmpUrl
                    presentPage = soup.select('div.paging > strong')[0].text
                    pageNum += 1

    return pd.DataFrame({"news_code": newsId, "url": urlL1, "title": title, "writing_time": writeTime,
                         "company": company}).drop_duplicates(subset='news_code')


#the functions to clean news articles
def htmlCleanText(text):
    pattern1 = re.compile(r'<a[\s\S]+?/a>')
    pattern2 = re.compile(r'<script[\s\S]+?/script>')
    pattern3 = re.compile(r'<td[\s\S]+?/td>')
    return BeautifulSoup(re.sub(pattern3, "", re.sub(pattern2, "", re.sub(pattern1, "", text))), 'lxml').text


def cleanText(text):
    pattern10 = re.compile(r'[\S]+@[\S]+.[\S]+')
    pattern11 = re.compile(r'^[\d]{4}.[\d]{1,2}.[\d]{1,2}')
    pattern5 = re.compile(r'[(][\s\S\d]+=[\s\S\d][)]')
    pattern4 = re.compile(r'\[[\s\S]+\]')
    pattern1 = re.compile(r'<.+>')
    pattern2 = re.compile(r'[\s]*[\S]+\s*기자')
    pattern3 = re.compile('[\{\}\[\]\/?.,;:|\)*~`!^\-_+<>@\#$%&\\\=\(\△\→\■\▶]')
    pattern6 = re.compile('[\'\"`]')
    pattern7 = re.compile(r'무단[\S]*[\s\S]*및?[\s]+재?배포[\s]*금지')
    pattern9 = re.compile(r'＜.+＞')
    return re.sub(pattern9, ' ', re.sub(pattern7, '', re.sub(pattern6, '', re.sub(pattern3, ' ', re.sub(pattern2, '', re.sub(pattern1, '', re.sub(pattern4, '', re.sub(pattern5, '', re.sub(pattern11, '', re.sub(pattern10, '', text))))))))))


#the function to get news contentes about collected urls
def getNewsContents(urlList):
    sid1Name = {'정치': 'politics', '경제': 'economy', '사회': 'social', 'IT/과학': 'it'}
    newsId = []
    newsContent = []
    imageAdrr = []
    fullNewsContents = []
    site = []
    aidFinder = re.compile(r'aid=[\d]+')
    aidSub = re.compile(r'[\d]+')
    entertainR = re.compile(r'entertain')
    sportsR = re.compile(r'sports')

    for url in urlList:
        soup = BeautifulSoup(requests.get(url).text, 'lxml')
        if len(soup.select("div.error")) == 0:
            if len(sportsR.findall(soup.find("meta", property="og:url").get('content'))) != 0:
                tmpArticle = htmlCleanText(str(soup.select('#newsEndContents')[0])).strip()
                fullNewsContents.append(tmpArticle)
                newsId.append(aidSub.findall(aidFinder.findall(url)[0])[0])
                newsContent.append(cleanText(tmpArticle).strip())
                site.append('sports')
                if len(soup.select('#newsEndContents')[0].select('img')) != 0:
                    imageAdrr.append(soup.select('#newsEndContents')[0].select('img')[0].get('src'))
                else:
                    imageAdrr.append('n')
            elif len(entertainR.findall(soup.find("meta", property="og:url").get('content'))) != 0:
                tmpArticle = htmlCleanText(str(soup.select('#articeBody')[0])).strip()
                fullNewsContents.append(tmpArticle)
                newsId.append(aidSub.findall(aidFinder.findall(url)[0])[0])
                newsContent.append(cleanText(tmpArticle).strip())
                site.append("entertainments")
                if len(soup.select('#articeBody')[0].select('img')) != 0:
                    imageAdrr.append(soup.select('#articeBody')[0].select('img')[0].get('src'))
                else:
                    imageAdrr.append('n')
            else:
                tmpArticle = htmlCleanText(str(soup.select('#articleBodyContents')[0])).strip()
                fullNewsContents.append(tmpArticle)
                newsId.append(aidSub.findall(aidFinder.findall(url)[0])[0])
                newsContent.append(cleanText(tmpArticle).strip())
                site.append(sid1Name.get(soup.find("meta", property="me2:category2").get('content')))
                if len(soup.select('#articleBodyContents')[0].select('img')) != 0:
                    imageAdrr.append(soup.select('#articleBodyContents')[0].select('img')[0].get('src'))
                else:
                    imageAdrr.append('n')

    return pd.DataFrame(
        {"news_code": newsId, "preproc_content": newsContent, "content": fullNewsContents, 'img': imageAdrr, 'site': site}).drop_duplicates(subset='news_code')


# the functions to save news Data to hive
def csvSaveNewsContents(df, fileName):
    df.to_csv(('D:/newsdata/' + fileName), index=False, encoding='utf-8')


def hiveSaveNews(dfNewsContents, table_name):
    from pyspark.sql import HiveContext

    hiveContext = HiveContext(sc)
    tmpDf = hiveContext.createDataFrame(
        dfNewsContents[['news_code', 'title', 'site', 'writing_time', 'preproc_content', 'img', 'content', 'company']])
    tmpDf.registerTempTable("tmpDf")
    hiveContext.sql("insert into table {table_name} select * from tmpDf".format(table_name=table_name))


#crawling news
hiveContext = HiveContext(sc)
finalCollectedTime = hiveContext.sql("select max(writing_time) from realtimes_db.realtimes_tb").collect()[0][0]

if finalCollectedTime == None:
    contentstmp = getNewsUrlList(startDate, endDate)
else:
    contentstmp = getNewsUrlReqular()

dfContentsTmp = pd.DataFrame(contentstmp)
dfNewsContents = pd.merge(getNewsContents(dfContentsTmp['url']), dfContentsTmp, on='news_code',
                          how='inner').drop_duplicates(subset='news_code')

hiveSaveNews(dfNewsContents, 'realtimes_db.realtimes_tb')
