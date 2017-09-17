# RealtimesAnalytics

### 실시간 뉴스 추천 서비스 RealTimes의 뉴스 수집 및 분석 코드.

- 참여: 서민영, 임수현, 이태현, 김장원, 최현호

- PPT link
  - https://www.slideshare.net/jangwon23/final-project-finalpdf

- 데이터 처리 및 분석 과정
  - 웹 크롤링을 통한 기사 수집(Python)
  - KONLPy 형태소 분석 라이브러리를 사용하여 형태소 분석
  - TF-IDF를 통해 핵심 키워드 선정(gensim 패키지)
  - 핵심 키워드를 통한 K-Means Clustering을 통해 클러스터 생성

- 분산 시스템 환경 및 소프트웨어 환경
  - OS:Debian GNU/Linux 8(Google Cloud Platform)
  - Apache Hadoop 2.7.3
  - Apache Hive 1.2.1
  - Apache Spark 1.6.2
  - Zeppelin 0.7.2
  - Python 3.4.2
