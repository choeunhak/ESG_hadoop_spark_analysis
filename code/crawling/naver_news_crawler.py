import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import time

com_name = "kg이니시스"
year_list = ["2015","2016","2017", "2018", "2019","2020","2021"]
month_list = ["01","02","03","04","05","06","07","08","09","10","11","12"]
day_slist = ["01", "16"]
day_elist = ["15", "31"]

info = []
for year_mem in year_list:
    #크롤링 차단을 위해 시간차주기
    rand_value = random.randint(1, 3)
#                 print("rand_value:", rand_value)
    time.sleep(rand_value)
    for month_mem in month_list:
        starttime = time.time()
        if(year_mem=="2021" and month_mem=="11"):
                break
        for day_smem, day_emem in zip(day_slist, day_elist):
            ds = year_mem + "." + month_mem + "." + day_smem   # 시작날 형식 입력
            de = year_mem + "." + month_mem + "." + day_emem  # 끝날 형식 입력
            print(ds + "-" + de)
            start = -9  # page: 1부터 10씩 증가
            while (True):
                start = start + 10
                raw = requests.get(
                    "https://search.naver.com/search.naver?where=news&sm=tab_pge&query=" + com_name + "&sort=0&photo=0&field=0&pd=3&ds=" + ds + "&de=" + de + "&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so:r,p:from0to0,a:all&start=" + str(
                        start) + "",
                    headers={'User-Agent': 'Mozilla/5.0'})
                html = BeautifulSoup(raw.text, "html.parser")
                articles = html.select("ul.list_news > li")
                if (len(articles) == 1):# or len(articles) == 0
                    break
                i = 0

                for a in articles:
                    if(articles[i].select_one("a.info") and articles[i].select_one("a.news_tit") and articles[i].select_one("a.api_txt_lines") is not None):
                        b = articles[i].select_one("a.news_tit").text
                        c = articles[i].select_one("a.info").text
                        d = articles[i].select_one("a.api_txt_lines").text
                        info.append([str(year_mem)+str(month_mem), c, b, d])
                        i = i + 1
        endtime = time.time()
        print(f"{endtime - starttime:.5f} sec")

# 중복제거 후 저장
col_name = ['time', 'news', 'title', 'summary']
list_df = pd.DataFrame(info, columns=col_name)
newlist_df = list_df.drop_duplicates(['summary'], keep = 'first')
newlist_df = newlist_df[~newlist_df['summary'].isnull()]
newlist_df.to_csv(r"C:\Users\eunhak\Documents\project\ESG_Analysis\data\15_21\kgini15_21.csv", header = True, index = False, encoding='utf-8-sig')

print("크롤링 완료\n")