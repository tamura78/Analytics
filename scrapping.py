from bs4 import BeautifulSoup
import requests
import joblib
from tqdm import tqdm
import pandas as pd


url = "https://www.baitoru.com/"
res=requests.get(url)
soup=BeautifulSoup(res.text,'html.parser')


url = "https://www.baitoru.com/kanto/jlist/tokyo/conveni/"
res=requests.get(url)
soup=BeautifulSoup(res.text,'html.parser')
total_num_str_pre = soup.find(id = "js-job-count").get_text()
#print(total_num_str_pre)

total_num_str = total_num_str_pre.replace(",","")
total_num = int(total_num_str)
#print(total_num)

total_page_num = total_num // 20 + 1#注釈を参照のこと
#print(total_page_num)

url_list_001 = list()
url = "https://www.baitoru.com/kanto/jlist/tokyo/conveni/"
url_list_001.append(url)
#1ページ目だけは別で対応し、リストに追加しておきます。
for i in range(2, total_page_num+1):
    url_key = "https://www.baitoru.com/kanto/jlist/tokyo/conveni/page" + str(i) + "/"
		#残りのページについては、for文を利用した取得を行います。
    url_list_001.append(url_key)
    
#print(url_list_001)

def joblib_get_url(i):
    url_list_pre = list()
    url = url_list_001[i]
    raw_url = "https://www.baitoru.com"
    res=requests.get(url)
    soup=BeautifulSoup(res.text,'html.parser')
    for i in range(0, 20):
        try: 
            elem = soup.find_all("article", class_="list-jobListDetail")[i].find("h3").find("a") 
            url_key = elem.attrs['href']
            n_url_key = raw_url + url_key
            url_list_pre.append(n_url_key)
        except: 
            pass
        
    return url_list_pre

a=10
joblib_num = total_page_num//a + 1 

url_list_002 = list()
for n in tqdm(range(0, joblib_num)):
    try:
        resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_url)(i) for i in range(n*a,(n+1)*a)])
        url_list_002.extend(resultList)
    except:
        pass
    
#print(url_list_002)

url_list_002_filtered = [x for x in url_list_002 if x is not None] 
flatten_url_list_002 = [ flatten for inner in url_list_002_filtered for flatten in inner ]
url_list_003 = []
for i in flatten_url_list_002: 
    if i not in url_list_003:
        url_list_003.append(i)

def joblib_get_data(i):
    new_list = list()
    houjin=syokusyu=kyuyo='' #←①
    
    url = url_list_003[i]
    res=requests.get(url)
    soup=BeautifulSoup(res.text,'html.parser')

    try:
        selector = '#contents > div.layout-grid-2-2 > div.layout-column-1 > div > div.detail-companyInfo > div > div.bg01 > div.pt02 > dl > dd > p'
        raw_houjin = soup.select_one(selector).get_text()
        houjin = raw_houjin.split()
    except:
        pass

    for i in range(0, 10):
        try:
            judge = soup.find("div", class_="detail-basicInfo").find_all("dt")[i].get_text()
            if "職種" in judge:
                pre_syokusyu = soup.find("div", class_="detail-basicInfo").find_all("dd")[i].get_text()
                syokusyu = pre_syokusyu.split()
        except:
            pass

    for i in range(0, 10):
        try:
            judge = soup.find("div", class_="detail-basicInfo").find_all("dt")[i].get_text()
            if "給与" in judge:
                pre_kyuyo = soup.find("div", class_="detail-basicInfo").find_all("dd")[i].get_text()
                kyuyo = pre_kyuyo.split()
        except:
            pass  
    
    new_list.append(houjin)
    new_list.append(syokusyu)
    new_list.append(kyuyo)

    return new_list

b=100
joblib_num = len(url_list_003)//b + 1

all_list = list()

for n in tqdm(range(0, joblib_num)):
    try:
        resultList = joblib.Parallel(n_jobs=12, verbose=3)( [joblib.delayed(joblib_get_data)(i) for i in range(n*b,(n+1)*b)])
        all_list.extend(resultList)
    except:
        pass
    
    
all_list_filtered = [x for x in all_list if x is not None]
kekka = pd.DataFrame(all_list_filtered,columns=["法人名","職種","給与情報"])

kekka.to_csv('scraping.csv',encoding="utf-8-sig")