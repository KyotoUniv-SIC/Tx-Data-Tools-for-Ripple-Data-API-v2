# %%
import requests
import pandas as pd
import io
import time

# %%
api = "https://data.ripple.com/v2/payments?limit={limit}&format={form}"
limit = 1000
url = api.format(limit=limit, form="csv")

r = requests.get(url)
df = pd.read_csv(io.BytesIO(r.content), sep=",")

headerLink = r.headers["Link"].split(";")
nextUrl = headerLink[0].replace("<", "").replace(">", "")
# 開始地点を入力,　最初の場合はコメントアウト
# nextUrl = ""

resultDf = pd.DataFrame(columns=df.columns)
resultDf = pd.concat([resultDf, df])

listDf = []

while len(headerLink) == 2:
    try:
        r = requests.get(nextUrl)
    except:
        print("stop now")
        time.sleep(11)
        continue

    if r.status_code == 429:
        print(r.headers)
        time.sleep(11)
        continue
    elif r.status_code == 200:
        nextdf = pd.read_csv(io.BytesIO(r.content), sep=",")
        print("AAAAAAAAAAAAAAA")
        ##resultDf = pd.concat([resultDf, nextdf])
        listDf.append(nextdf)
        print("BBBBBBBBBBBBBBBBBB")

        headerLink = r.headers["Link"].split(";")
        nextUrl = headerLink[0].replace("<", "").replace(">", "")

        print(headerLink)
        # 区切りでbreakする
        # 年次の場合は20x(x+1)
        # 半期の場合は20xx07、20x(x+1)
        # Quaterの場合は20xx04、20xx07、20xx10,20x(x+1)
        if "marker=2015" in r.headers["Link"]:
            print("CCCCCCCCCCCCCCCCC")
            tempDf = pd.concat(listDf)
            resultDf = pd.concat([resultDf, tempDf])
            print(len(resultDf))
            # CSVに出力
            # result20xx
            # result20xxfirst, result20xxsecond
            # result20xxq1, result20xxq2, result20xxq3, result20xxq4
            resultDf.to_csv("./result2013.csv", index=False)
            print("complete save csv")
            print("nextUrl")
            print(nextUrl)
            break
    else:
        print("unexpected http error")
        time.sleep(20)
        break

# %%
