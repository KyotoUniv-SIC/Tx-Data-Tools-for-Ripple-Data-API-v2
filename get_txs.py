# %%
import requests
import pandas as pd
import io
import time

# %%
api = "https://data.ripple.com/v2/payments?limit={limit}&format={form}"
limit = 1000
url = api.format(limit=limit, form="csv")
# 出力するcsv
# result20xx
# result20xxfirst, result20xxsecond
# result20xxq1, result20xxq2, result20xxq3, result20xxq4
file = "./result2022q1.csv"
# 区切り
# 年次の場合は20x(x+1)
# 半期の場合は20xx07、20x(x+1)
# Quaterの場合は20xx04、20xx07、20xx10,20x(x+1)
end = "marker=202204"

r = requests.get(url)
df = pd.read_csv(io.BytesIO(r.content), sep=",")

headerLink = r.headers["Link"].split(";")
nextUrl = headerLink[0].replace("<", "").replace(">", "")
# 開始地点を入力,　最初の場合はコメントアウト
nextUrl = "http://data.ripple.com/v2/payments?limit=1000&format=csv&marker=20220101000631|000068710365|00055"

resultDf = pd.DataFrame(columns=df.columns)

# nextUrlを指定する場合コメントアウトする
# resultDf = pd.concat([resultDf, df])

resultDf.to_csv(file, index=False)

while len(headerLink) == 2:
    try:
        r = requests.get(nextUrl)
    except:
        print("stop now")
        time.sleep(11)
        continue

    if r.status_code == 429:
        print("429 Too Many Requests")
        print(r.headers)
        time.sleep(11)
        continue
    elif r.status_code == 200:
        print("GET!")
        nextdf = pd.read_csv(io.BytesIO(r.content), sep=",")
        print("Writing data...")
        nextdf.to_csv(file, mode='a',
                      header=False, index=False)
        if "Link" in r.headers:
            headerLink = r.headers["Link"].split(";")
            nextUrl = headerLink[0].replace("<", "").replace(">", "")
            print(headerLink)
            if end in r.headers["Link"]:
                print("nextUrl")
                print(nextUrl)
                break
        else:
            print("Reach the Latest Tx")
            break
    else:
        print("Unexpected HTTP Error")
        time.sleep(20)
        break

# %%
print("Complete!")
