import requests
from multiprocessing import Pool
import os
import re
import pandas as pd

headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/18.17763"
}

def get_areas():
    areas = []
    url = "http://www.tianqihoubao.com/lishi/jiangxi.htm"
    mp = requests.get(url)
    pat = "<dd>(.*?)</dd>"
    datadd = re.compile(pat, re.S).findall(mp.text)
    for con in datadd:
        patdt = "lishi/(.*?).html"
        data = re.compile(patdt, re.S).findall(con)
        areas += data
    return areas

def __crawler(area,year,month,path):
    # print(area,year,month)
    url = "http://www.tianqihoubao.com/lishi/{0}/month/{1}{2}.html".format(area,year,month)
    mp = requests.get(url,headers=headers)
    if '{0}-{1}{2}.html'.format(area,year,month) in os.listdir(path):  #已经下载过的忽略
        print("{0}-{1}{2}.html  已经下载完毕！".format(area,year,month) )
        return None
    if mp.status_code != 200:
        print("error")
    with open(path+"\\{0}-{1}{2}.html".format(area,year,month), 'wb') as f:
        print(url)
        f.write(mp.content)

def crawler(areas,years,months,path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    args = []
    for area in areas:
        for year in years:
            for month in months:
                if year == '2020' and month in ['06', '07', '08', '09', '10', '11', '12']:
                    continue
                args.append((area,year,month,path))
    pool = Pool(100)
    pool.starmap(__crawler, args)
    pool.close()
    pool.join()

def decode_data(path):

    files_path = os.listdir(path)
    data = []
    for file_path in files_path:
        with open(path + '\\' + file_path, 'r') as f:
            file_data = f.readlines()
            # print(file_data)
            pattr = "<tr>(.*?)</tr>"
            data_month = re.compile(pattr, re.S).findall("".join(file_data))[1:]
            for data_day in data_month:
                pattd = "<td>(.*?)</td>"

                data_data_list = re.compile(pattd, re.S).findall(data_day)
                # print(data_data_list)
                #  处理地区
                patarea = "日(.*?)天气"
                data_area = re.compile(patarea, re.S).findall(data_data_list[0])[0]
                #  处理日期
                pattitle = "title=\"(.*?)日"
                data_date = re.compile(pattitle, re.S).findall(data_data_list[0])[0]
                data_date = data_date.replace("年",'-')
                data_date = data_date.replace("月", '-')
                #   处理天气
                data_weather = data_data_list[1].replace("\n",'')
                data_weather = data_weather.replace(" ",'')
                #   处理温度
                data_temp = data_data_list[2].replace("\n", '')
                data_temp = data_temp.replace(" ", '')
                data_temp = data_temp.replace("℃", '')
                data_temp = data_temp.split('/')
                if data_temp[0] == '':
                    data_temp[0] = -999999  #需要进一步处理


                #   处理风力
                data_wind = data_data_list[3].replace("\n", '')
                data_wind = data_wind.replace(" ", '')
                print([data_area,data_date,data_weather,data_temp[0],data_temp[1],data_wind])
                data.append([data_area,data_date,data_weather,data_temp[0],data_temp[1],data_wind])
    df = pd.DataFrame(data=data,columns=['area','date','weather','temperatureH','temperatureL','wind'])
    df[['temperatureH','temperatureL']] = df[['temperatureH','temperatureL']].astype(float)
    print(df.groupby("area"))
    df.to_excel("data_set.xlsx")
    # print(df.groupby("area").min())


def read_data(path="data_set.xlsx"):

    df = pd.read_excel(path)
    groupby_df = df.groupby(["area"], as_index=False)
    with pd.ExcelWriter('filename.xlsx') as writer:
        for i in groupby_df:
            i[1].to_excel(writer, index=False, sheet_name=i[0])



if __name__ == '__main__':

    years = ['2015','2016','2017', '2018', '2019', '2020']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    # df = pd.date_range("2019-01-01","2020-05-12")
    # print(df)

    # get_areas()

    # crawler(areas=get_areas(), years=years, months=months,path="D:\\data_set")

    # decode_data(path="D:\\data_set")
    read_data(path="data_set.xlsx")



