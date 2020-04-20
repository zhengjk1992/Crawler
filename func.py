# -*- coding:utf-8 -*-
import re
import requests
import os
import time
import socket
import imghdr
import gc
from functools import partial
from PIL import Image
from multiprocessing import Pool
from queue import Queue
from PIL import ImageFile
# url = 'http://www.6mmh.com/?bookId=548&a=index&f=directory'
# title_page = 'http://www.6mmh.com/?id=145&a=index&f=book'
headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/18.17763"
}

proxy_list = [
    'http://117.177.250.151:8081',
    'http://111.85.219.250:3129',
    'http://122.70.183.138:8118',
    ]
# illegal_character=['?','*',':','"','<','>','\\','/','|',' ']

def file_in(path):
    file_list = os.listdir(path)
    pdf_list = []
    for file_name in file_list:
        if ".pdf" in file_name:
            pdf_list.append(int(file_name.split('.')[0]))

    return pdf_list

def change(count):
    count = str(count).rjust(4, '0')
    file_list = os.listdir('.')
    dest = ""
    for i in file_list:
        if i.count(count) == 1 and i.count(".pdf") == 0:
            dest = i
            break

    print(dest)

# 将多张图片转换为1个PDF
def reaC2(pic_name,pdf_name,pos):
    im_list = []
    im1 = Image.open(pic_name[0])
    if im1.mode == "RGBA":
        im1 = im1.convert('RGB')
    pic_name.pop(0)
    for i in pic_name:
        try:#png损坏的就直接跳过
            if imghdr.what(i):#png损坏的就直接跳过
                # print(imghdr.what(i))
                img = Image.open(i)
                print(i+"图像模式", img.mode)
                if img.mode in ["RGBA", 'P']:
                    img = img.convert("RGB")
                # print(img.mode)
                im_list.append(img)
            else:
                continue
        except (SyntaxError,OSError,IOError):
            continue
    im1.save(pdf_name + '('+str(pos)+')' + '.pdf', "PDF", resolution=100.0, save_all=True, append_images=im_list)
    del im1
    gc.collect()
    print(pdf_name + '('+str(pos)+')'+"PDF转换完成OK")

def reaC1(pic_name,pdf_name,number):
    #分成number份
    count = int(len(pic_name) / number)

    if number == 1:
        reaC2(pic_name, pdf_name,1)
    elif number == 2:
        pic_name1 = pic_name[:count]
        pic_name2 = pic_name[count:]
        reaC2(pic_name1, pdf_name, 1)
        reaC2(pic_name2, pdf_name, 2)
    elif number == 3:
        pic_name1 = pic_name[:count]
        pic_name2 = pic_name[count:count*2]
        pic_name3 = pic_name[count*2:]
        reaC2(pic_name1, pdf_name, 1)
        reaC2(pic_name2, pdf_name, 2)
        reaC2(pic_name3, pdf_name, 3)
        pass
    elif number == 4:
        pic_name1 = pic_name[:count]
        pic_name2 = pic_name[count:count * 2]
        pic_name3 = pic_name[count * 2:count * 3]
        pic_name4 = pic_name[count * 3:]
        reaC2(pic_name1, pdf_name, 1)
        reaC2(pic_name2, pdf_name, 2)
        reaC2(pic_name3, pdf_name, 3)
        reaC2(pic_name4, pdf_name, 4)
    else:
        pass

def rea(pdf_name):
    totalSize = 0
    for filename in os.listdir(pdf_name):
        totalSize = totalSize + os.path.getsize(os.path.join(pdf_name, filename))
    # print(pdf_name)
    file_list = os.listdir(pdf_name)
    # print(file_list)
    pic_name = []

    for x in file_list:
        if ".jpg" in x or '.png' in x or '.jpeg' in x or '.gif' in x:
            pic_name.append(pdf_name + '.\\'+ x)
    if len(pic_name) == 0:
        return None

    sliceT = totalSize//300000000 + 1
    reaC1(pic_name, pdf_name, sliceT)

def CrawlerF(count,path):
    print(count)
    pdf_list = file_in(path)
    # print(pdf_list)
    # 1.已经生成的电子书不执行本程序

    if count in pdf_list:
        print(str(count) + "漫画已存在")
        return None
    count = str(count).rjust(4, '0')
    # proxy_ip = random.choice(proxy_list)  # 随机获取代理ip
    # proxies = {'http': proxy_ip}
    q = Queue(maxsize=0)

    url = 'http://www.6mmh.com/?bookId=' + count + "&a=index&f=directory"
    title_page = 'http://www.6mmh.com/?id=' + count + '&a=index&f=book'
    # 获取漫画名称并建立文件夹
    title = requests.get(title_page, headers=headers)
    if title.status_code != 200:
        return None
    pat_title = "\"name\":(.*?),\"author\""
    y = re.compile(pat_title, re.S).findall(title.text)[0]
    y = y.replace("\"", "")
    y = y.encode().decode('unicode_escape')

    y = re.sub(r'[?*:"<>\/|]*', '', y)#路径中的非法字符用下划线进行替换
    # print(y)
    folder = os.path.exists(path + count + '.' + y + '/')
    # print(folder,path + count + '.' + y + '/')
    if not folder:  # 判断是否存在文件夹如果不存在则创建为文件夹
        os.makedirs(path + count + '.' + y + '/')

    # 获取当前漫画所有图片的链接
    data = requests.get(url, headers=headers).text
    # print(data)
    temp = data.replace("\\", "")
    # print(temp)
    pat = "<img src=\"(.*?)\""
    x = re.compile(pat, re.S).findall(temp)
    # print(x)
    len_x = len(x)
    # print(path + count +'.'+ y + "\t"+ str(len_x))
    for key, value in enumerate(x):
        q.put([key, value, 1])
    # print("hello",q.qsize())
    # 2.已经下载过的图片生成一个list
    file_list = os.listdir(path + ".\\" + count + '.' + y)
    # print(file_list)
    downed = []
    for file in file_list:
        downed.append(int(file.split('.')[0]))
    # print(downed)
    # 获取当前漫画所有图片的内容
    print(path +count + '.' + y + "共有图片"+str(len_x)+'张')
    # if len_x > 10000:
    #     print("skip")
    #     return None

    while q.qsize() != 0:
        # print("hello")
        [key, value, failed_count] = q.get()

        if (key + 1) in downed:
            continue
        starttime = time.time()
        png_jpg = value.split(".")[-1]
        # print([key, value, failed_count])
        try:
            mp = requests.get(value, headers=headers, timeout=(failed_count)*5)

            filename = path + count + '.' + y + '/' + str(key + 1).rjust(4, '0') + '.' + png_jpg
            with open(filename, 'wb') as f:
                f.write(mp.content)
            endtime = time.time()
            print("第" + "(" + str(key + 1) + '/' + str(len_x) + ")" + "张图已保存完毕！\t\t\t\t\t\t\t"
                  +count+'.'+ y + "\t\t\t\t\t\t\t耗时" + str(round(endtime - starttime, 1)) + 'S')

        except (requests.exceptions.RequestException, requests.exceptions.Timeout,
                requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout,
                socket.timeout, socket.error):
            failed_count = failed_count + 0.3
            q.put([key, value, failed_count])
            # print("第" + "(" + str(key + 1) + '/' + str( len_x) + ")" +
            #       "张图已保存失败！，加入补录队列\t\t\t\t\t\t\t" + y)
    starttime = time.time()
    rea(path + count+'.'+y)
    endtime = time.time()
    print("t耗时" + str(round(endtime - starttime, 1)) + 'S')

if __name__ == '__main__':
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    # x = [*range(593,700)]
    x = [343]
    y = len(x) *["C:\\C\\"]


    for k,v in enumerate(x):
        CrawlerF(v,y[k])

    # zip_args = list(zip(x, y))
    # pool = Pool(20)
    # pool.starmap(CrawlerF, zip_args)
    # pool.close()
    # pool.join()
