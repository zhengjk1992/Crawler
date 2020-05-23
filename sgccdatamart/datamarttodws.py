# -*- coding: utf-8 -*-
# @Time    : 2020/5/22 23:26
# @Author  : Zheng Jinkun
# @Github  ：https://github.com/zhengjk1992

import requests
import json
import time
import psycopg2
import argparse

class datamarttoDWS(object):
	headers = {
		'User-Agent1': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36',
	}
	
	
	def __init__(self,
				 url="****",
				 userName="****",
				 password="****",
				 dataCode="****",
				 current=1,
				 pageSize=1000000,
				 timeRange="****",
				 ):
		_param = self.param  
		self._url = url
		self._userName = userName
		self._password = password
		self._dataCode = dataCode
		self._current = current
		self._pageSize = pageSize
		self._timeRange = timeRange
		self._data = self.data 
		self._dbhost = _param.host
		self._dbport = _param.port
		self._dbdatabase = _param.database
		self._dbuser = _param.password
		self._dbpassword = _param.client_encoding
		self._client_encoding = _param.client_encoding
		self._tablename = _param.tablename
		self._json_path = _param.json_path

		
	def getparam(self):
		parser = argparse.ArgumentParser()
		parser.add_argument('-url', default=""****",")
		parser.add_argument('-host', default='"****",')
		parser.add_argument('-port', default='8000')
		parser.add_argument('-database', default='"****",')
		parser.add_argument('-password', default='"****",')
		parser.add_argument('-client_encoding', default='UTF-8')
		parser.add_argument('-json_path', default='data.rows')
		parser.add_argument('-tablename', default='"****",')
		args = parser.parse_args()
		return args

		
	param = property(getparam)

	
	def crawler(self):
		starttime = time.clock()
		url = self._url.format(self._userName, self._password, self._dataCode, self._current, self._pageSize,
							   self._timeRange)
		try:
			while 1:
				timeout = 5
				mp = requests.get(url=url, headers=self.headers, timeout=timeout)
				if mp.status_code == 200: 
					print("数据获取时间为{0}秒".format(round(time.clock() - starttime, 2)))
					return mp.content
				else:
					if timeout > 100: 
						return None
					timeout += 2
		except (requests.exceptions.ReadTimeout, requests.exceptions.RequestException) as e:
			print(e)
			return None

			
	data = property(crawler)

	
	def readjsontoDWS(self):		
		try: 
			connection = psycopg2.connect(host=self._dbhost,
										  port=self._dbport,
										  database=self._dbdatabase,
										  user=self._dbuser,
										  password=self._dbpassword,
										  client_encoding=self._client_encoding
										  )
		except psycopg2.DatabaseError as e:
			print(e)
		try:
			js = json.loads(self._data)  
			cursor = connection.cursor()
			for i, rows in enumerate(js['data']['rows']):  
				columns = ', '.join(map(str, list(rows.keys()))) 
				values = ', '.join(map(lambda x: '\'' + str(x) + '\'', list(rows.values())))
				strsql = 'insert into {0}({1}) values({2});'.format(self._tablename, columns, values)
				cursor.execute(strsql)
				if i // 100 == 0:
					connection.commit()
				print("第{0}条纪录已入库".format((str(i + 1))))
		except psycopg2.ProgrammingError as e:
			print(e)
		else:
			cursor.close()
			cursor.close()