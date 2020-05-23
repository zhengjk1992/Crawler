# 数据超市数据加工进入数据中台DWS（PostgreSQL）
# 1.构建思路
最近国网公司大数据中心开放了数据超市，参考一些数据服务提供商向外提供数据服务，我们以江西省海关进出口贸易数据为例打通数据从数据超市进入华为数据中台DWS（PostgreSQL）数据链路，并进行适度数据清洗转换。主要分为以下几个步骤：

1.在页面上申请该项数据接口服务获取接口相关参数（链接、用户名、密钥等），审批通过之后记录这些信息；

2.利用接口获取json格式数据下载到本地内存（或硬盘）；

3.在数据中台高阶组件上进行网络及任务配置，并确保调试通过；

4.将json格式数据解析为DWS的入库语句进行入库；

5.在封装好的类中加入argparse包的使用，方便在linux终端上直接输入参数以命令行的形式进行调用。

经过测试，本技术方案可行。
# 2.实现方式
接口参数说明

```python
名称	    类型	    必填	示例值	                        描述
userName	String	    是	    test	                        用户名
password	String	    是	    testkey	                        订单密钥
dataCode	String	    是	    HG_ENTERPRISE_INFO	                数据编码
timeRange	String	    是	    20190101010101,20190823221422	时间区间
```

示例数据
```python
{"code":0,"message":"正常","data":
	{"current":1,"total":22799,"totalPage":12,"pageSize":2000,"rows":
		[
			{
				"stand_id":"D1AE47D605D14B358B317740BFD5694C",
				"sea_date":"2020-05-08 10:34:24",
				"soci_credit_code":null,
				"company_name":"上海厚生堂（江西）酒业有限公司",
				"addr_name":"上高县科技工业园旺旺路2号",
				"legal_name":"谢安龙",
				"rg_date":"2004-06-21 00:00:00",
				"rg_cust_code":"4009",
				"rg_cust_name":"新余海关",
				"admin_division_code":"360923000000",
				"admin_division_name":"宜春市上高县",
				"economic_area_code":"09",
				"economic_area_name":"一般经济区域",
				"manage_type":"10000000000000000000000000000000",
				"manage_type_name":"进出口货物收发货人",
				"special_trade_code":"99999999",
				"special_trade_name":null,
				"trade_type":"1521",
				"trade_name":"白酒制造",
				"cust_valid_date":"2068-07-31 00:00:00",
				"commerce_type":"00000000",
				"commerce_type_name":null,
				"logout_sign_code":"10000000",
				"logout_sign_name":"注销",
				"credit_abnormal":"否",
				"rowno":1
			},
		]
	}
}
```

## 2.1构造datamarttoDWS类
初始化链接接口相关信息、链接数据库相关信息。
```python
	def __init__(self,url="****",userName="****",password="****",dataCode="****",current=1,pageSize=1000000,timeRange="****",):
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
```
## 2.2实现数据获取接口函数
利用requests库的get方法进行数据的获取。
```python
	def crawler(self):
		starttime = time.clock()
		url = self._url.format(self._userName, self._password, self._dataCode, self._current, self._pageSize,self._timeRange)
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
```
## 2.3实现数据的解析入DWS数据库
利用json库处理多重嵌套的json格式bytes串解析，利用psycopg2库进行DWS数据的入库操作。代码的关键点主要有两点：

1.通过dict或者list数据类型数据准确无误的构造出sql的insert into语句。主要陷阱是数据类型及单引号、分号的构造。

2.数据库提交的次数需要合理控制，commit操作太过频繁可能影响数据库性能，太不频繁则影响入库速度，需要找到合理的平衡点。

```python
def readjsontoDWS(self):		
		try: 
			connection = psycopg2.connect(host=self._dbhost,port=self._dbport,database=self._dbdatabase,user=self._dbuser,password=self._dbpassword,client_encoding=self._client_encoding)
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
```
## 2.4Linux终端以指令的方式调用
加入argparse的add_argument方法使用，可以方便的使py文件被定期调度执行，方便扩展。
```python
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
```

# 3.待改进的部分

1.需进一步改进数据解析函数，针对不同的json格式应该能根据参数内容提供不同的解析方法。

2.需针对获取的数据类型（str、int、datatime、float）不同，进行建表语句（CREATE TABLE）的自动生成，消除人工工作量及错误。

