# -*- coding: utf-8 -*-
# @Time    : 2020/6/5 21:37
# @Author  : Zheng Jinkun
# @FileName: utilize.py
# @Software: PyCharm
# @Github  ：https://github.com/zhengjk1992

import sys
import os
import psutil
import argparse
import platform
from time import sleep, ctime


class Utilize():

	def __init__(self):
		"""本类用于定期清理释放磁盘空间.

		Args:
		Returns:
		Raises:

		"""
		_param = self.param  # 获取来自命令号的参数
		self._path = _param.path
		self._usagepercent = _param.useage  # 将百分比参数转为float类型
		self._systemversion = platform.system()  # 获取执行环境的操作系统版本windows/linux
		self._wait = _param.wait
		if (not self.is_number(self._usagepercent)) and (not self.is_number(self._wait)):  # 判断输入百分比及实践是否为合法数字
			raise Exception("Please input float type, example:95.2, 0.85 etc.", self._usagepercent)
		if not os.path.isdir(self._path):  # 判断是否为目录格式，不是则抛出异常
			raise Exception("Invalid path", self._path)
		self._usagepercent = float(_param.useage)  # 将百分比参数转为float类型
		if self._usagepercent < 0.0 or self._usagepercent > 100.0:
			raise Exception("Usagepercent bigger than 0.0 and lower than 100", self._usagepercent)
		elif self._usagepercent > 0.0 and self._usagepercent < 1.0:
			self._usagepercent = self._usagepercent * 100
		else:
			pass

	@staticmethod
	# 判断字符串是否为字母或者数字(浮点数)的静态方法
	def is_number(tempstr):
		try:  # 因为使用float有一个例外是'NaN'
			if tempstr == 'NaN':
				return False
			float(tempstr)
			return True
		except ValueError:
			return False

	def getparam(self):
		"""获取控制台所输入的参数，主要包含需要输入所需要清理的文件夹绝对路径，设置触发清理程序的磁盘占用百分比等信息.

		Args:
		Returns: args	<class 'argparse.Namespace'>类型
		Raises:

		"""
		parser = argparse.ArgumentParser()
		parser.add_argument('-p', '--path', default='D:\\test', help="需要输入所需要清理的文件夹绝对路径", required=True)
		parser.add_argument('-u', '--useage', default='85.0', help="设置触发清理程序的磁盘占用百分比", required=True)
		parser.add_argument('-w', '--wait', default='900', help="设置程序执行周期", required=True)
		args = parser.parse_args()
		return args

	param = property(getparam)

	def _get_status(self, path):
		listtemp = self._get_disk_info()
		for i in listtemp:
			if path == i[0]:
				if i[1] > self._usagepercent:
					return (False, i[1])
				else:
					return (True, i[1])

	def _get_disk_info(self):
		"""获取所有逻辑分区的使用情况

		Args:
		Returns: <class 'list'>类型，例如 [('C', 59.9), ('D', 52.1)]
		Raises:

		"""
		disk_useages = []
		disk_names = []
		for disk in psutil.disk_partitions():
			disk_info = psutil.disk_usage(disk.device)
			disk_useages.append(round(100 * (disk_info.used / disk_info.total), 3))
			# sys.stdout.write(str(100 * (disk_info.used / disk_info.total)))
			# sys.stdout.flush()
			disk_names.append(disk.device.split(':')[0])
		# print(list(zip(disk_names, disk_useages)))
		return list(zip(disk_names, disk_useages))

	def _remove_file(self):
		"""删除修改时间最大的一个文件

		Args:
		Returns:
		Raises:

		"""
		listfiles = os.listdir(self._path)
		if listfiles is None:
			return None
		filename = []
		filetime = []
		for file in listfiles:
			filename.append(self._path + '\\' + file)
			filetime.append(os.path.getmtime(self._path + '\\' + file))
		if not filetime:
			return False
		maxindex = filetime.index(max(filetime))
		getsize = os.path.getsize(filename[maxindex])
		try:
			os.remove(filename[maxindex])
		except (PermissionError, FileNotFoundError) as error:
			sys.stdout.write(str(error))
			sys.stdout.flush()
			# print(error)
		fileusage = round(getsize / (1024 * 1024 * 1024), 2)
		sys.stdout.write("已将文件{0}删除，文件占用空间{1}GB，文件最后修改时间{2}\n".format(filename[maxindex], fileusage, ctime(filetime[maxindex])))
		sys.stdout.flush()
		return True
		# print("已将文件{0}删除，文件占用空间{1}GB".format(self._path + '\\' + filename[maxindex], fileusage))

	def main(self):
		"""主函数，每900秒检测一次所有逻辑分区的情况，如果均正常则不执行操作

		Args:
		Returns:
		Raises:

		"""
		while True:
			if self._systemversion == 'Windows':
				status = self._get_status(self._path.split(":")[0])
				if status[0]:
					sys.stdout.write("硬盘逻辑分区存储情况良好,存储占用{0}%,等候.....\n".format(str(status[1])))
					sys.stdout.flush()
					# print("等候\t{0}/{1}\t秒\n".format(str(i+1), self._wait))
					sleep(int(self._wait))
				else:  # 只处理和输入路径所属的逻辑分区
					for disk in self._get_disk_info():
						disk_input = self._path.split(":")[0]
						if disk_input == disk[0]:
							status_remove = self._remove_file()
							# if status_remove == False:
							# 	sys.stdout.write("目标目录为空,等候.....\n")
							# 	sys.stdout.flush()
							sleep(1)
							break
			elif self._systemversion == 'Linux':
				pass
			else:
				pass


if __name__ == '__main__':
	u = Utilize()
	u.main()
