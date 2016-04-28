#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__='Wby'

import asyncio,logging
logging.basicConfig(level=logging.INFO)

import aiomysql  #MySql异步IO驱动

def log(sql,args=()):
	logging.info('SQL:%s'%sql)

#创建数据库连接池
@asyncio.coroutine
def create_pool(loop,**kw):
	logging.info('create database connection pool...')
	#全局变量__pool
	global __pool
	#创建数据库连接池
	__pool=yield from aiomysql.create_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf8'),
		autocommit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
		)
#查询函数，该函数是协程
@asyncio.coroutine
def select(sql,args,size=None):
	log(sql,args) #调用log函数写日志
	global __pool
	#从连接池取出一个conn处理，with...as...会在运行完后把conn放回连接池
	with (yield from __pool) as conn:
		#获取一个cursor，通过aiomysql.DictCursor获取到的cursor在返回结果时会返回一个字典格式
		cur=yield from conn.cursor(aiomysql.DictCursor)
		#将SQL语句的占位符?替换为MySql的占位符%s，并执行SQL
		yield from cur.execute(sql.replace('?','%s'),args or ()) 
		if size:
			rs=yield from cur.fetchmany(size)
		else:
			rs=yield from cur.fetchall()
		yield from cur.close()  #关闭cursor
		logging.info('rows returned:%s'%len(rs))
		return rs
#用于执行insert,update,delete语句
@asyncio.coroutine
def execute(sql,args,autocommit=True):
	#yield from print('SQL:',sql,'Args:',args)
	log(sql)
	#async with __pool.get() as conn:
	with (yield from __pool) as conn:
		if not autocommit:
			#await conn.begin()
			yield from conn.begin()
		try:
			#async with conn.cursor(aiomysql.DictCursor) as cur:
			cur=yield from conn.cursor()
			#await cur.execute(sql.replace('?','%s'),args)
			yield from cur.execute(sql.replace('?','%s'),args)
			affected=cur.rowcount
			yield from cur.close()
			if not autocommit:
				#await conn.commit()
				yield from conn.commit()
		except BaseException as e:
			if not autocommit:
				#await conn.rollback()
				yield from conn.rollback()
			raise
		return affected

#构造sql语句参数字符串，最后返回的字符串会以‘,’分割多个'?'，如num==2,则会返回'?,?'
def create_args_string(num):
	L=[]
	for n in range(num):
		L.append('?')
	return ', '.join(L)

#用于标识model里每个成员变量的类
#name:名字
#column_type:列类型
#primary_key:是否是主键
#default:默认值
class Field(object):
	"""用于标识model里每个成员变量的类"""
	#init函数，在对象new之后初始化时自动调用，这里初始化一些成员变量
	def __init__(self, name,column_type,primary_key,default):
		self.name=name
		self.column_type=column_type
		self.primary_key=primary_key
		self.default=default

	#直接打印对象的实现方法
	def __str__(self):
		return '<%s,%s:%s>'%(self.__class__.__name__,self.column_type,self.name)
#string类型的默认设定
class StringField(Field):
	"""docstring for StringField"""
	def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
		super().__init__(name,ddl,primary_key,default)

#bool类型的默认设定
class BooleanField(Field):
	"""docstring for BooleanField"""
	def __init__(self, name=None,default=False):
		super().__init__(name,'boolean',False,default)

#integer类型的默认设定
class IntegerField(Field):
	"""docstring for IntegerField"""
	def __init__(self, name=None,primary_key=False,default=0):
		super().__init__(name,'bigint',primary_key,default)

#float类型的默认设定
class FloatField(Field):
	"""docstring for FloatField"""
	def __init__(self,name=None,primary_key=False,default=0.0):
		super().__init__(name,'real',primary_key,default)

#text类型的默认设定
class TextField(Field):
	"""docstring for TextField"""
	def __init__(self, name=None,default=None):
		super().__init__(name,'text',False,default)

#model元类，元类可以创建类对象，可以查看这个http://blog.jobbole.com/21351/,了解元类
class ModelMetaclass(type):
	#new函数
	#cls:当前准备创建的类的对象
	#name:创建的类名
	#bases:父类的数组，可为空，如果有值的话，生成的类是继承此数组里的类
	#attrs:包含属性的字典
	def __new__(cls,name,bases,attrs):
		#如果当前类是Model类，直接返回
		if name=='Model':
			return type.__new__(cls,name,bases,attrs)

		#Model的子类会继续往下走
		#获取table名称:attrs的‘__table__’键对应的value，如果为空的话则用neme字段的值
		tableName=attrs.get('__table__',None) or name
		logging.info('found model:%s(table:%s)'%(name,tableName))
		#获取所有Field和主键名
		mappings=dict()		#mappings字典，存放所有Field键值对，属性名：value
		fields=[]			#fields数组，存放除了主键以外的属性名
		primaryKey=None		#primaryKey主键
		for k,v in attrs.items():	#k,v分别对应创建时传进来的需要赋值的属性名，和要赋的值
			if isinstance(v,Field): #查看value是否是Field类型，是的话继续
				logging.info('found mapping:%s==>%s'%(k,v))
				mappings[k]=v       #吧符合要求的放到mappings里面
				if v.primary_key:	#如果当前value是主键，则记下来
					#找到主键
					if primaryKey:
						raise StandardError('Duplicate primary key for field:%s'%k)
					primaryKey=k    #记录主键
				else:
					fields.append(k)#不是主键的话把key值放到fields里
		if not primaryKey:
			#如果遍历后没有主键，抛错
			raise StandardError('Primary key not found')
		
		#把attrs里除了主键以外的其他键去掉
		for k in mappings.keys():
			attrs.pop(k)

		escaped_fields=list(map(lambda f:'%s'%f,fields))     #把fields的值全部加个''
		attrs['__mappings__']=mappings 			#保存属性和列的映射关系
		attrs['__table__']=tableName			#表名
		attrs['__primary_key__']=primaryKey 	#主键属性名
		attrs['__fields__']=fields 				#除主键外的属性名
		#构造默认的SELECT,INSERT,UPDATE和DELETE语句
		attrs['__select__']='select `%s`,%s from `%s`'%(primaryKey,', '.join(escaped_fields),tableName)
		attrs['__insert__']='insert into `%s` (%s,`%s`) values (%s)'%(tableName,', '.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
		attrs['__update__']='update `%s` set %s where `%s`=?'%(tableName,', '.join(map(lambda f:'`%s`=?'%(mappings.get(f).name or f),fields)),primaryKey)
		attrs['__delete__']='delete from `%s` where `%s`=?'%(tableName,primaryKey)
		#构造类
		return type.__new__(cls,name,bases,attrs)

#Model类，元类是ModelMetalclass
class Model(dict,metaclass=ModelMetaclass):
	"""ORM映射的基类Model"""
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	#重写访问属性的方法，没有属性和Key一样抛错
	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'"%key)

	#重写写属性的方法
	def __setattr(self,key,value):
		self[key]=value

	def getValue(self,key):
		return getattr(self,key,None)
	
	#访问某个key的方法，如果value是None，则去mappings获取default的值
	def getValueOrDefault(self,key):
		value=getattr(self,key,None)
		if value is None:
			field=self.__mappings__[key]
			if field.default is not None:
				value=field.default() if callable(field.default) else field.default
				logging.info('using default value for %s:%s'%(key,str(value)))
				setattr(self,key,value)
		return value

	#查询所有，可以设定查询顺序'order by',查询条件'limit'
	@classmethod
	@asyncio.coroutine
	def findAll(cls,where=None,args=None,**kw):
		'find objects by where clause.'
		sql=[cls.__select__]
		if where:
			sql.append('where')
			sql.append(where)

		if args is None:
			args=[]
		orderBy=kw.get('order by')
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit=kw.get('limit',None)
		if limit is not None:
			sql.append('limit')
			if  isinstance(limit,int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit,tuple) and len(limit)==2:
				sql.append('?,?')
				args.extend(limit)
			else:
				raise ValueError('Invalid limit value:%s'%str(limit))
		rs=yield from select(' '.join(sql),args)
		return [cls(**r) for r in rs]

	#查询某个条件下的数据有多少条
	@classmethod
	@asyncio.coroutine
	def findNumber(cls,selectField,where=None,args=None):
		'find number by select and where'
		sql=['select %s _num_ from `%s`'%(selectField,cls.__table__)]
		if where:
			sql.append('where')
			sql.append(where)
		rs=yield from select(' '.join(sql),args,1)
		if len(rs)==0:
			return None
		return rs[0]['_num_']

	#根据主键查找pk的值，取第一条
	@classmethod
	@asyncio.coroutine
	def find(cls,pk):
		'find object by primary key.'
		rs=yield from select('%s where `%s`=?'%(cls.__select__,cls.__primary_key__),[pk],1)
		if len(rs)==0:
			return None
		return cls(**rs[0])

	#根据当前类的属性，往相关table里插入一条数据
	@asyncio.coroutine
	def save(self):
		args=list(map(self.getValueOrDefault,self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows=yield from execute(self.__insert__,args)
		if rows!=1:
			logging.warn('failed to insert record:affected rows:%s'%rows)

	#更新条目数据
	@asyncio.coroutine
	def update(self):
		args=list(map(self.getValue,self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows=yield from execute(self.__update__,args)
		if rows!=1:
			logging.warn('failed to update by primary key:affected rows:%s'%rows)

	#根据主键的值删除条目
	@asyncio.coroutine
	def remove(self):
		args=[self.getValue(self.__primary_key__)]
		rows=yield from execute(self.__delete__,args)
		if rows!=1:
			logging.warn('failed to remove by primary key:affected rows:%s'%rows)
