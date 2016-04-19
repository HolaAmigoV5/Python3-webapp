#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Wby'

'''
async web application.
'''

import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web

def index(request):
    return web.Response(body=b'<h1>Awesome</h1>')

#async相当于@asyncio.coroutine
#await相当于yield from
async def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)   #将URL和函数进行关联
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 9000)  #利用asyncio创建TCP服务
    logging.info('server started at http://127.0.0.1:9000...')
    return srv

#获取EventLoop
loop = asyncio.get_event_loop()
#执行
loop.run_until_complete(init(loop))
loop.run_forever()
