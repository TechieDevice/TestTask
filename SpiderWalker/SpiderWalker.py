import time
import json
from enum import Enum
from enum import EnumMeta
from dataclasses import dataclass
from functools import partial

import asyncio
import aiohttp
from aio_pika import connect
from aio_pika import Message
from aio_pika import IncomingMessage
from aio_pika import Exchange
from bs4 import BeautifulSoup
import reorder_python_imports
from selenium import webdriver


class LinkType(str, Enum):
    partial_link = 'partial_link'
    hash_link = 'hash_link'
    direct_link = 'direct_link'
    junk_link = 'junk_link'
    base_link = 'base_link'
    init_link = 'init_link'


@dataclass
class Link:
    url: str
    link_type: LinkType


class Encoder(json.JSONEncoder):
    def default(self, obj):
        return obj.__dict__


def link_decoder(link):
    return Link(link["url"], LinkType[link["link_type"]])


# Скачивание кода страницы через браузер
def load_browser(url):
    start = time.time()
    browser = webdriver.Chrome('./chromedriver.exe')
    try:
        browser.get(url)
        content = browser.page_source
        print('Process load took: {:.2f} seconds'.format(time.time() - start))
    except(Exception):
        return "Неправильная ссылка"
    return content


# Получение кода
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


# Скачивание страницы через http клиент
async def load_http(url):
    start = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            html = await fetch(session, url)
        print('Process load took: {:.2f} seconds'.format(time.time() - start))
    except(Exception):
        return "Неправильная ссылка"
    return html


# Парсинг
def parse_data(html_data):
    start = time.time()
    soup = BeautifulSoup(html_data, 'lxml')
    link_list = soup.find_all('a')
    links = []
    for link in link_list:
        links.append(link.get('href'))
    print('Process parse took: {:.2f} seconds'.format(
        time.time() - start))
    return links


# Запись в файл (для тестов)
def links_writer(url, data, request_id):
    start = time.time()
    with open('./links {}.txt'.format(request_id), 'w') as file:
        for link in data:
            if link:
                if link[0] == '/':
                    file.write('неполная ссылка - ' + link + '\n')
                elif link[0] == '#':
                    file.write('якорь - ' + url + link + '\n')
                elif link[0] != 'h':
                    file.write('мусор - ' + link + '\n')
                else:
                    file.write(link + '\n')
            else:
                file.write(url + '\n')
        file.close()
    print('Process write took: {:.2f} seconds'.format(
        time.time() - start))
    return ('./links {}.txt'.format(request_id))


def link_sort(links_mas, links):
    url = Link('', LinkType.init_link)
    print(links_mas)
    for link in links_mas:
        if not link:
            continue

        print(link)
        if link[0] == '/':
            links.append(Link(link, LinkType.partial_link))
        elif link[0] == '#':
            links.append(Link(link, LinkType.hash_link))
        elif link[0] != 'h':
            links.append(Link(link, LinkType.junk_link))
        else:
            links.append(Link(link, LinkType.direct_link))

    return links


async def links_fetcher(base_url, request_id):
    start = time.time()
    html_data = await load_http(base_url.url)
    links_mas = parse_data(html_data)

    links = []
    links.append(base_url)
    links = link_sort(links_mas, links)

    print('Done {} for {}, prosess took: {:.2f} seconds'.format(
        base_url, request_id, time.time() - start))

    return links


async def on_message(exchange: Exchange, message: IncomingMessage):
    with message.process():
        base_url: Link
        base_url = link_decoder(json.loads(message.body.decode('utf-8')))
        request_id = message.correlation_id

        links = await links_fetcher(base_url, request_id)

        msg = json.dumps(links, cls=Encoder)
        await exchange.publish(
            Message(
                bytes(msg, 'utf-8'),
                correlation_id=request_id
            ),
            routing_key=message.reply_to
        )
        print(request_id + ' ' + base_url.url + ' send to ' + message.reply_to)


# Прослушивание
async def main(loop):
    connection = await connect("amqp://guest:guest@localhost/", loop=loop)
    channel = await connection.channel()

    queue = await channel.declare_queue("linkSender")
    await queue.consume(partial(on_message, channel.default_exchange))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
