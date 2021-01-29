import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass
from enum import Enum
from functools import partial

import aiohttp
from aio_pika import connect
from aio_pika import Exchange
from aio_pika import IncomingMessage
from aio_pika import Message
from bs4 import BeautifulSoup
from selenium import webdriver


FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler())
    logger.propagate = False
    return logger


class LinkType(str, Enum):
    partial = "partial"
    hash = "hash"
    direct = "direct"
    junk = "junk"
    base = "base"
    init = "init"


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
    browser = webdriver.Chrome("./chromedriver.exe")
    try:
        browser.get(url)
        content = browser.page_source
    except Exception:
        debug_logger.error(f"Неверная ссылка - {url}")
        return "Неправильная ссылка"
    return content


# Получение кода
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


# Скачивание страницы через http клиент
async def load_http(url):
    try:
        async with aiohttp.ClientSession() as session:
            html = await fetch(session, url)
    except aiohttp.client_exceptions.InvalidURL:
        debug_logger.error(f"Неверная ссылка - {url}")
        return "Неправильная ссылка"
    return html


# Парсинг
def parse_data(html_data):
    soup = BeautifulSoup(html_data, "lxml")
    link_list = soup.find_all("a")
    links = []
    for link in link_list:
        links.append(link.get("href"))
    return links


# Запись в файл (для тестов)
def links_writer(url, data, request_id):
    with open(f"./links {request_id}.txt", "w") as file:
        for link in data:
            if link:
                if link[0] == "/":
                    file.write(f"неполная ссылка - {link}\n")
                elif link[0] == "#":
                    file.write(f"якорь - {url}{link}\n")
                elif link[0] != "h":
                    file.write(f"мусор - {link}\n")
                else:
                    file.write(f"{link}\n")
            else:
                file.write(f"{url}\n")

    return f"./links {request_id}.txt"


def link_sort(links_mas, links):
    for link in links_mas:
        if not link:
            continue

        if link[0] == "/":
            links.append(Link(link, LinkType.partial))
        elif link[0] == "#":
            links.append(Link(link, LinkType.hash))
        elif link[0] != "h":
            links.append(Link(link, LinkType.junk))
        else:
            links.append(Link(link, LinkType.direct))

    return links


async def links_fetcher(base_url, request_id):
    start = time.time()
    html_data = await load_http(base_url.url)
    links_mas = parse_data(html_data)

    links = []
    links = link_sort(links_mas, [base_url])

    debug_logger.debug(
        f"Done {base_url.url} for {request_id}, \
          prosess took: {(time.time() - start):.2f} seconds"
    )

    return links


async def on_message(exchange: Exchange, message: IncomingMessage):
    with message.process():
        base_url: Link
        base_url = link_decoder(json.loads(message.body.decode("utf-8")))
        request_id = message.correlation_id

        links = await links_fetcher(base_url, request_id)

        msg = json.dumps(links, cls=Encoder)
        await exchange.publish(
            Message(bytes(msg, "utf-8"), correlation_id=request_id),
            routing_key=message.reply_to,
        )
        debug_logger.debug(
            f"{request_id} {base_url.url} \
                             send to {message.reply_to}"
        )


# Прослушивание
async def main(loop):
    e = 0
    while e < 5:
        await asyncio.sleep(5)
        try:
            connection = await connect("amqp://admin:admin@rabbitmq/", loop=loop)
            channel = await connection.channel()
            queue = await channel.declare_queue("linkSender")
            debug_logger.info("Waiting for messages. To exit press CTRL+C")
            await queue.consume(partial(on_message, channel.default_exchange))
            e = 5
        except ConnectionError as err:
            debug_logger.error(f"{err}")
            if e < 6:
                e += 1
                debug_logger.error("reconnect")
            else:
                debug_logger.debug("exit")
                return



if __name__ == "__main__":
    debug_logger = get_logger("logger")
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    loop.run_forever()
