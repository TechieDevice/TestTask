import time
from functools import partial
import asyncio
import aiohttp
from aio_pika import connect, Message, IncomingMessage, Exchange
from bs4 import BeautifulSoup
from selenium import webdriver

# Скачивание кода страницы через браузер
def load_browser(url):
    start = time.time()
    browser = webdriver.Chrome('./chromedriver.exe')
    try:
        browser.get(url)
        content = browser.page_source
        print('Process load took: {:.2f} seconds'.format( time.time() - start))
    except:
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
    except:
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
    print('Process parse took: {:.2f} seconds'.format(time.time() - start))
    return links


# Запись в файл (для тестов)
def links_writer(url, data, user_id):
    start = time.time()
    with open('./links {}.txt'.format(user_id), 'w') as file:
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
    print('Process write took: {:.2f} seconds'.format(time.time() - start))
    return ('./links {}.txt'.format(user_id))


async def links_fetcher(url, user_id):
    start = time.time()
    print(url)
    #data_content = load_browser(url)
    html_data = await load_http(url)
    links_mas = parse_data(html_data)
    #result = links_writer(url, links, user_id)

    links = url + '\n'
    for link in links_mas:
            if not link:
                continue

            if link[0] == '/':
                links = links + 'неполная ссылка - ' + link + "\n"
            elif link[0] == '#':
                links = links + 'якорь - ' + url + link + "\n"
            elif link[0] != 'h':
                links = links + 'мусор - ' + link + "\n"
            else:
                links = links + link + "\n"

    await asyncio.sleep(2)
    print('Done {} for {}, prosess took: {:.2f} seconds'.format(url, user_id, time.time() - start))

    return links


async def on_message(exchange: Exchange, message: IncomingMessage):
    with message.process():
        url = message.body.decode('utf-8')
        user_id = message.correlation_id

        links = await links_fetcher(url, user_id)

        await exchange.publish(
            Message(
                bytes(links, 'utf-8'),
                correlation_id=user_id
            ),
            routing_key=message.reply_to
        )
        print(user_id + ' ' + url + ' send to ' + message.reply_to)


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



