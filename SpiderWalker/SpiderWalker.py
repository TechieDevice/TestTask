from bs4 import BeautifulSoup
from selenium import webdriver
import asyncio
import time
import aiohttp
from aio_pika import connect, Message, IncomingMessage


# Скачивание кода страницы через браузер
def load_browser(url, num):
    start = time.time()
    browser = webdriver.Chrome('./chromedriver.exe')
    try:
        browser.get(url)
        content = browser.page_source
        print('Process load{} took: {:.2f} seconds'.format(num, time.time() - start))
    except:
        return "Неправильная ссылка"
    return content


# Получение кода
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

# Скачивание страницы через http клиент
async def load_http(url, num):
    start = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            html = await fetch(session, url)
        print('Process load{} took: {:.2f} seconds'.format(num, time.time() - start))
    except:
        return "Неправильная ссылка"
    return html


# Парсинг
async def parse_data(content, num):
    start = time.time()
    soup = BeautifulSoup(content, 'lxml')
    link_list = soup.find_all('a')
    links = []
    for link in link_list:
        links.append(link.get('href'))
    print('Process parse{} took: {:.2f} seconds'.format(num, time.time() - start))
    return links


# Запись в файл (для тестов)
async def links_writer(url, data, num):
    start = time.time()
    with open('./links{}.txt'.format(num), 'w') as file:
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
    print('Process write{} took: {:.2f} seconds'.format(num, time.time() - start))
    return ('./links{}.txt complite'.format(num))


# Получатель ссылок
async def data_releaser(url, num):
    start = time.time()
    print(url)
    #data_content = load_browser(url, num)
    data_content = await load_http(url, num)
    data = await parse_data(data_content, num)
    result = await links_writer(url, data, num)
    await asyncio.sleep(2)
    print('Done{} {}, prosess took: {:.2f} seconds'.format(num, url, time.time() - start))

    return data


# Вызывается при получании сообщения
async def on_message(message: IncomingMessage):
    url = message.body.decode('utf-8')
    num = str(url[0])
    url = url[1:]
    data = await data_releaser(url, num)

    global channel

    for link in data:
        if link:
            if link[0] == '/':
                await channel.default_exchange.publish(Message(bytes(num + 'неполная ссылка - ' + link, 'utf-8')), routing_key="linkReceiver")
            elif link[0] == '#':
                await channel.default_exchange.publish(Message(bytes(num + 'якорь - ' + url + link, 'utf-8')), routing_key="linkReceiver")
            elif link[0] != 'h':
                await channel.default_exchange.publish(Message(bytes(num + 'мусор - ' + link, 'utf-8')), routing_key="linkReceiver")
            else:
                await channel.default_exchange.publish(Message(bytes(num + link, 'utf-8')), routing_key="linkReceiver")

    await channel.default_exchange.publish(Message(bytes(num + 'done', 'utf-8')), routing_key="linkReceiver")


# Прослушивание
async def main(loop):
    global connection
    connection = await connect("amqp://guest:guest@localhost/", loop=loop)
    global channel
    channel = await connection.channel()

    queue = await channel.declare_queue("linkSender")

    await queue.consume(on_message, no_ack=True)


# data_releaser('https://edu.fb.tusur.ru/', 0)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()


#if __name__ == '__main__':
#    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#    channel = connection.channel()
#    asyncio.run(main())

