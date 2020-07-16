from bs4 import BeautifulSoup
from selenium import webdriver
import asyncio
import pika
import time

# tasks = []

# Скачивание кода страницы
def load(url, num):
    start = time.time()
    browser = webdriver.Chrome('./chromedriver.exe')
    try:
        browser.get(url)
        content = browser.page_source
        print('Process load{} took: {:.2f} seconds'.format(num, time.time() - start))
    except:
        return "Неправильная ссылка"
    return content

# Парсинг
def parse_data(content, num):
    start = time.time()
    soup = BeautifulSoup(content, 'lxml')
    link_list = soup.find_all('a')
    links = []
    for link in link_list:
        links.append(link.get('href'))
    print('Process parse{} took: {:.2f} seconds'.format(num, time.time() - start))
    return links

# Для тестов
def links_writer(url, data, num):
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

# 
def data_releaser(url, num):
    start = time.time()
    data_content = load(url, num)
    data = parse_data(data_content, num)
    #result = links_writer(url, data, num)
    print('Done{}, prosess took: {:.2f} seconds'.format(num, time.time() - start))

    return data

#def task_maker():
#    while True:
#        if yes == True:
#            i = 0
#            task = asyncio.create_task(data_releaser(url, i))
#            tasks.append(task)
#            await task 
#        await asyncio.sleep(0)


# Вызывается при получании сообщения
def callback(ch, method, properties, body):
    url = body.decode('utf-8')
    data = data_releaser(url, 0)
    channel.queue_declare(queue='linkReceiver')
    for link in data:
        if link:
            if link[0] == '/':
                channel.basic_publish(exchange='', routing_key='linkReceiver', body='неполная ссылка - ' + link)
            elif link[0] == '#':
                channel.basic_publish(exchange='', routing_key='linkReceiver', body='якорь - ' + url + link)
            elif link[0] != 'h':
                channel.basic_publish(exchange='', routing_key='linkReceiver', body='мусор - ' + link)
            else:
                channel.basic_publish(exchange='', routing_key='linkReceiver', body=link)

    channel.basic_publish(exchange='', routing_key='linkReceiver', body='done')

# Прослушивание
async def main():
    channel.queue_declare(queue='linkSender')
    channel.basic_consume(queue='linkSender', on_message_callback=callback, auto_ack=True)
    
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

#async def main():
#    main_task1 = asyncio.create_task(task_maker())
#    main_task2 = asyncio.create_task(conn())
#    await main_task1
#    await main_task2


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    asyncio.run(main())

