import uuid
import json
from dataclasses import dataclass
from enum import Enum, EnumMeta
import asyncio
from aio_pika import connect, Message, IncomingMessage
from flask import Flask
from flask import render_template, request, url_for, redirect
from flask_sqlalchemy import SQLAlchemy
import config


app = Flask(__name__)
app.secret_key = config.SECRET_KEY
app.config.from_object(config.Config)
db = SQLAlchemy(app)


class LinkType(str, Enum):
    partial_link = 'partial_link'
    hash_link = 'hash_link'
    direct_link = 'direct_link'
    junk_link = 'junk_link'
    base_link = 'base_link'


@dataclass
class Link:
    url: str
    link_type: LinkType


class GlobalVar:
    futures = dict()


class Encoder(json.JSONEncoder):
    def default(self, obj):
        return obj.__dict__


def link_decoder(link):
    return Link(link["url"], LinkType[link["link_type"]])


# ---------------DATABASE-----------------

class LinkTable(db.Model):
    __tablename__ = 'links'

    id = db.Column(db.Integer, primary_key=True)
    base_url = db.Column(db.String, nullable=False)
    url = db.Column(db.String, nullable=False)

    def __repr__(self):
        return "<Link(name='%s', url='%s')>" % (self.base_url, self.url)


def addLink(base_url, url):
    db.session.add(LinkTable(base_url=base_url, url=url))
    db.session.commit()


def queryPost():
    links = db.session.query(LinkTable).all()
    return links


# --------------MESSAGE SENDER-----------------

def mes_sort(links, base_link):
    mes = ''
    for link in links:
        link = link_decoder(link)
        addLink(base_link.url, link.url)

        if link.link_type == LinkType.partial_link:
            mes = mes + 'неполная ссылка - ' + link.url + '\n'
        elif link.link_type == LinkType.hash_link:
            mes = mes + 'якорь - ' + base_link.url + link.url + '\n'
        elif link.link_type == LinkType.direct_link:
            mes = mes + link.url + '\n'
        else: 
            mes = mes + 'мусор - ' + link.url + '\n'   

    return mes


async def on_response(message: IncomingMessage):
    links: List[Link]
    links = []
    links = json.loads(message.body.decode('utf-8'))
    base_link = link_decoder(links.pop(0))

    mes = mes_sort(links, base_link)

    print(message.correlation_id + ' ' + base_link.url + ' done')
    future = GlobalVar.futures.pop(message.correlation_id)
    future.set_result(mes)
    

async def sender(loop, channel, base_link, request_id):
    future = loop.create_future()
    GlobalVar.futures[request_id] = future

    callback_queue = await channel.declare_queue(exclusive=True)
    await callback_queue.consume(on_response)

    print(base_link)

    msg = json.dumps(base_link, cls=Encoder)
    print(msg)

    await channel.default_exchange.publish(
        Message(
            bytes(msg, 'utf-8'),
            correlation_id=request_id,
            reply_to = callback_queue.name
        ), 
        routing_key="linkSender"
    )

    print(request_id + ' ' + base_link.url + ' send to ' + callback_queue.name)

    return str(await future) 


async def sender_conn(loop, base_link, request_id):
    conn = await connect("amqp://guest:guest@localhost/", loop=loop)
    channel = await conn.channel()

    queue = await channel.declare_queue("linkReceiver")
    await queue.consume(on_response)

    print(request_id + ' ' + base_link.url + ' presend')

    links_message = await sender(loop, channel, base_link, request_id)

    print(request_id + ' ' + base_link.url + ' postsend')

    return links_message
    

# --------------PAGE-----------------

@app.route('/', methods=['GET', 'POST'])
def pageRender():
    if request.method == 'POST':
        links_message = ''

        base_link = Link(str(request.form['link']), LinkType.base_link)

        request_id = str(uuid.uuid4())

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        links_message = loop.run_until_complete(sender_conn(loop, base_link, request_id))
 
        return render_template('mainPage.html', massage=links_message)

    return render_template('mainPage.html', massage='')
