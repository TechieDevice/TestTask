import uuid
import json
from enum import Enum
from enum import EnumMeta
from dataclasses import dataclass

import asyncio
from aio_pika import connect
from aio_pika import Message
from aio_pika import IncomingMessage
from flask import Flask
from flask import render_template
from flask import request
from flask import url_for
from flask import redirect
from flask_sqlalchemy import SQLAlchemy
import reorder_python_imports

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


prefix = {
    LinkType.partial_link: 'неполная ссылка - ', 
    LinkType.hash_link: 'якорь - ', 
    LinkType.direct_link: '', 
    LinkType.junk_link: 'мусор - '
}


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
    base_id = db.Column(db.Integer, db.ForeignKey('base_links.id'))
    url = db.Column(db.String)
    link_type = db.Column(db.String)

    def __repr__(self):
        return "<Link(base='%s', url='%s')>" % (self.base_id, self.url)


class BaseLinkTable(db.Model):
    __tablename__ = 'base_links'

    id = db.Column(db.Integer, primary_key=True)
    base_url = db.Column(db.String)

    def __repr__(self):
        return "<Link(id='%s', url='%s')>" % (self.id, self.base_url)


def addLink(base_id, url, link_type):
    db.session.add(LinkTable(base_id=base_id, url=url, link_type = link_type))
    db.session.commit()


def addBaseLink(base_url):
    db.session.add(BaseLinkTable(base_url=base_url))
    db.session.commit()


def queryLink(url):
    base_link = BaseLinkTable.query.filter_by(base_url=url).first()
    return base_link.id


# db.create_all()                                                       # Для внесения изменений)  


# --------------MESSAGE SENDER-----------------

def mes_sort(links, base_id, base_link):
    mes = ''
    for link in links:
        link = link_decoder(link)

        mes = mes + prefix.get(link.link_type) + link.url + '\n'
        addLink(base_id, link.url, link.link_type)

    return mes


async def on_response(message: IncomingMessage):
    links: List[Link]
    links = []
    links = json.loads(message.body.decode('utf-8'))
    base_link = link_decoder(links.pop(0))

    addBaseLink(base_link.url)
    id = queryLink(base_link.url)

    mes = mes_sort(links, id, base_link)

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
