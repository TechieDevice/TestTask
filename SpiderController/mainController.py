import uuid
import asyncio
from aio_pika import connect, Message, IncomingMessage
from flask import Flask
from flask import render_template
from flask import redirect
from flask import url_for
from flask import request
from flask_sqlalchemy import SQLAlchemy
import config

app = Flask(__name__)
app.secret_key = config.SECRET_KEY
app.config.from_object(config.Config)
db = SQLAlchemy(app)


class GlobalVar:
    futures = dict()


#--------------DATABASE-----------------

class Link(db.Model):
    __tablename__ = 'links'

    id = db.Column(db.Integer, primary_key=True)
    base_url = db.Column(db.String, nullable=False)
    url = db.Column(db.String, nullable=False)

    def __repr__(self):
        return "<Link(name='%s', url='%s')>" % (self.base_url, self.url)


def addLink(base_url, url):
    db.session.add(Link(base_url=base_url, url=url))
    db.session.commit()

def queryPost():
    links = db.session.query(Link).all()
    return links


#--------------MESSAGE SENDER-----------------

async def on_response(message: IncomingMessage):
    mes = message.body.decode('utf-8')
    links = mes.split("\n")
    mes = ''
    base_link = links.pop(0)
    for link in links:
        mes = mes + link + '\n'
        addLink(base_link, link)

    print(message.correlation_id + ' ' + base_link + ' done')
    future = GlobalVar.futures.pop(message.correlation_id)
    future.set_result(mes)
    

async def sender(loop, channel, base_link, user_id):
    future = loop.create_future()
    GlobalVar.futures[user_id] = future

    callback_queue = await channel.declare_queue(exclusive=True)
    await callback_queue.consume(on_response)

    await channel.default_exchange.publish(
        Message(
            bytes(base_link, 'utf-8'),
            correlation_id=user_id,
            reply_to = callback_queue.name
        ), 
        routing_key="linkSender"
    )

    print(user_id + ' ' + base_link + ' send to ' + callback_queue.name)

    return str(await future) 


async def sender_conn(loop, base_link, user_id):
    conn = await connect("amqp://guest:guest@localhost/", loop=loop)
    channel = await conn.channel()

    queue = await channel.declare_queue("linkReceiver")
    await queue.consume(on_response)

    print(user_id + ' ' + base_link + ' presend')

    links_message = await sender(loop, channel, base_link, user_id)

    print(user_id + ' ' + base_link + ' postsend')

    return links_message
    

#--------------PAGE-----------------

@app.route('/', methods=['GET', 'POST'])
def pageRender():
    if request.method == 'POST':
        links_message = ''
        base_link = str(request.form['link'])
        
        user_id = str(uuid.uuid4())

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        links_message = loop.run_until_complete(sender_conn(loop, base_link, user_id))
 
        return render_template('mainPage.html', massage=links_message)

    return render_template('mainPage.html', massage='')
