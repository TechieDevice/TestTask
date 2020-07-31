import uuid
import json
from enum import Enum
from enum import EnumMeta
from dataclasses import dataclass
import logging
import sys

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


FORMATTER = logging.Formatter(" * %(asctime)s — %(name)s — %(levelname)s — %(message)s")


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


app = Flask(__name__)
app.secret_key = config.SECRET_KEY
app.config.from_object(config.Config)
db = SQLAlchemy(app)
debug_logger = get_logger("logger")


class LinkType(str, Enum):
    partial = "partial"
    hash = "hash"
    direct = "direct"
    junk = "junk"
    base = "base"


prefix = {
    LinkType.partial: "неполная ссылка - ",
    LinkType.hash: "якорь - {base_link}",
    LinkType.direct: "",
    LinkType.junk: "мусор - ",
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
    __tablename__ = "links"

    id = db.Column(db.Integer, primary_key=True)
    base_id = db.Column(db.Integer, db.ForeignKey("base_links.id"))
    url = db.Column(db.String)
    link_type = db.Column(db.String)

    def __repr__(self):
        return f"<Link(base_id={str(self.base_id)}, \
                      url={str(self.url)}, \
                      link_type={str(self.link_type)})>"


class BaseLinkTable(db.Model):
    __tablename__ = "base_links"

    id = db.Column(db.Integer, primary_key=True)
    base_url = db.Column(db.String)

    def __repr__(self):
        return f"<BaseLink(id={str(self.id)}, \
                           url={str(self.base_url)})>"


def addLink(base_id, url, link_type):
    db.session.add(LinkTable(base_id=base_id, url=url, link_type=link_type))
    db.session.commit()


def addBaseLink(base_url):
    db.session.add(BaseLinkTable(base_url=base_url))
    db.session.commit()


def queryLink(url):
    base_link = BaseLinkTable.query.filter_by(base_url=url).first()
    return base_link.id


# db.create_all()                                    # Для внесения изменений)


# --------------MESSAGE SENDER-----------------


def mes_sort(links, base_id, base_link):
    mes = ""
    for link in links:
        link = link_decoder(link)

        prefix = (prefix.get(link.link_type)).format(base_link=base_link.url)
        mes += "{prefix}{url}\n".format(prefix=prefix, url=link.url)
        addLink(base_id, link.url, link.link_type)

    return mes


async def on_response(message: IncomingMessage):
    links: List[Link]
    links = []
    links = json.loads(message.body.decode("utf-8"))
    base_link = link_decoder(links.pop(0))

    addBaseLink(base_link.url)
    id = queryLink(base_link.url)

    mes = mes_sort(links, id, base_link)

    debug_logger.debug(f"{message.correlation_id} {base_link.url} done")
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
            bytes(msg, "utf-8"), correlation_id=request_id, reply_to=callback_queue.name
        ),
        routing_key="linkSender",
    )

    debug_logger.debug(f"{request_id} {base_link.url} send to {callback_queue.name}")

    return str(await future)


async def sender_conn(loop, base_link, request_id):
    conn = await connect("amqp://guest:guest@localhost/", loop=loop)
    channel = await conn.channel()

    queue = await channel.declare_queue("linkReceiver")
    await queue.consume(on_response)

    links_message = await sender(loop, channel, base_link, request_id)

    return links_message


# --------------PAGE-----------------


@app.route("/", methods=["GET", "POST"])
def pageRender():
    if request.method == "POST":
        links_message = ""

        base_link = Link(str(request.form["link"]), LinkType.base)

        request_id = str(uuid.uuid4())

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        links_message = loop.run_until_complete(
            sender_conn(loop, base_link, request_id)
        )

        return render_template("mainPage.html", massage=links_message)

    return render_template("mainPage.html", massage="")


debug_logger.info("Server started. To exit press CTRL+C")
