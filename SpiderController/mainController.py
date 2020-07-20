from flask import Flask

from flask import render_template
from flask import redirect
from flask import url_for
from flask import request
import random

from flask_sqlalchemy import SQLAlchemy
import pika

app = Flask(__name__)

app.secret_key = 'lol_da_ladno_kak_ti_ego_ugadal'
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:1234567890@localhost/test_database'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


class urllinks:
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = conn.channel()
    linkURL = ''
    linksMess = ''
    num = '0'

#--------------DATABASE-----------------

class Link(db.Model):
    __tablename__ = 'links'

    id = db.Column(db.Integer, primary_key=True)
    base_url = db.Column(db.String, nullable=False)
    url = db.Column(db.String, nullable=False)

    def __repr__(self):
        return "<Link(name='%s', url='%s')>" % (self.base_url, self.url)

def addLink(Base_url, strURL):
    db.session.add(Link(base_url=Base_url, url=strURL))
    db.session.commit()

def queryPost():
    links = db.session.query(Link).all()
    return links


#--------------PAGE-----------------

def callback(ch, method, properties, body):
    mes = body.decode('utf-8')
    if str(mes[0]) == urllinks.num:
        url = mes[1:]
        if url != 'done':
            addLink(urllinks.linkURL, url)
            urllinks.linksMess = urllinks.linksMess + url + ' \n'
        else:
            urllinks.channel.stop_consuming()
    else:
        urllinks.channel.queue_declare(queue='linkReceiver')
        urllinks.channel.basic_publish(exchange='', routing_key='linkReceiver', body = mes)



@app.route('/', methods=['GET', 'POST'])
def pageRender():
    if request.method == 'POST':
        urllinks.linksMess = ''
        urllinks.conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        urllinks.channel = urllinks.conn.channel()
        urllinks.linkURL = str(request.form['link'])
        urllinks.num = str(random.randint(1, 9))
        urllinks.channel.queue_declare(queue='linkSender')
        urllinks.channel.basic_publish(exchange='', routing_key='linkSender', body=urllinks.num + urllinks.linkURL)

        print(urllinks.num + ' ' + urllinks.linkURL + ' send')

        urllinks.channel.queue_declare(queue='linkReceiver')
        urllinks.channel.basic_consume(queue='linkReceiver', on_message_callback=callback, auto_ack=True)

        urllinks.channel.start_consuming()
        
        print(urllinks.linkURL + ' done')
        return render_template('mainPage.html', massage=urllinks.linksMess)

    return render_template('mainPage.html', massage='')
