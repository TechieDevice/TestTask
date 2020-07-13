from flask import Flask

from flask import render_template
from flask import redirect
from flask import url_for
from flask import request

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
    if body.decode('utf-8') != 'done':
        url = body.decode('utf-8')
        addLink(urllinks.linkURL, url)
        urllinks.linksMess = urllinks.linksMess + url + ' \n'
    else:
        urllinks.channel.stop_consuming()
        urllinks.channel.close()

@app.route('/', methods=['GET', 'POST'])
def pageRender():
    if request.method == 'POST':
        urllinks.linksMess = ''
        urllinks.channel = urllinks.conn.channel()
        urllinks.linkURL = str(request.form['link'])
        urllinks.channel.queue_declare(queue='linkSender')
        urllinks.channel.basic_publish(exchange='', routing_key='linkSender', body=urllinks.linkURL)
        urllinks.channel.queue_declare(queue='linkReceiver')
        urllinks.channel.basic_consume(queue='linkReceiver', on_message_callback=callback, auto_ack=True)

        urllinks.channel.start_consuming()
        return render_template('mainPage.html', massage=urllinks.linksMess)

    return render_template('mainPage.html', massage='')
