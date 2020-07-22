import os


SECRET_KEY = 'da_ladno_kak_ti_ego_ugadal'

class Config(object):
    #SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
    SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:1234567890@localhost/test_database'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
