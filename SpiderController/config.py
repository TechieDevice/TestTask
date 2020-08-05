import os


SECRET_KEY = "da_ladno_kak_ti_ego_ugadal"


class Config:
    SQLALCHEMY_DATABASE_URI = (
        os.getenv("DATABASE_URL")
        or "postgresql://postgres:1234567890@localhost/test_database"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
