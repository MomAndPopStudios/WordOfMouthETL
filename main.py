import wikipediaapi
import yaml

from console import import_platforms
from genre import import_genres
from sqlalchemy import create_engine
from publisher import import_publishers

# Connect to mariadb
config = yaml.safe_load(open('settings.yml'))

db_config = config['database']
db_username = db_config['username']
db_password = db_config['password']
db_server = db_config['server']
db_port = db_config['port']

database_url = f"mariadb+pymysql://{db_username}:{db_password}@{db_server}:{db_port}/word_of_mouth"
engine = create_engine(database_url, echo=True)
connection = engine.connect().connection
cursor = connection.cursor()

api_config = config['wiki_api']
access_token = api_config['access_token']
authorization_header = {'Authorization': f'Bearer {access_token}'}
en_wiki = wikipediaapi.Wikipedia('en', headers=authorization_header)

import_genres(en_wiki, engine, cursor)
import_platforms(en_wiki, engine, cursor)
import_publishers(en_wiki, engine, cursor)

cursor.close()
connection.commit()
connection.close()
