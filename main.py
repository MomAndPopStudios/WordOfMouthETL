import json

from console import import_platforms
from genre import import_genres
from sqlalchemy import create_engine
from publisher import import_publishers

# Connect to mariadb
connections = json.load(open('connections.json'))
database_url = connections['database_url']
engine = create_engine(database_url, echo=True)
connection = engine.connect().connection
cursor = connection.cursor()

import_genres(engine, cursor)
import_platforms(engine, cursor)
import_publishers(engine, cursor)

cursor.close()
connection.commit()
connection.close()
