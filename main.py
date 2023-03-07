import json
import wikipediaapi
import pandas as pd
from sqlalchemy import create_engine

# Connect to mariadb
connections = json.load(open('connections.json'))
database_url = connections['database_url']
engine = create_engine(database_url, echo=True)

# Get List of video game genres
wiki_wiki = wikipediaapi.Wikipedia('en')
page_py = wiki_wiki.page('Category:Video game genres')
genres = page_py.categorymembers
ignored_genres = ['Video game genre', 'List of video game genres']
genres_df = pd.DataFrame(columns=['genre', 'summary'])
for genre in genres:
    if genre in ignored_genres:
        continue
    page_py = wiki_wiki.page(genre)
    summary = page_py.summary
    print(genre, summary)
    genre_df = pd.DataFrame([[genre, summary]], columns=['genre', 'summary'])
    genres_df = pd.concat([genres_df, genre_df])
genres_df.to_sql('genres', con=engine, if_exists='append', index=False)

