import pandas as pd
import requests
from bs4 import *


def import_games(en_wiki, engine, cursor):
    # Get list of games from Wikipedia
    genre_games_page = en_wiki.page("Category:Video_game_lists_by_genre")
    genre_games = genre_games_page.categorymembers
    ignored_lists = []
    game_urls = []
    for game_list_page in genre_games:
        game_list_url = game_list_page.replace(' ', '_')
        wiki_url = 'https://en.wikipedia.org/wiki/' + game_list_url
        games_df = pd.DataFrame(columns=['game', 'url'])
        print('********************\n'+game_list_page+'\n********************')
        table_attributes = {'class': 'wikitable sortable'}
        table_df = pd.read_html(wiki_url, attrs=table_attributes)
