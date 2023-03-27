import pandas as pd
import requests
import re
from bs4 import *


def import_games(en_wiki, engine, cursor):
    # Get list of games from Wikipedia
    genre_games_page = en_wiki.page("Category:Video_game_lists_by_genre")
    genre_games = genre_games_page.categorymembers
    games_df = pd.DataFrame(columns=['game', 'url'])
    ignored_genres = [
        'List_of_quiz_arcade_games', 'List_of_shogi_software', 'Template:Video_game_lists_by_genre',
        'List_of_best-selling_Japanese_role-playing_game_franchises'
        ]
    non_table_game_list = []
    for game_list_page in genre_games:
        game_list_url = add_underscores(game_list_page)
        if game_list_url in ignored_genres or 'Category:' in game_list_url:
            continue
        wiki_url = 'https://en.wikipedia.org/wiki/' + game_list_url
        table_attributes = {'class': 'wikitable sortable'}
        try:
            tables = pd.read_html(wiki_url, attrs=table_attributes)
        except ValueError:
            tables = pd.read_html(wiki_url)
        for table_df in tables:
            columns = table_df.columns
            has_game = 'Game' in columns
            has_name = 'Name' in columns
            has_title = 'Title' in columns
            game_column = ''
            if game_list_url == "List_of_beat_'em_ups":
                print(table_df.columns)
                games = []
                if '2D gameplay' in columns and '3D gameplay' in columns:
                    games_2d = table_df['2D gameplay']
                    print(games_2d)
                    games_3d = table_df['3D gameplay']
                    print(games_3d)
                    games = games_2d.append(games_3d)
                print(games)
                continue
            if not has_game and not has_name and not has_title:
                if game_list_url not in non_table_game_list:
                    non_table_game_list.append(game_list_url)
                continue
            if has_game:
                game_column = 'Game'
            if has_name:
                game_column = 'Name'
            if has_title:
                game_column = 'Title'
            table_df[game_column] = table_df[game_column].apply(remove_brackets)
            games = table_df[game_column]
            table_df[game_column] = table_df[game_column].apply(add_underscores)
            urls = table_df[game_column]
            stage_games_df = pd.DataFrame({'game': games, 'url': urls})
            games_df = pd.concat([stage_games_df, games_df])
    games_df = games_df.drop_duplicates()
    print('Game list size:' + str(games_df.size))
    print('Non-table game lists size: ' + str(len(non_table_game_list)) + '\n' + str(non_table_game_list))


def remove_brackets(string):
    string = str(string)
    return re.sub(r'\[.*?]', '', string)


def add_underscores(string):
    return string.replace(' ', '_')


def clean_game_name(string):
    string = string.split(' - ')[0]
    return re.sub(r'[(].*?[)]', '', string)
