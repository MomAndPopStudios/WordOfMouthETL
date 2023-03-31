import pandas as pd
import requests
import re
from bs4 import *


def import_games(en_wiki, engine, cursor):
    # Variable initialization and setup
    connection = engine.raw_connection()
    genre_games_page = en_wiki.page("Category:Video_game_lists_by_genre")
    genre_games_members = genre_games_page.categorymembers
    wiki_games_df = pd.DataFrame(columns=['game', 'url'])
    stage_game_developer_df = pd.DataFrame(columns=['game_id', 'developer_id'])
    stage_game_genre_df = pd.DataFrame(columns=['game_id', 'genre_id'])
    stage_game_mode_df = pd.DataFrame(columns=['game_id', 'mode_id'])
    stage_game_platform_df = pd.DataFrame(columns=['game_id', 'platform_id'])
    stage_game_publisher_df = pd.DataFrame(columns=['game_id', 'publisher_id'])
    games_df = pd.read_sql('select * from word_of_mouth.games', con=connection)
    developers_df = pd.read_sql('select * from word_of_mouth.developers', con=connection)
    publishers_df = pd.read_sql('select * from word_of_mouth.publishers', con=connection)
    platforms_df = pd.read_sql('select * from word_of_mouth.platforms', con=connection)
    genres_df = pd.read_sql('select * from word_of_mouth.genres', con=connection)
    modes_df = pd.read_sql('select * from word_of_mouth.modes', con=connection)
    game_developer_df = pd.read_sql('select * from word_of_mouth.game_developer', con=connection)
    game_genre_df = pd.read_sql('select * from word_of_mouth.game_genre', con=connection)
    game_mode_df = pd.read_sql('select * from word_of_mouth.game_mode', con=connection)
    game_platform_df = pd.read_sql('select * from word_of_mouth.game_platform', con=connection)
    game_publisher_df = pd.read_sql('select * from word_of_mouth.game_publisher', con=connection)
    ignored_genres = [
        'List_of_quiz_arcade_games', 'List_of_shogi_software', 'Template:Video_game_lists_by_genre',
        'List_of_best-selling_Japanese_role-playing_game_franchises'
    ]
    non_table_game_list = []

    # Create a list of games from each Wikipedia genre game list
    for game_list_page in genre_games_members:
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
            stage_game_df = pd.DataFrame({'game': games, 'url': urls})
            wiki_games_df = pd.concat([stage_game_df, wiki_games_df])
    wiki_games_df = wiki_games_df.drop_duplicates()
    print('Game list size:' + str(wiki_games_df.size))
    print('Non-table game lists size: ' + str(len(non_table_game_list)) + '\n' + str(non_table_game_list))

    # Find games with wiki pages and add to DB
    for game in wiki_games_df.itertuples():
        game_url = game.url
        game_name = game.name

        wiki_url = 'https://en.wikipedia.org/wiki/' + game_url
        try:
            tables = pd.read_html(wiki_url, attrs={'class': 'infobox'})
        except ValueError:
            continue
        for game_info_df in tables:
            # Check if game is in database, if not add it
            if game_name not in games_df['game'].values:
                stage_game_df = pd.DataFrame({'game': game_name})
                stage_game_df.to_sql('games', con=engine, if_exists='append', index=False, schema='word_of_mouth')
                games_df = pd.read_sql('select * from word_of_mouth.games', con=connection)
            game_id = games_df.loc[games_df['game'] == game_name, 'id'].values[0]
            # filter Developer(s) row, split and find each id
            developer_df = game_info_df.filter(like='Developer(s)', axis=0)
            if developer_df.empty:
                continue
            developer_list = developer_df.iloc[0].values[0]
            developer_list = developer_list.split(',')
            for developer in developer_list:
                developer = developer.strip()
                developer_id = developers_df.loc[developers_df['developer'] == developer, 'id'].values[0]
                stage_game_developer_df = pd.concat(
                    [stage_game_developer_df, pd.DataFrame({'game_id': game_id, 'developer_id': developer_id})])
                print(developer_id)
            # filter Publisher(s) row, split and find each id
            # filter Platform(s) row, split and find each id
            # filter Genre(s) row, split and find each id
    cursor.execute('truncate table stage.game_developer')
    stage_game_developer_df.to_sql('game_developer', con=engine, if_exists='append', index=False, schema='stage')
    insert_new_game_developer = (
        'insert into word_of_mouth.game_developer (game_id, developer_id)'
        'select '
        'game_id, developer_id '
        'from stage.game_developer '
        'except select publisher from word_of_mouth.publishers;'
    )
    connection.close()


def remove_brackets(string):
    string = str(string)
    return re.sub(r'\[.*?]', '', string)


def add_underscores(string):
    return string.replace(' ', '_')
