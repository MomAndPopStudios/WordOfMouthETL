import pandas as pd
import re


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
        game_name = game.game

        wiki_url = 'https://en.wikipedia.org/wiki/' + game_url
        try:
            tables = pd.read_html(wiki_url, attrs={'class': 'infobox'})
        except:
            continue
        for game_info_df in tables:
            # Check if game is in database, if not add it
            if game_name not in games_df['game'].values:
                cursor.execute('insert into word_of_mouth.games (game) values (%s) returning id', (game_name,))
                game_id = cursor.fetchone()
            else:
                game_id = games_df.loc[games_df['game'] == game_name].iloc[0]['id']
            # filter Mode(s) row, split and find each id or add to DB
            mode_df = game_info_df.loc[game_info_df.iloc[:, 0] == 'Mode(s)']
            if mode_df.empty:
                print("Mode for " + game_name + " not found")
                continue
            mode_list = mode_df.iloc[0].values[1]
            mode_list = mode_list.split(',')
            html_jargon_list = [mode for mode in mode_list if '.mw' in mode]
            if html_jargon_list:
                print("Mode for " + game_name + " has HTML jargon")
                continue
            for mode in mode_list:
                mode = mode.strip()
                if mode not in modes_df['mode'].values:
                    cursor.execute('insert into word_of_mouth.modes (mode) values (%s) returning id', (mode,))
                    mode_id = cursor.fetchone()
                else:
                    mode_id = modes_df.loc[modes_df['mode'] == mode].iloc[0]['id']
                new_game_mode_df = pd.DataFrame([[game_id, mode_id]], columns=['game_id', 'mode_id'])
                stage_game_mode_df = pd.concat([stage_game_mode_df, new_game_mode_df])
            # filter Developer(s) row, split and find each id
            developer_df = game_info_df.loc[game_info_df.iloc[:, 0] == 'Developer(s)']
            if developer_df.empty:
                print("Developer for " + game_name + " not found")
                continue
            developer_list = developer_df.iloc[0].values[1]
            developer_list = developer_list.split(',')
            developer_list = [developer for developer in developer_list if ".mw" not in developer]
            for developer in developer_list:
                developer = developer.strip()
                if developer not in developers_df['developer'].values:
                    cursor.execute('insert into word_of_mouth.developers (developer) values (%s) returning id',
                                   (developer,))
                    developer_id = cursor.fetchone()
                else:
                    developer_id = developers_df.loc[developers_df['developer'] == developer].iloc[0]['id']
                new_game_developer_df = pd.DataFrame([[game_id, developer_id]], columns=['game_id', 'developer_id'])
                stage_game_developer_df = pd.concat([stage_game_developer_df, new_game_developer_df])

            # filter Publisher(s) row, split and find each id
            publisher_df = game_info_df.loc[game_info_df.iloc[:, 0] == 'Publisher(s)']
            if not publisher_df.empty:
                publisher_list = publisher_df.iloc[0].values[1]
                publisher_list = publisher_list.split(',')
                if len(publisher_list) == 1:
                    publisher_list = publisher_list[0].split('  ')
                publisher_list = [publisher for publisher in publisher_list if ".mw" not in publisher]
                html_jargon_list = [publisher for publisher in publisher_list if ".mw" in publisher]
                if len(html_jargon_list) > 0:
                    print("Publisher for " + game_name + " uses html jargon")
                for publisher in publisher_list:
                    publisher = publisher.strip()
                    publisher = remove_parenthesis(publisher)
                    if publisher in publishers_df['publisher'].values:
                        publisher_id = publishers_df.loc[publishers_df['publisher'] == publisher].iloc[0]['id']
                    else:
                        cursor.execute('insert into word_of_mouth.publishers (publisher) values (%s) returning id',
                                       (publisher,))
                        publisher_id = cursor.fetchone()
                    new_game_publisher_df = pd.DataFrame([[game_id, publisher_id]], columns=['game_id', 'publisher_id'])
                    stage_game_publisher_df = pd.concat([stage_game_publisher_df, new_game_publisher_df])
            else:
                print("Publisher for " + game_name + " not found")
            # filter Platform(s) row, split and find each id
            platform_df = game_info_df.loc[game_info_df.iloc[:, 0] == 'Platform(s)']
            if platform_df.empty:
                print("Platform for " + game_name + " not found")
                continue
            platform_list = platform_df.iloc[0].values[1]
            platform_list = platform_list.split(',')
            platform_list = [platform for platform in platform_list if ".mw" not in platform]
            for platform in platform_list:
                platform = platform.strip()
                if platform not in platforms_df['platform'].values:
                    cursor.execute('insert into word_of_mouth.platforms (platform) values (%s) returning id',
                                   (platform,))
                    platform_id = cursor.fetchone()
                else:
                    platform_id = platforms_df.loc[platforms_df['platform'] == platform].iloc[0]['id']
                new_game_platform_df = pd.DataFrame([[game_id, platform_id]], columns=['game_id', 'platform_id'])
                stage_game_platform_df = pd.concat([stage_game_platform_df, new_game_platform_df])

            # filter Genre(s) row, split and find each id
            genre_df = game_info_df.loc[game_info_df.iloc[:, 0] == 'Genre(s)']
            if genre_df.empty:
                print("Genre for " + game_name + " not found")
                continue
            genre_list = genre_df.iloc[0].values[1]
            genre_list = genre_list.split(',')
            for genre in genre_list:
                genre = genre.strip()
                genre_game = genre + ' game'
                is_genre_in_db = genre in genres_df['genre'].values
                is_genre_game_in_db = genre_game in genres_df['genre'].values
                if is_genre_in_db:
                    genre_id = genres_df.loc[genres_df['genre'] == genre].iloc[0]['id']
                elif is_genre_game_in_db:
                    genre_id = genres_df.loc[genres_df['genre'] == genre_game].iloc[0]['id']
                else:
                    if ".mw" in genre:
                        print(game_name + " has a genre that is html jargon " + genre)
                        continue
                    cursor.execute('insert into word_of_mouth.genres (genre) values (%s) returning id', (genre,))
                    genre_id = cursor.fetchone()
                new_game_genre_df = pd.DataFrame([[game_id, genre_id]], columns=['game_id', 'genre_id'])
                stage_game_genre_df = pd.concat([stage_game_genre_df, new_game_genre_df])

    cursor.execute('truncate table stage.game_developer')
    stage_game_developer_df.to_sql('game_developer', con=engine, if_exists='append', index=False, schema='stage')
    insert_new_game_developer = (
        'insert into word_of_mouth.game_developer (game_id, developer_id)'
        'select game_id, developer_id from stage.game_developer '
        'except select game_id, developer_id from word_of_mouth.game_developer;'
    )
    cursor.execute(insert_new_game_developer)
    cursor.execute('truncate table stage.game_publisher')
    stage_game_publisher_df.to_sql('game_publisher', con=engine, if_exists='append', index=False, schema='stage')
    insert_new_game_publisher = (
        'insert into word_of_mouth.game_publisher (game_id, publisher_id)'
        'select game_id, publisher_id from stage.game_publisher '
        'except select game_id, publisher_id from word_of_mouth.game_publisher;'
    )
    cursor.execute(insert_new_game_publisher)
    cursor.execute('truncate table stage.game_platform')
    stage_game_platform_df.to_sql('game_platform', con=engine, if_exists='append', index=False, schema='stage')
    insert_new_game_platform = (
        'insert into word_of_mouth.game_platform (game_id, platform_id)'
        'select game_id, platform_id from stage.game_platform '
        'except select game_id, platform_id from word_of_mouth.game_platform;'
    )
    cursor.execute(insert_new_game_platform)

    connection.close()


def remove_brackets(string):
    string = str(string)
    return re.sub(r'\[.*?]', '', string)


def remove_parenthesis(string):
    string = str(string)
    return re.sub(r'\(.*?\)', '', string)


def add_underscores(string):
    return string.replace(' ', '_')
