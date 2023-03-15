import pandas as pd


def import_developers(en_wiki, engine, cursor):
    aaa_page_py = en_wiki.page('Category:Video_game_development_companies')
    indie_page_py = en_wiki.page('Category:Indie_video_game_developers')
    aaa_developers = aaa_page_py.categorymembers
    indie_developers = indie_page_py.categorymembers
    developers = aaa_developers | indie_developers
    ignored_developers = [
        'List of video game developers', 'List of indie game developers', 'Indie video game developers',
        'Game developer logos', 'First-party video game developers'
    ]
    developers_df = pd.DataFrame(columns=['developer'])
    for developer in developers:
        developer = developer.replace(' (1993–present)', '')
        developer = developer.replace(' (2018–present)', '')

        developer = developer.replace('Category:', '')
        developer = developer.replace(' (company)', '')
        developer = developer.replace(' (Italian company)', '')
        developer = developer.replace(' (Japanese company)', '')
        developer = developer.replace(' (Korean company)', '')
        developer = developer.replace(' (software company)', '')
        developer = developer.replace(' (video game company)', '')

        developer = developer.replace(' (game designer)', '')
        developer = developer.replace(' (developer)', '')
        developer = developer.replace(' (game developer)', '')
        developer = developer.replace(' (video game developer)', '')
        developer = developer.replace(' (writer)', '')

        if developer in ignored_developers or developer in developers_df['developer'].values:
            continue
        developer_df = pd.DataFrame([[developer]], columns=['developer'])
        developers_df = pd.concat([developers_df, developer_df])
    cursor.execute('truncate table stage.developers')
    developers_df.to_sql('developers', con=engine, if_exists='append', index=False, schema='stage')
    insert_new = (
        'insert into word_of_mouth.developers (developer)'
        'select '
        'developer '
        'from stage.developers '
        'except select developer from word_of_mouth.developers;'
    )
    cursor.execute(insert_new)
