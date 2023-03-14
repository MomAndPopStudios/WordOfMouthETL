import wikipediaapi
import pandas as pd


# Inserts List of video game genres from wikipedia
def import_publishers(engine, cursor):
    wiki_wiki = wikipediaapi.Wikipedia('en')
    page_py = wiki_wiki.page('Category:Video_game_publishers')
    publishers = page_py.categorymembers
    ignored_publishers = []
    publishers_df = pd.DataFrame(columns=['publisher'])
    for publisher in publishers:
        publisher = publisher.replace('Category:', '')
        publisher = publisher.replace(' (company)', '')
        publisher = publisher.replace(' (video game company)', '')
        publisher = publisher.replace(' (game company)', '')
        publisher = publisher.replace(' (1993–present)', '')
        publisher = publisher.replace(' (software house)', '')
        publisher = publisher.replace(' (video game developer)', '')
        publisher = publisher.replace(' (software publisher)', '')
        publisher = publisher.replace(' (Minnesota company)', '')
        publisher = publisher.replace(' (Japanese company)', '')
        publisher = publisher.replace(' (game producer)', '')
        publisher = publisher.replace('  (2018–present)', '')
        if publisher in ignored_publishers or publisher in publishers_df['publisher'].values:
            continue
        publisher_df = pd.DataFrame([[publisher]], columns=['publisher'])
        publishers_df = pd.concat([publishers_df, publisher_df])
    cursor.execute('truncate table stage.publishers')
    publishers_df.to_sql('publishers', con=engine, if_exists='append', index=False, schema='stage')
    insert_new = (
        'insert into word_of_mouth.publishers (publisher)'
        'select '
        'publisher '
        'from stage.publishers '
        'except select publisher from word_of_mouth.publishers;'
    )
    cursor.execute(insert_new)
