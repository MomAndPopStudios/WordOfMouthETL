import wikipediaapi
import pandas as pd


# Inserts List of video game genres from wikipedia
def import_genres(engine, cursor):
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
        genre_df = pd.DataFrame([[genre, summary]], columns=['genre', 'summary'])
        genres_df = pd.concat([genres_df, genre_df])
    cursor.execute('truncate table stage.genres')
    genres_df.to_sql('genres', con=engine, if_exists='append', index=False, schema='stage')
    delete_stage_dupes = (
        'delete s from stage.genres s ' 
        'inner join word_of_mouth.genres w '
        'on s.genre = w.genre '
        'and s.summary = w.summary '
        'where s.genre = w.genre '
        'and s.summary = w.summary;'
    )
    cursor.execute(delete_stage_dupes)
    update_summary = (
        'update word_of_mouth.genres w, stage.genres s '
        'set w.genre = s.genre, '
        'w.summary = s.summary '
        'where s.genre = w.genre and s.summary != w.summary;'
    )
    cursor.execute(update_summary)
    insert_new = (
        'insert into word_of_mouth.genres (genre, summary)'
        'select '
        'genre, summary '
        'from stage.genres '
        'except select genre, summary from word_of_mouth.genres;'
    )
    cursor.execute(insert_new)
