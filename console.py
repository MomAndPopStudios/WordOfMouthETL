import wikipediaapi
import pandas as pd


def import_platforms(engine):
    wiki_wiki = wikipediaapi.Wikipedia('en')
    page_py = wiki_wiki.page('Category:Video_game_consoles')
    platforms = page_py.categorymembers
    ignored_platforms = ['Video game platform', 'List of video game platforms']
    platforms_df = pd.DataFrame(columns=['platform', 'summary'])
    for platform in platforms:
        if platform in ignored_platforms:
            continue
        page_py = wiki_wiki.page(platform)
        summary = page_py.summary
        print(platform, summary)
        platform_df = pd.DataFrame([[platform, summary]], columns=['platform', 'summary'])
        platforms_df = pd.concat([platforms_df, platform_df])
    platforms_df.to_sql('platforms', con=engine, if_exists='append', index=False)