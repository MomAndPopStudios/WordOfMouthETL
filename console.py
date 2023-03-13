import wikipediaapi
import pandas as pd


def import_platforms(engine, cursor):
    en_wiki = wikipediaapi.Wikipedia('en')
    ignored_platforms = [
        'Amiga arcade conversions', 'Android (operating system)', 'backward-compatible for Xbox One and Series X/S',
        'Famicom Disk System', 'games compatible with EyeToy', 'Lists of best-selling by platform', 'Lists of PC',
        'Lists of Virtual Console', 'multiplayer Game Boy', 'NES', 'Nintendo 3DS Wi-Fi Connection',
        'PlayStation (brand)-related lists', 'Super NES', 'Virtual Console', 'Wii on Wii U eShop',
        'Wii with traditional control schemes', 'Xbox 360 System Link', 'Xbox compatible with Xbox 360',
        'Xbox Live on Windows 10', 'Xbox Live on Windows Phone', 'Xbox network', 'Xbox One X enhanced',
        'Xbox System Link'
    ]
    platforms_df = pd.DataFrame(columns=['platform'])
    page_py = en_wiki.page('Category:Video_game_lists_by_platform')
    platform_categories = page_py.categorymembers
    for category in platform_categories:
        platform = category.replace('List of ', '')
        platform = platform.replace('Category:', '')
        platform = platform.replace('cancelled ', '')
        platform = platform.replace(' homebrew', '')
        platform = platform.replace(' accessories', '')
        platform = platform.replace(' games', '')
        platform = platform.replace('games for ', '')
        platform = platform.replace('Games for ', '')
        platform = platform.replace(' platforms', '')
        platform = platform.replace(' consoles', '')
        platform = platform.replace(' titles', '')
        platform = platform.replace(' (0–9 and A)', '')
        platform = platform.replace(' (A–C)', '')
        platform = platform.replace(' (A–H)', '')
        platform = platform.replace(' (A–K)', '')
        platform = platform.replace(' (A–L)', '')
        platform = platform.replace(' (A–M)', '')
        platform = platform.replace(' (B)', '')
        platform = platform.replace(' (C–G)', '')
        platform = platform.replace(' (D–I)', '')
        platform = platform.replace(' (H–P)', '')
        platform = platform.replace(' (I–O)', '')
        platform = platform.replace(' (J–P)', '')
        platform = platform.replace(' (L–Z)', '')
        platform = platform.replace(' (M–Z)', '')
        platform = platform.replace(' (N–Z)', '')
        platform = platform.replace(' (P–Z)', '')
        platform = platform.replace(' (Q–Z)', '')
        platform = platform.replace(' video', '')
        platform = platform.replace('Virtual Console for ', '')
        platform = platform.replace(' (Japan)', '')
        platform = platform.replace(' (North America)', '')
        platform = platform.replace(' (PAL region)', '')
        platform = platform.replace(' (South Korea)', '')
        platform = platform.replace(' (Taiwan and Hong Kong)', '')
        if platform in ignored_platforms or platform in platforms_df['platform'].values:
            continue
        platform_df = pd.DataFrame([[platform]], columns=['platform'])
        platforms_df = pd.concat([platforms_df, platform_df])
    cursor.execute('truncate table stage.platforms')
    platforms_df.to_sql('platforms', con=engine, if_exists='append', index=False, schema='stage')
    insert_new = (
        'insert into word_of_mouth.platforms (platform)'
        'select '
        'platform '
        'from stage.platforms '
        'except select platform from word_of_mouth.platforms;'
    )
    cursor.execute(insert_new)
