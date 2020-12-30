"""
Run user-level reports on the data in the local database.
"""

from calendar import monthrange
import datetime
import sqlite3
import click
import numpy as np
import pandas as pd
import tabulate

tabulate.PRESERVE_WHITESPACE = True

SQL_SELECT_COLLECTION = "SELECT * FROM collectionitems WHERE username = ?"
SQL_SELECT_PLAYS = "SELECT * FROM plays WHERE username = ?"
SQL_SELECT_GAMES = 'SELECT * FROM games'
USERNAME = 'NormandyWept'

def base_data(db_path='bgg.db', username=USERNAME):
    """Create data frames of collection items, plays, and games for use in the various reports."""
    data = sqlite3.connect(db_path)
    collectiondata = pd.read_sql_query(SQL_SELECT_COLLECTION, data, params=(username,))
    gamedata = pd.read_sql_query(SQL_SELECT_GAMES, data)
    playdata = pd.read_sql_query(SQL_SELECT_PLAYS, data, params=(username,))
    return (playdata, gamedata, collectiondata)

def forty_char_name(name):
    """Make name 40 characters long exactly, padding with spaces."""
    if len(name) > 40:
        return name[:37] + '...'
    return '{:<40}'.format(name[:40])

def add_gameid_link(forty_name, gameid):
    """Add [thing=X][/thing] tags around a game's name, creating a link on BGG forums."""
    link_text = forty_name.strip()
    extra_spaces = len(forty_name) - len(link_text)
    return '[thing={}]{}[/thing]'.format(gameid, link_text) + (' '*extra_spaces)

def bgg_table(dataframe, title, headers):
    """Run tabulate on the given dataframe, then replace game names with geeklinks."""
    dataframe['name'] = dataframe['name'].map(forty_char_name)
    table = tabulate.tabulate(dataframe, headers=headers, showindex=False, floatfmt='.4f')
    for (gameid, name) in dataframe['name'].iteritems():
        table = table.replace(name, add_gameid_link(name, gameid))
    return '[b][u]{}[/u][/b]\n[c]{}[/c]'.format(title, table)

def hindex_data(plays, games, collection, date):
    """Calculate the items in the h-index for the given set of plays."""
    if date is not None:
        plays = plays[plays['date'] <= date]

    play_totals = plays.groupby('gameid').agg(
        plays=('quantity', 'sum'),
        latest=('date', 'max')
    )
    game_data = pd.merge(games, collection).loc[:, ['gameid', 'name', 'expansion', 'rating']]
    game_data = game_data.set_index('gameid')

    hitems = pd.merge(play_totals, game_data, left_index=True, right_index=True)
    hitems = hitems[hitems['expansion'] == 0]
    hitems['sort_plays'] = -hitems['plays']
    hitems = hitems.sort_values(by=['sort_plays', 'latest'])
    hitems['h'] = np.arange(hitems.shape[0])
    return (
        hitems.loc[
            hitems['h'] < hitems['plays'],
            ['name', 'plays']
        ],
        hitems.loc[
            (hitems['h'] >= hitems['plays']) & (hitems['rating'] == 10),
            ['name', 'plays']
        ]
    )

def new_to_me_data(plays, games, collection, start, finish):
    """Calculate which games are new to me in the provided date range."""
    game_data = pd.merge(games, collection).loc[:, ['gameid', 'name', 'expansion', 'rating']]
    game_data = game_data.set_index('gameid')

    plays['before'] = 0
    plays['during'] = 0
    plays.loc[plays['date'] < start, ['before']] = plays['quantity']
    plays.loc[(start <= plays['date']) & (plays['date'] < finish), ['during']] = plays['quantity']

    new = plays.groupby('gameid').agg(plays=('during', 'sum'), previous=('before', 'sum'))
    new = new[(new['previous'] == 0) & (new['plays'] > 0)]

    merged = pd.merge(new, game_data, left_index=True, right_index=True)
    merged = merged[merged['expansion'] == 0]
    return merged.loc[:, ['name', 'rating']].sort_values(by='rating', ascending=False)

def default_dates(start, finish):
    """Correct any missing dates by populating with appropriate defaults."""
    if start is None and finish is None:
        # default to last month
        first = datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        finish = first - datetime.timedelta(days=1)
        start = finish.replace(day=1)
    elif start is None:
        # default to start of month contaning the finish date
        finish = datetime.datetime.fromisoformat(finish)
        start = finish.replace(day=1)
    elif finish is None:
        # default to end of the month containing the start date
        start = datetime.datetime.fromisoformat(start)
        _, last = monthrange(start.year, start.month)
        finish = start.replace(day=last)

    return start.strftime('%Y-%m-%d'), finish.strftime('%Y-%m-%d')

@click.group()
def cli():
    """Run user-level reports on the data in the local database."""

@cli.command('hindex')
@click.option('--date', default=None)
def hindex(date):
    """Run a report on h-index and games desired to be in it (10-rated)."""
    plays, games, collection = base_data()

    hitems, top10items = hindex_data(plays, games, collection, date)
    hitems.insert(loc=0, column='row_num', value=np.arange(1, 1+len(hitems)))
    top10items.insert(loc=0, column='row_num', value=np.arange(1, 1+len(top10items)))

    filename = '{} {}.txt'.format(date or datetime.datetime.now().strftime('%Y-%m-%d'), 'hindex')

    with open(filename, 'w') as report_file:
        report_file.write(bgg_table(
            hitems,
            'H-Index: {}'.format(hitems.shape[0]),
            ['#', 'Name', 'Plays']
        ))
        report_file.write('\n\n')
        report_file.write(bgg_table(
            top10items,
            'Other favourite games - targets for the list',
            ['#', 'Name', 'Plays']
        ))

    click.echo('Report was output to: {}'.format(filename))

def new_to_me_row(gameid, name, rating):
    ratingcolour = {
        1: '#FF3366',
        2: '#FF3366',
        3: '#FF66CC',
        4: '#FF66CC',
        5: '#9999FF',
        6: '#9999FF',
        7: '#66FF99',
        8: '#66FF99',
        9: '#00CC00',
        10: '#00CC00'
    }
    return '[b][BGCOLOR={}] {} [/BGCOLOR] [thing={}]{}[/thing][/b]\n\n'.format(
        ratingcolour.get(rating, '#A3A3A3'),
        rating or 'N/A',
        gameid,
        name
    )

@cli.command('newtome')
@click.option('--start', default=None)
@click.option('--finish', default=None)
def new_to_me(start, finish):
    """Create a report of new-to-me items in the given date range."""
    start, finish = default_dates(start, finish)
    plays, games, collection = base_data()
    new = new_to_me_data(plays, games, collection, start, finish)

    filename = '{} {}.txt'.format(finish or datetime.datetime.now().strftime('%Y-%m-%d'), 'newtome')

    with open(filename, 'w') as report_file:
        report_file.write('[b][u]NEW TO ME: {} - {}[/u][/b]\n\n'.format(start, finish))
        for gameid, row in new.iterrows():
            report_file.write(new_to_me_row(gameid, row['name'], int(row['rating'])))

    click.echo('Report was output to: {}'.format(filename))

if __name__ == '__main__':
    cli()
