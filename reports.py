"""
Run user-level reports on the data in the local database.
"""

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

if __name__ == '__main__':
    cli()
