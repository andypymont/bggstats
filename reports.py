"""
Run reports on the data in the local database.
"""

import datetime
import sqlite3
import numpy as np
import pandas as pd
import click
from tabulate import tabulate

GUILD = 901
EXTRA_RATINGS = 5

SQL_SELECT_COLLECTIONS = 'SELECT * FROM collectionitems'
SQL_SELECT_GUILD_MEMBERS = 'SELECT * FROM guildmembers'
SQL_SELECT_GAMES = 'SELECT * FROM games'

@click.group()
def cli():
    """Run reports on data in the local database."""

def guild_collection_data(guild=GUILD):
    """Return a single data frame of all collection items for guild members."""
    # run SQL queries
    data = sqlite3.connect('bgg.db')
    guildmembers = pd.read_sql_query(SQL_SELECT_GUILD_MEMBERS, data)
    collections = pd.read_sql_query(SQL_SELECT_COLLECTIONS, data)
    games = pd.read_sql_query(SQL_SELECT_GAMES, data)

    # filter for relevant guild
    guildmembers = guildmembers[guildmembers['guildid'] == guild]

    # merge dataframes
    return pd.merge(pd.merge(guildmembers, collections), games)

def adjusted_average(gcs_row):
    """Add extra '5' ratings to the guild average to account for small sample sizes."""
    total = (gcs_row['guild_average'] * gcs_row['guild_ratings']) + (5 * EXTRA_RATINGS)
    return total / (gcs_row['guild_ratings'] + EXTRA_RATINGS)

def guild_collection_summary(guild=GUILD):
    """Return a data frame summarising key attributes of guild members' collections."""
    gcd = guild_collection_data(guild)
    gcs = gcd.groupby('gameid').agg(
        name=('name', 'first'),
        is_expansion=('expansion', 'first'),
        copies_owned=('owned', 'sum'),
        bgg_average=('rating_average', 'first'),
        guild_average=('rating', 'mean'),
        guild_std=('rating', 'std'),
        guild_ratings=('rating', 'count')
    )
    gcs['guild_adj_average'] = gcs.apply(adjusted_average, axis='columns')
    return gcs

def thirty_char_name(name):
    """Make name 30 characters long exactly, padding with spaces."""
    if len(name) > 30:
        return name[:27] + '...'
    return '{:<30}'.format(name[:30])

def bgg_table(dataframe, headers):
    """Run tabulate on the given dataframe, then replace game names with geeklinks."""
    if 'name' in dataframe.columns:
        dataframe['name'] = dataframe['name'].map(thirty_char_name)

    table = tabulate(dataframe, headers=headers, showindex=False, floatfmt='.4f')

    if 'name' in dataframe.columns:
        for (gameid, name) in dataframe['name'].iteritems():
            table = table.replace(name,
                                  '[thing={}]{}[/thing]'.format(gameid, name))
    return '[c]' + table + '[/c]'

def write_report(reportname, dataframe, headers):
    """Format the provided report and write it to the filesystem."""
    now = datetime.datetime.now().strftime('%Y-%m-%d')
    filename = '{} {}.txt'.format(now, reportname)
    with open(filename, 'w') as reportfile:
        reportfile.write(bgg_table(dataframe, headers))
        click.echo('wrote report to {}'.format(filename))

@cli.command()
@click.option('--guild', default=GUILD)
def top20(guild):
    """Create a report of the guild's top 20 games."""
    gcs = guild_collection_summary(guild)

    # filter out expansions
    is_not_an_expansion = (gcs['is_expansion'] == 0)
    gcs = gcs[is_not_an_expansion]

    # sort by adjusted average descending
    gcs = gcs.sort_values(by='guild_adj_average', ascending=False)

    # select rows and columns for output
    gcs = gcs.loc[:, ['name', 'guild_ratings', 'guild_adj_average']]
    gcs = gcs.head(20)
    gcs.insert(loc=0, column='row_num', value=np.arange(1, 1+len(gcs)))

    # output
    write_report('top20', gcs, ('#', 'Name', '# ratings', 'Rating'))

@cli.command()
@click.option('--guild', default=GUILD)
def bottom10(guild):
    """Create a report of the guild's bottom 10 games with 5+ ratings."""
    gcs = guild_collection_summary(guild)

    # filter out expansions
    is_not_an_expansion = (gcs['is_expansion'] == 0)
    sufficiently_rated = (gcs['guild_ratings'] >= 5)
    gcs = gcs[is_not_an_expansion & sufficiently_rated]

    # sort by adjusted average descending
    gcs = gcs.sort_values(by='guild_average', ascending=True)

    # select rows and columns for output
    gcs = gcs.loc[:, ['name', 'guild_ratings', 'guild_adj_average']]
    gcs = gcs.head(10)
    gcs.insert(loc=0, column='row_num', value=np.arange(1, 1+len(gcs)))

    # output
    write_report('bottom10', gcs, ('#', 'Name', '# ratings', 'Rating'))

@cli.command()
@click.option('--guild', default=GUILD)
def varied(guild):
    """Create a report of the guild's top 10 most varied games."""
    gcs = guild_collection_summary(guild)

    # filter out expansions
    is_not_an_expansion = (gcs['is_expansion'] == 0)
    sufficiently_rated = (gcs['guild_ratings'] >= 5)
    gcs = gcs[is_not_an_expansion & sufficiently_rated]

    # sort by adjusted average descending
    gcs = gcs.sort_values(by='guild_std', ascending=False)

    # select rows and columns for output
    gcs = gcs.loc[:, ['name', 'guild_ratings', 'guild_std']]
    gcs = gcs.head(10)
    gcs.insert(loc=0, column='row_num', value=np.arange(1, 1+len(gcs)))

    # output
    write_report('varied', gcs, ('#', 'Name', '# ratings', 'St.Dev'))

def vs_bgg(reportname, guild, ascending):
    """Create a report comparing the guild's ratings to BGG averages."""
    gcs = guild_collection_summary(guild)

    # filter out expansions
    is_not_an_expansion = (gcs['is_expansion'] == 0)
    sufficiently_rated = (gcs['guild_ratings'] >= 5)
    gcs = gcs[is_not_an_expansion & sufficiently_rated]

    # add comparison to BGG and sort on it
    gcs['vs_bgg'] = gcs['guild_average'] - gcs['bgg_average']
    gcs = gcs.sort_values(by='vs_bgg', ascending=ascending)

    # select rows and columns for output
    gcs = gcs.loc[:, ['name', 'guild_ratings', 'vs_bgg']]
    gcs = gcs.head(10)
    gcs.insert(loc=0, column='row_num', value=np.arange(1, 1+len(gcs)))

    # output
    write_report(reportname, gcs, ('#', 'Name', '# ratings', 'vs BGG'))

@cli.command()
@click.option('--guild', default=GUILD)
def morethanbgg(guild):
    """Create a report of the guild's top 10 liked-more-than-BGG games."""
    vs_bgg('morethanbgg', guild, False)

@cli.command()
@click.option('--guild', default=GUILD)
def lessthanbgg(guild):
    """Create a report of the guild's top 10 liked-less-than-BGG games."""
    vs_bgg('lessthanbgg', guild, True)

if __name__ == '__main__':
    cli()
