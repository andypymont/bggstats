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

def top50line(pos, line):
    """Format a line in the top-50 report for output."""
    return '{:>3} {:<35}  {:>3}  {:.2f}  {:.2f}  {:.2f}\n'.format(
        pos,
        line['name'],
        line['guild_ratings'],
        line['guild_average'],
        line['guild_adj_average'],
        line['guild_std']
    )

def bgg_table(dataframe, headers):
    """Run tabulate on the given dataframe, then replace game names with geeklinks."""
    table = tabulate(dataframe, headers=headers, showindex=False)
    if 'name' in dataframe.columns:
        for (gameid, name) in dataframe['name'].iteritems():
            table = table.replace(
                name,
                '[thing={}]{}[/thing]'.format(gameid, name)
            )
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
    gcs = gcs.round(4).head(20)
    gcs.insert(loc=0, column='row_num', value=np.arange(1, 1+len(gcs)))

    # output
    write_report('top20', gcs, ('#', 'Name', '# ratings', 'Rating'))

# TOP 50
# BOTTOM 10
# MOST VARIED
# MOST RATED
# LIKE MORE THAN BGG
# LIKE LESS THAN BGG

if __name__ == '__main__':
    cli()