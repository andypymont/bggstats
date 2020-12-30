"""
Run guild-level reports on the data in the local database.
"""

from collections import OrderedDict
import datetime
import sqlite3
import numpy as np
import pandas as pd
import click
import tabulate

tabulate.PRESERVE_WHITESPACE = True
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
    gcs['vs_bgg'] = gcs['guild_average'] - gcs['bgg_average']
    return gcs

def forty_char_name(name):
    """Make name 30 characters long exactly, padding with spaces."""
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

def filter_gcs(dataframe, expansions, min_ratings):
    """Filter the provided guild collection summary based on other arguments."""
    expansion_status = True if expansions == 'all' else (dataframe['is_expansion'] == expansions)
    sufficient_ratings = (dataframe['guild_ratings'] >= min_ratings)
    return dataframe[expansion_status & sufficient_ratings]

def run_report(dataframe, report_settings):
    """Return a function which will produce a report from a provided dataframe."""
    sort_by = report_settings.get('sort_by', 'guild_adj_average')
    sort_asc = report_settings.get('sort_ascending', False)

    dataframe = filter_gcs(dataframe,
                           report_settings.get('expansions', 'all'),
                           report_settings.get('min_ratings', 0))
    dataframe = dataframe.sort_values(by=sort_by, ascending=sort_asc)
    dataframe = dataframe.loc[:, ['name', 'guild_ratings', sort_by]]
    dataframe = dataframe.head(report_settings.get('rows', 10))
    dataframe.insert(loc=0, column='row_num', value=np.arange(1, 1+len(dataframe)))
    return bgg_table(
        dataframe,
        report_settings.get('title', 'Report'),
        ['#', 'Name', '# ratings', report_settings.get('col_name', 'Rating')]
    )

reports = OrderedDict([
    (
        'top20',
        {
            'title': 'Top 20 Games',
            'expansions': 0,
            'rows': 20
        }
    ),
    (
        'top10expansions',
        {
            'title': 'Top 10 Expansions',
            'expansions': 1
        }
    ),
    (
        'bottom10',
        {
            'title': 'Bottom 10 Games',
            'expansions': 0,
            'sort_by': 'guild_average',
            'sort_ascending': True,
            'min_ratings': 5
        }
    ),
    (
        'varied',
        {
            'title': 'Most Varied Ratings',
            'expansions': 0,
            'sort_by': 'guild_std',
            'min_ratings': 5,
            'col_name': 'St.Dev'
        }
    ),
    (
        'morethanbgg',
        {
            'title':'Games Liked More than BoardGameGeek',
            'expansions': 0,
            'sort_by': 'vs_bgg',
            'min_ratings': 5,
            'col_name': 'vs BGG'
        }
    ),
    (
        'lessthanbgg',
        {
            'title': 'Games Liked Less than BoardGameGeek',
            'expansions': 0,
            'sort_by':
            'vs_bgg',
            'sort_ascending': True,
            'min_ratings': 5,
            'col_name': 'vs BGG'
        }
    )
])

@cli.command()
def listall():
    """List all known reports."""
    all_reports = [
        (name, settings.get('title', 'Untitled Report')) for (name, settings) in reports.items()
    ]
    click.echo(tabulate.tabulate(all_reports, headers=['Name', 'Title']))

@click.option('--report_name', default='all')
@cli.command()
def run(report_name):
    """Run a report and output to a text file."""
    now = datetime.datetime.now().strftime('%Y-%m-%d')
    filename = '{} {}.txt'.format(now, report_name)

    outputs = [settings for (rep, settings) in reports.items() if report_name in ('all', rep)]
    if not outputs:
        click.echo('No report found named "{}"'.format(report_name))
        return

    with open(filename, 'w') as report_file:
        gcs = guild_collection_summary()
        for i, report in enumerate(outputs):
            if i > 0:
                report_file.write('\n\n\n')
            report_file.write(run_report(gcs, report))
        click.echo('Report was output to: {}'.format(filename))

if __name__ == '__main__':
    cli()
