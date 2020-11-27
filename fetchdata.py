"""
Fetch data from BoardGameGeek for data analysis.
"""

import sqlite3
from itertools import islice
from bggthread import BGGClientWithThreadSupport
import click

GUILD = 901
USERNAME = 'NormandyWept'

SQL_SCHEMA_GAMES = """CREATE TABLE IF NOT EXISTS games (
        gameid INTEGER PRIMARY KEY,
        name TEXT,
        expansion INTEGER,
        min_players INTEGER,
        max_players INTEGER,
        playing_time INTEGER,
        rating_average REAL,
        weight REAL,
        year INTEGER
    )"""
SQL_UPDATE_GAMES = """INSERT OR REPLACE
        INTO games (gameid, name, expansion, min_players, max_players, playing_time, rating_average, weight, year)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""

SQL_SCHEMA_GUILDMEMBERS = """CREATE TABLE IF NOT EXISTS guildmembers (
        guildid INTEGER,
        username TEXT
    )"""
SQL_SELECT_GUILDMEMBERS = "SELECT * FROM guildmembers WHERE guildid = ?"
SQL_DELETE_GUILDMEMBERS = "DELETE FROM guildmembers WHERE guildid = ? AND USERNAME = ?"
SQL_INSERT_GUILDMEMBERS = "INSERT INTO guildmembers (guildid, username) VALUES (?, ?)"

SQL_SCHEMA_COLLECTIONITEMS = """CREATE TABLE IF NOT EXISTS collectionitems (
        username TEXT,
        gameid INTEGER,
        owned INTEGER,
        rating INTEGER,
        PRIMARY KEY ( username, gameid )
    )"""
SQL_SELECT_COLLECTIONITEMS = "SELECT * FROM collectionitems WHERE username = ?"
SQL_SELECT_COLLECTIONITEMS_ALL = "SELECT * FROM collectionitems"
SQL_DELETE_COLLECTIONITEMS = "DELETE FROM collectionitems WHERE username = ? AND gameid = ?"
SQL_UPDATE_COLLECTIONITEMS = """INSERT OR REPLACE
        INTO collectionitems (username, gameid, owned, rating)
        VALUES (?, ?, ?, ?)"""

class Database():
    """Abstract the SQL connection for use in functions in this module."""

    def __init__(self):
        self.data = sqlite3.connect('bgg.db')
        self._initdb()

    def _initdb(self):
        cursor = self.data.cursor()
        cursor.execute(SQL_SCHEMA_GAMES)
        cursor.execute(SQL_SCHEMA_GUILDMEMBERS)
        cursor.execute(SQL_SCHEMA_COLLECTIONITEMS)
        self.data.commit()

    def get_guild_members(self, guildid):
        """Return set of current guild members from the database."""
        cursor = self.data.cursor()
        cursor.execute(SQL_SELECT_GUILDMEMBERS, (guildid,))
        return set(username for (guidid, username) in cursor.fetchall())

    def insert_and_delete_guild_members(self, additions, deletions):
        """Insert and delete guild members per the provided lists."""
        if additions or deletions:
            cursor = self.data.cursor()
            if additions:
                cursor.executemany(SQL_INSERT_GUILDMEMBERS, additions)
            if deletions:
                cursor.executemany(SQL_DELETE_GUILDMEMBERS, deletions)
            self.data.commit()

    def get_collection_gameids(self, username):
        """Return set of gameids in the given user's collection in the database."""
        cursor = self.data.cursor()
        cursor.execute(SQL_SELECT_COLLECTIONITEMS, (username,))
        return set(gameid for (_, gameid, _, _) in cursor.fetchall())

    def get_all_collection_gameids(self):
        """Return set of all gameids in users' collections in the database."""
        cursor = self.data.cursor()
        cursor.execute(SQL_SELECT_COLLECTIONITEMS_ALL)
        return set(gameid for (_, gameid, _, _) in cursor.fetchall())

    def update_and_delete_collection_items(self, updates, deletions):
        """Update and delete collection items per the provided lists."""
        if updates or deletions:
            cursor = self.data.cursor()
            if updates:
                cursor.executemany(SQL_UPDATE_COLLECTIONITEMS, updates)
            if deletions:
                cursor.exeuctemany(SQL_DELETE_COLLECTIONITEMS, deletions)
            self.data.commit()

    def update_games(self, updates):
        """Update games per the provided list."""
        if updates:
            cursor = self.data.cursor()
            cursor.executemany(SQL_UPDATE_GAMES, updates)
            self.data.commit()

bgg = BGGClientWithThreadSupport()
db = Database()

@click.group()
def cli():
    """Fetch data from BoardGameGeek for data analysis."""

@cli.command()
@click.option('--guild', default=GUILD)
@click.option('--thread', default=None)
def guildmembers(guild, thread):
    """Fetch and update database with latest list of guild members"""
    additions = set()
    deletions = db.get_guild_members(guild)

    members = set(bgg.guild(guild).members).union(
        set(article.username for article in bgg.thread(thread).articles)
        if thread else set()
    )

    for member in members:
        if member in deletions:
            deletions.remove(member)
        else:
            additions.add(member)

    if len(additions) + len(deletions) > 0:
        click.echo('Adding {} and deleting {} members from database'.format(
            len(additions),
            len(deletions)
        ))
        db.insert_and_delete_guild_members(
            [(guild, addition) for addition in additions],
            [(guild, deletion) for deletion in deletions],
        )

@cli.command()
@click.option('--guild', default=GUILD)
def guildcollections(guild):
    """Fetch and update the database with all guild members' collections"""
    members = db.get_guild_members(guild)
    for member in members:
        collection(member)

def collection(username):
    """Fetch and update the database with a user's collection."""
    updates = list()
    deletions = db.get_collection_gameids(username)
    for game in bgg.collection(user_name=username):
        if game.id in deletions:
            deletions.remove(game.id)
        updates.append((
            username,
            game.id,
            1 if game.owned else 0,
            game.rating if game.rating else None
        ))
    if len(updates) + len(deletions) > 0:
        click.echo('Updating {} and deleting {} items from collection of {}'.format(
            len(updates),
            len(deletions),
            username
        ))
        db.update_and_delete_collection_items(
            updates,
            [(username, gameid) for gameid in deletions]
        )

@cli.command('collection')
@click.option('--username', default=USERNAME)
def _collection(username):
    """Fetch and update the database with a user's collection."""
    collection(username)

def partition(sequence, size):
    """Split a large sequence into smaller sequences of the given size."""
    for i in range(0, len(sequence), size):
        yield list(islice(sequence, i, i + size))

@cli.command()
def games():
    """Fetch and update the database for all known games."""
    gameids = list(db.get_all_collection_gameids())
    click.echo('{} total games to update'.format(len(gameids)))
    for i, chunk in enumerate(partition(gameids, 500)):
        click.echo('-- updating set {} containing {} games'.format(i+1, len(chunk)))
        rows = [(
            game.id,
            game.name,
            1 if game.expansion else 0,
            game.min_players if game.min_players else None,
            game.max_players if game.max_players else None,
            game.playing_time if game.playing_time else None,
            game.rating_average if game.rating_average else None,
            game.rating_average_weight if game.rating_average_weight else None,
            game.year if game.year else None,
        ) for game in bgg.game_list(chunk)]
        db.update_games(rows)

if __name__ == '__main__':
    cli()
