"""
Fetch data from BoardGameGeek for data analysis.
"""

import requests
import sqlite3
from datetime import datetime
from itertools import count, islice
import click
from bggthread import BGGClientWithThreadSupport
from bs4 import BeautifulSoup

GUILD = 901
USERNAME = "NormandyWept"

SQL_SCHEMA_GAMES = """CREATE TABLE IF NOT EXISTS games (
        gameid INTEGER PRIMARY KEY,
        name TEXT,
        expansion INTEGER,
        min_players INTEGER,
        max_players INTEGER,
        playing_time INTEGER,
        rank INTEGER,
        rating_average REAL,
        weight REAL,
        year INTEGER
    )"""
SQL_SELECT_GAMES = "SELECT * FROM games"
SQL_UPDATE_GAMES = """INSERT OR REPLACE
        INTO games (gameid, name, expansion, min_players, max_players, playing_time, rank, rating_average, weight, year)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

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
SQL_DELETE_COLLECTIONITEMS = (
    "DELETE FROM collectionitems WHERE username = ? AND gameid = ?"
)
SQL_UPDATE_COLLECTIONITEMS = """INSERT OR REPLACE
        INTO collectionitems (username, gameid, owned, rating)
        VALUES (?, ?, ?, ?)"""

SQL_SCHEMA_PLAYS = """CREATE TABLE IF NOT EXISTS plays (
        playid INTEGER PRIMARY KEY,
        username TEXT,
        gameid INTEGER,
        date TEXT,
        quantity INTEGER
    )"""
SQL_SELECT_PLAYS = "SELECT * FROM plays WHERE username = ?"
SQL_SELECT_PLAYS_ALL = "SELECT * FROM plays"
SQL_UPDATE_PLAYS = """INSERT OR REPLACE
    INTO plays (playid, username, gameid, date, quantity)
    VALUES (?, ?, ?, ?, ?)"""
SQL_SCHEMA_GAME_HINDEX = """CREATE TABLE IF NOT EXISTS game_hindex (
    gameid INTEGER PRIMARY KEY,
    hindex INTEGER,
    most_plays INTEGER,
    top_ten_plays INTEGER
)"""
SQL_SELECT_GAME_HINDEX = "SELECT * FROM game_hindex"
SQL_UPDATE_GAME_HINDEX = """INSERT OR REPLACE
    INTO game_hindex (gameid, hindex, most_plays, top_ten_plays)
    VALUES (?, ?, ?, ?)"""

class Database:
    """Abstract the SQL connection for use in functions in this module."""

    def __init__(self):
        self.data = sqlite3.connect("bgg.db")
        self._initdb()

    def _initdb(self):
        cursor = self.data.cursor()
        cursor.execute(SQL_SCHEMA_GAMES)
        cursor.execute(SQL_SCHEMA_GUILDMEMBERS)
        cursor.execute(SQL_SCHEMA_COLLECTIONITEMS)
        cursor.execute(SQL_SCHEMA_PLAYS)
        cursor.execute(SQL_SCHEMA_GAME_HINDEX)
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
                cursor.executemany(SQL_DELETE_COLLECTIONITEMS, deletions)
            self.data.commit()

    def update_games(self, updates):
        """Update games per the provided list."""
        if updates:
            cursor = self.data.cursor()
            cursor.executemany(SQL_UPDATE_GAMES, updates)
            self.data.commit()

    def get_latest_play_date(self, username):
        """Identify the latest date with plays already in the database for the given user."""
        cursor = self.data.cursor()
        cursor.execute(SQL_SELECT_PLAYS, (username,))
        rows = cursor.fetchall()
        return datetime.fromisoformat(
            max(date for (_, _, _, date, _) in rows) if rows else "1990-01-01"
        )

    def update_plays(self, updates):
        """Update plays per the provided list."""
        if updates:
            cursor = self.data.cursor()
            cursor.executemany(SQL_UPDATE_PLAYS, updates)
            self.data.commit()

    def get_all_play_gameids(self):
        """Return set of all gameids in users' plays in the database."""
        cursor = self.data.cursor()
        cursor.execute(SQL_SELECT_PLAYS_ALL)
        return set(gameid for (_, _, gameid, _, _) in cursor.fetchall())

    def get_all_gameids(self):
        """Return all known gameids from both collections and plays in the database."""
        return self.get_all_collection_gameids() | self.get_all_play_gameids()

    def get_known_gameids(self):
        """Return all gameids from the games table."""
        cursor = self.data.cursor()
        cursor.execute(SQL_SELECT_GAMES)
        return set(gameid for gameid, *_ in cursor.fetchall())

    def get_missing_gameids(self):
        """
        Return all missing gameids, i.e. those that appear in a play or collection item but aren't
        in the games table.
        """
        return self.get_all_play_gameids().difference(self.get_known_gameids())
    
    def update_game_hindices(self, updates):
        """Update game hindex info per the provided list."""
        if updates:
            cursor = self.data.cursor()
            cursor.executemany(SQL_UPDATE_GAME_HINDEX, updates)
            self.data.commit()


bgg = BGGClientWithThreadSupport()
db = Database()


@click.group()
def cli():
    """Fetch data from BoardGameGeek for data analysis."""


@cli.command()
@click.option("--guild", default=GUILD)
@click.option("--thread", default=None)
def guildmembers(guild, thread):
    """Fetch and update database with latest list of guild members"""
    additions = set()
    deletions = db.get_guild_members(guild)

    members = set(bgg.guild(guild).members).union(
        set(article.username for article in bgg.thread(thread).articles)
        if thread
        else set()
    )

    for member in members:
        if member in deletions:
            deletions.remove(member)
        else:
            additions.add(member)

    if len(additions) + len(deletions) > 0:
        click.echo(
            "Adding {} and deleting {} members from database".format(
                len(additions), len(deletions)
            )
        )
        db.insert_and_delete_guild_members(
            [(guild, addition) for addition in additions],
            [(guild, deletion) for deletion in deletions],
        )


@cli.command()
@click.option("--guild", default=GUILD)
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
        updates.append(
            (
                username,
                game.id,
                1 if game.owned else 0,
                game.rating if game.rating else None,
            )
        )
    if len(updates) + len(deletions) > 0:
        click.echo(
            "Updating {} and deleting {} items from collection of {}".format(
                len(updates), len(deletions), username
            )
        )
        db.update_and_delete_collection_items(
            updates, [(username, gameid) for gameid in deletions]
        )


@cli.command("collection")
@click.option("--username", default=USERNAME)
def _collection(username):
    """Fetch and update the database with a user's collection."""
    collection(username)


def partition(sequence, size):
    """Split a large sequence into smaller sequences of the given size."""
    for i in range(0, len(sequence), size):
        yield list(islice(sequence, i, i + size))


@cli.command()
@click.option("--missing-only/--all", default=False)
def games(missing_only):
    """Fetch and update the database for all known games."""
    gameids = list(db.get_missing_gameids() if missing_only else db.get_all_gameids())
    click.echo("{} total games to update".format(len(gameids)))
    for i, chunk in enumerate(partition(gameids, 20)):
        click.echo("-- updating set {} containing {} games".format(i + 1, len(chunk)))
        rows = [
            (
                game.id,
                game.name,
                1 if game.expansion else 0,
                game.min_players if game.min_players else None,
                game.max_players if game.max_players else None,
                game.playing_time if game.playing_time else None,
                game.ranks[0].value if game.ranks else None,
                game.rating_average if game.rating_average else None,
                game.rating_average_weight if game.rating_average_weight else None,
                game.year if game.year else None,
            )
            for game in bgg.game_list(chunk)
        ]
        db.update_games(rows)


@cli.command()
@click.option("--username", default=USERNAME)
def plays(username):
    """Fetch and update the database with a user's plays."""
    latest_date = db.get_latest_play_date(username)
    playlist = [
        (play.id, username, play.game_id, play.date, play.quantity)
        for play in bgg.plays(name=username, min_date=latest_date)
    ]
    if playlist:
        click.echo("Recording {} plays".format(sum(p[4] for p in playlist)))
        db.update_plays(playlist)


def game_hindex_info(gameid):
    """Scrape BGG for the game hindex info for the given game id."""
    click.echo(f"----- fetching hindex data for game id: {gameid}")
    plays = []
    finished = False
    prev = 0
    for page_no in count(1):
        page = requests.get(f"https://boardgamegeek.com/playstats/thing/{gameid}/page/{page_no}")
        soup = BeautifulSoup(page.content, 'html.parser')
        for table_row in soup.find_all('td', class_='lf'):
            play_count = int(table_row.find('a').text)
            if play_count < len(plays):
                finished = True
                break
            plays.append(play_count)
        if finished or (len(plays) == prev):
            break
        prev = len(plays)

    if len(plays) == 0:
        return gameid, 0, 0, 0
    
    return gameid, len(plays), max(plays), (min(plays) if len(plays) < 10 else plays[9])

@cli.command()
def ghi():
    """Fetch and update the database with the game hindex information for all played games."""
    gameids = db.get_all_play_gameids()
    click.echo("{} total games to update".format(len(gameids)))
    for i, chunk in enumerate(partition(gameids, 100)):
        click.echo("-- updating set {} containing {} games".format(i + 1, len(chunk)))
        rows = [game_hindex_info(gameid) for gameid in chunk]
        db.update_game_hindices(rows)

if __name__ == "__main__":
    cli()
