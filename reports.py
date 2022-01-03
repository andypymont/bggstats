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
SQL_SELECT_GAMES = "SELECT * FROM games"
USERNAME = "NormandyWept"


def base_data(db_path="bgg.db", username=USERNAME):
    """Create data frames of collection items, plays, and games for use in the various reports."""
    data = sqlite3.connect(db_path)
    collectiondata = pd.read_sql_query(SQL_SELECT_COLLECTION, data, params=(username,))
    gamedata = pd.read_sql_query(SQL_SELECT_GAMES, data)
    playdata = pd.read_sql_query(SQL_SELECT_PLAYS, data, params=(username,))
    playdata["date"] = pd.to_datetime(playdata["date"])
    return (playdata, gamedata, collectiondata)


def forty_char_name(name):
    """Make name 40 characters long exactly, padding with spaces."""
    if len(name) > 40:
        return name[:37] + "..."
    return "{:<40}".format(name[:40])


def add_gameid_link(forty_name, gameid):
    """Add [thing=X][/thing] tags around a game's name, creating a link on BGG forums."""
    link_text = forty_name.strip()
    extra_spaces = len(forty_name) - len(link_text)
    return "[thing={}]{}[/thing]".format(gameid, link_text) + (" " * extra_spaces)


def bgg_table(dataframe, title, headers):
    """Run tabulate on the given dataframe, then replace game names with geeklinks."""
    dataframe["name"] = dataframe["name"].map(forty_char_name)
    table = tabulate.tabulate(
        dataframe, headers=headers, showindex=False, floatfmt=".4f"
    )
    for (gameid, name) in dataframe["name"].iteritems():
        table = table.replace(name, add_gameid_link(name, gameid))
    return "[b][u]{}[/u][/b]\n[c]{}[/c]".format(title, table)


def hindex_data(plays, games, collection, date):
    """Calculate the items in the h-index for the given set of plays."""
    if date is not None:
        plays = plays[plays["date"] <= date]

    play_totals = plays.groupby("gameid").agg(
        plays=("quantity", "sum"), latest=("date", "max")
    )
    game_data = pd.merge(games, collection).loc[
        :, ["gameid", "name", "expansion", "rating"]
    ]
    game_data = game_data.set_index("gameid")

    hitems = pd.merge(play_totals, game_data, left_index=True, right_index=True)
    hitems = hitems[hitems["expansion"] == 0]
    hitems["sort_plays"] = -hitems["plays"]
    hitems = hitems.sort_values(by=["sort_plays", "latest"])
    hitems["h"] = np.arange(hitems.shape[0])
    return (
        hitems.loc[hitems["h"] < hitems["plays"], ["name", "plays"]],
        hitems.loc[
            (hitems["h"] >= hitems["plays"]) & (hitems["rating"] == 10),
            ["name", "plays"],
        ],
    )


def new_to_me_data(plays, games, collection, start, finish):
    """Calculate which games are new to me in the provided date range."""
    game_data = pd.merge(games, collection).loc[
        :, ["gameid", "name", "expansion", "rating"]
    ]
    game_data = game_data.set_index("gameid")

    plays["before"] = 0
    plays["during"] = 0
    plays.loc[plays["date"] < start, ["before"]] = plays["quantity"]
    plays.loc[(start <= plays["date"]) & (plays["date"] <= finish), ["during"]] = plays[
        "quantity"
    ]

    new = plays.groupby("gameid").agg(
        plays=("during", "sum"), previous=("before", "sum")
    )
    new = new[(new["previous"] == 0) & (new["plays"] > 0)]

    merged = pd.merge(new, game_data, left_index=True, right_index=True)
    merged = merged[merged["expansion"] == 0]
    return merged.loc[:, ["name", "rating"]].sort_values(by="rating", ascending=False)


def dust_data(plays, games, collection, start, finish):
    """
    Calculate games that returned from out of the dust (1+ yr no plays) during provided date
    range.
    """
    game_data = pd.merge(games, collection).loc[
        :, ["gameid", "name", "expansion", "rating"]
    ]
    game_data = game_data.set_index("gameid")

    plays["before"] = plays["date"].map(lambda date: date if date < start else np.NaN)
    plays["during"] = plays["date"].map(
        lambda date: date if start <= date <= finish else np.NaN
    )

    dusty = (
        plays.groupby("gameid")
        .agg(last_before=("before", "max"), first_during=("during", "min"))
        .dropna()
    )
    dusty["gap"] = dusty["first_during"] - dusty["last_before"]
    dusty = dusty[dusty["gap"] > datetime.timedelta(days=365)]

    return pd.merge(dusty, game_data, left_index=True, right_index=True)


def dateplays_data(plays):
    """
    Calculate the number of plays on each date in the year.
    """
    plays["month"] = plays["date"].dt.month
    plays["day"] = plays["date"].dt.day
    playtotals = plays.groupby(["month", "day"]).agg(plays=("quantity", "sum"))

    FIRST = datetime.datetime(2000, 1, 1)
    possible_days = [FIRST + datetime.timedelta(days=d) for d in range(366)]
    days = pd.DataFrame(possible_days, columns=["date"])
    days["month"] = days["date"].dt.month
    days["day"] = days["date"].dt.day

    dateplays = days.join(playtotals, on=["month", "day"], how="left")
    dateplays["plays"] = dateplays["plays"].fillna(0)
    return dateplays.loc[:, ["month", "day", "plays"]].sort_values(
        by=["plays", "month", "day"]
    )


def default_dates(start, finish):
    """Correct any missing dates by populating with appropriate defaults."""
    if (start is None) and (finish is None):
        # default to last month
        first = datetime.datetime.now().replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
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

    return start, finish


def annual_report_data(plays, games, collection, year):
    """
    Data for the annual report including both high-level stats and also plays by game and year
    published.
    """
    data = {"stats": {}}

    start = datetime.datetime(year, 1, 1)
    finish = datetime.datetime(year, 12, 31)

    hindex_relevant = plays[(start <= plays["date"]) & (plays["date"] <= finish)]

    plays["before"] = 0
    plays["during"] = 0
    plays.loc[plays["date"] < start, ["before"]] = plays["quantity"]
    plays.loc[(start <= plays["date"]) & (plays["date"] <= finish), ["during"]] = plays[
        "quantity"
    ]
    totals = plays.groupby("gameid").agg(
        plays=("during", "sum"), previous=("before", "sum")
    )
    totals = pd.merge(totals, games, left_index=True, right_on="gameid")
    totals = totals[(totals["expansion"] == 0) & (totals["plays"] > 0)]
    totals["sort_plays"] = -totals["plays"]
    totals = totals.sort_values(by=["sort_plays", "name"])

    new = totals[(totals["previous"] == 0) & (totals["plays"] > 0)]

    years = totals.groupby("year").agg(total_plays=("plays", "sum"))
    years = years[years["total_plays"] > 0]

    data["stats"]["Total Plays"] = totals["plays"].sum()
    data["stats"]["New to Me"] = new[new["expansion"] == 0].shape[0]
    data["stats"]["Nickels"] = totals[totals["plays"] >= 5].shape[0]
    data["stats"]["Dimes"] = totals[totals["plays"] >= 10].shape[0]
    data["stats"]["H-Index"] = hindex_data(
        hindex_relevant, games, collection, date=None
    )[0].shape[0]

    data["years"] = years
    data["games"] = totals

    return data


@click.group()
def cli():
    """Run user-level reports on the data in the local database."""


@cli.command("hindex")
@click.option("--date", default=None)
def hindex(date):
    """Run a report on h-index and games desired to be in it (10-rated)."""
    plays, games, collection = base_data()
    date = datetime.datetime.fromisoformat(date) if date else None

    hitems, top10items = hindex_data(plays, games, collection, date)
    hitems.insert(loc=0, column="row_num", value=np.arange(1, 1 + len(hitems)))
    top10items.insert(loc=0, column="row_num", value=np.arange(1, 1 + len(top10items)))

    filename = "{} {}.txt".format(
        (date or datetime.datetime.now()).strftime("%Y-%m-%d"), "hindex"
    )

    with open(filename, "w") as report_file:
        report_file.write(
            bgg_table(
                hitems, "H-Index: {}".format(hitems.shape[0]), ["#", "Name", "Plays"]
            )
        )
        report_file.write("\n\n")
        report_file.write(
            bgg_table(
                top10items,
                "Other favourite games - targets for the list",
                ["#", "Name", "Plays"],
            )
        )

    click.echo("Report was output to: {}".format(filename))


def new_to_me_row(gameid, name, rating):
    """Write a row of the new-to-me report, formatted for BGG forums."""
    ratingcolour = {
        1: "#FF3366",
        2: "#FF3366",
        3: "#FF66CC",
        4: "#FF66CC",
        5: "#9999FF",
        6: "#9999FF",
        7: "#66FF99",
        8: "#66FF99",
        9: "#00CC00",
        10: "#00CC00",
    }
    return "\n\n[b][BGCOLOR={}] {} [/BGCOLOR][/b] [thing={}]{}[/thing]".format(
        ratingcolour.get(rating, "#A3A3A3"), rating or "N/A", gameid, name
    )


def dateplays_row(month, day, plays):
    return "{:<16} {}".format(
        datetime.datetime(2000, month, day).strftime("%d %B"), plays
    )


@cli.command("newtome")
@click.option("--start", default=None)
@click.option("--finish", default=None)
def new_to_me(start, finish):
    """Create a report of new-to-me items in the given date range."""
    start, finish = default_dates(start, finish)
    plays, games, collection = base_data()
    new = new_to_me_data(plays, games, collection, start, finish)

    filename = "{} {}.txt".format(
        finish or datetime.datetime.now().strftime("%Y-%m-%d"), "newtome"
    )

    with open(filename, "w") as report_file:
        report_file.write("[b][u]NEW TO ME: {} - {}[/u][/b]\n\n".format(start, finish))
        for gameid, row in new.iterrows():
            report_file.write(new_to_me_row(gameid, row["name"], int(row["rating"])))

    click.echo("Report was output to: {}".format(filename))


@cli.command("dust")
@click.option("--start", default=None)
@click.option("--finish", default=None)
def out_of_the_dust(start, finish):
    """Create a report of out-of-the-dust items in the given date range."""
    start, finish = default_dates(start, finish)
    start = datetime.datetime.fromisoformat(start) if isinstance(start, str) else start
    finish = (
        datetime.datetime.fromisoformat(finish) if isinstance(finish, str) else finish
    )
    plays, games, collection = base_data()
    dusty = dust_data(plays, games, collection, start, finish)

    filename = "{} {}.txt".format(
        (finish or datetime.datetime.now()).strftime("%Y-%m-%d"), "dust"
    )

    with open(filename, "w") as report_file:
        report_file.write(
            "[b][u]OUT OF THE DUST: {} - {}[/u][/b]".format(start, finish)
        )
        for gameid, row in dusty.iterrows():
            report_file.write(new_to_me_row(gameid, row["name"], int(row["rating"])))
            years, days = row["gap"].days // 365, row["gap"].days % 365
            report_file.write(
                "\n[i]{} year{}, {} days[/i]".format(
                    years, "s" if years > 1 else "", days
                )
            )

    click.echo("Report was output to: {}".format(filename))


@cli.command("annual")
@click.option("--year", default=None)
def annual_report(year):
    """Create annual report of stats and games/year-published totals."""
    report_year = int(year) or datetime.datetime.now().year
    plays, games, collection = base_data()
    data = annual_report_data(plays, games, collection, report_year)

    filename = "{} {}.txt".format(report_year, "annual")

    with open(filename, "w") as report_file:
        report_file.write("[b]Stats[/b]\n")
        for stat, value in data.get("stats", {}).items():
            report_file.write("{}: {}\n".format(stat, value))

        report_file.write("\n[b]Total Plays by Year of Release[/b]")
        for (row_num, (year_pub, plays)) in enumerate(
            data.get("years", pd.DataFrame()).iterrows()
        ):
            report_file.write(
                "\n{}{:>4}x {}".format(
                    "[c]" if row_num == 0 else "", plays["total_plays"], int(year_pub)
                )
            )

        report_file.write("[/c]\n\n[b]Plays by Game[/b]")
        for (row_num, (_, row)) in enumerate(
            data.get("games", pd.DataFrame()).iterrows()
        ):
            report_file.write(
                "\n{}{:>4}x [thing={}]{}[/thing]".format(
                    "[c]" if row_num == 0 else "",
                    row["plays"],
                    row["gameid"],
                    row["name"],
                )
            )
        report_file.write("[/c]")

    click.echo("Report was output to: {}".format(filename))


@cli.command("dateplays")
def dateplays():
    """Create a report of the dates with the fewest plays."""
    plays, _, _ = base_data()
    data = dateplays_data(plays)

    filename = "{} {}.txt".format(
        datetime.datetime.now().strftime("%Y-%m-%d"), "dateplays"
    )

    with open(filename, "w") as report_file:
        report_file.write("[b]Dates with Fewest Plays[/b]\n")
        for _, row in data.iterrows():
            report_file.write(
                "\n"
                + dateplays_row(int(row["month"]), int(row["day"]), int(row["plays"]))
            )

    click.echo("Report was output to: {}".format(filename))


if __name__ == "__main__":
    cli()
