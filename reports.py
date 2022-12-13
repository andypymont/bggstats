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
SQL_SELECT_GAME_HINDEX = "SELECT * FROM game_hindex"
USERNAME = "NormandyWept"
TTY_START_YEAR = 1985


def base_data(db_path="bgg.db", username=USERNAME):
    """Create data frames of collection items, plays, and games for use in the various reports."""
    data = sqlite3.connect(db_path)
    collectiondata = pd.read_sql_query(SQL_SELECT_COLLECTION, data, params=(username,))
    gamedata = pd.read_sql_query(SQL_SELECT_GAMES, data)
    playdata = pd.read_sql_query(SQL_SELECT_PLAYS, data, params=(username,))
    playdata["date"] = pd.to_datetime(playdata["date"])
    return (playdata, gamedata, collectiondata)

def game_hindex_data(db_path="bgg.db"):
    """Cretae a dataframe of the game h-index data."""
    data = sqlite3.connect(db_path)
    return pd.read_sql_query(SQL_SELECT_GAME_HINDEX, data)

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

    first = datetime.datetime(2000, 1, 1)
    possible_days = [first + datetime.timedelta(days=d) for d in range(366)]
    days = pd.DataFrame(possible_days, columns=["date"])
    days["month"] = days["date"].dt.month
    days["day"] = days["date"].dt.day

    date_plays = days.join(playtotals, on=["month", "day"], how="left")
    date_plays["plays"] = date_plays["plays"].fillna(0)
    return date_plays.loc[:, ["month", "day", "plays"]].sort_values(
        by=["plays", "month", "day"]
    )


def through_the_years_data(plays, games, year):
    """
    Data for the 'Through the Years' challenge: the first game of each publishing year played in
    the given year.
    """
    play_data = (
        pd.merge(
            plays[plays["date"].dt.year == year],
            games[games["expansion"] == 0],
            left_on="gameid",
            right_on="gameid",
            how="left",
        )
        .groupby(["year", "date"])
        .agg(gameid=("gameid", "first"), name=("name", "first"))
        .reset_index()
        .sort_values(by=["date"])
    )
    first_by_year = play_data.groupby("year").agg(
        date=("date", "first"), gameid=("gameid", "first"), name=("name", "first")
    )
    years = pd.DataFrame(range(TTY_START_YEAR, year + 1), columns=["year"])
    data = pd.merge(years, first_by_year, how="left", left_on="year", right_on="year")
    data["name"] = data["name"].fillna("")
    return data


def archaeologist_data(plays, games, year):
    """
    Data for the 'Archaeologist' challenge: games not near the top of the BGG rankings.
    """
    play_data = (
        pd.merge(
            plays[plays["date"].dt.year == year],
            games[games["expansion"] == 0],
            left_on="gameid",
            right_on="gameid",
            how="left",
        )
        .groupby(["rank"])
        .agg(date=("date", "first"), gameid=("gameid", "first"), name=("name", "first"))
        .reset_index()
        .sort_values(by=["rank"], ascending=False)
    )

    limit = max(10_000, int(play_data["rank"].max() // 1000 + 1) * 1000)
    bins = tuple(range(1000, limit + 1000, 1000))
    play_data["rank_bin"] = pd.cut(
        play_data["rank"], bins, labels=[f"{bin-999}-{bin}" for bin in bins[1:]]
    )

    data = play_data.groupby("rank_bin").agg(
        rank=("rank", "first"),
        date=("date", "first"),
        gameid=("gameid", "first"),
        name=("name", "first"),
    )
    data["name"] = data["name"].fillna("")
    data["rank"] = data["rank"].fillna(-1)
    return data.reset_index()


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
    """Write a row of the date-plays report."""
    return "{:<16} {}".format(
        datetime.datetime(2000, month, day).strftime("%d %B"), plays
    )


def through_the_years_row(year, date, gameid, name):
    """Write a row of the through-the-years report."""
    return "\n{} {} {}".format(
        str(year),
        "          " if date is pd.NaT else date.strftime("%Y-%m-%d"),
        "" if np.isnan(gameid) else add_gameid_link(forty_char_name(name), int(gameid)),
    )


def archaeologist_row(rank_bin, rank, date, gameid, name):
    """Write a row of the archaeologit report."""
    return "\n{} {} {} {}".format(
        rank_bin.rjust(11),
        "     " if rank == -1 else str(rank).rjust(5),
        "          " if date is pd.NaT else date.strftime("%Y-%m-%d"),
        "" if np.isnan(gameid) else add_gameid_link(forty_char_name(name), int(gameid)),
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


@cli.command("throughtheyears")
@click.option("--year", default=None)
def through_the_years(year):
    """Report of games played (by publishing year) in the given year."""
    report_year = int(year) if year else datetime.datetime.now().year

    plays, games, _ = base_data()
    data = through_the_years_data(plays, games, report_year)

    filename = "{} {}.txt".format(report_year, "throughtheyears")

    with open(filename, "w") as report_file:
        report_file.write("[b]THROUGH THE YEARS - {}[/b]\n".format(report_year))
        for row_num, row in data.iterrows():
            report_file.write(
                "{}{}".format(
                    "[c]" if row_num == 0 else "",
                    through_the_years_row(
                        int(row["year"]), row["date"], row["gameid"], row["name"]
                    ),
                )
            )
        report_file.write("[/c]\n")

    click.echo("Report was output to: {}".format(filename))


@cli.command("archaeologist")
@click.option("--year", default=None)
def archaeologist(year):
    """Report of games played (by BGG ranking, binned in 1000s) in the given year."""
    report_year = int(year) if year else datetime.datetime.now().year

    plays, games, _ = base_data()
    data = archaeologist_data(plays, games, report_year)

    filename = "{} {}.txt".format(report_year, "archaeologist")

    with open(filename, "w") as report_file:
        report_file.write("[b]ARCHAEOLOGIST CHALLENGE - {}[/b]\n".format(report_year))
        for row_num, row in data.iterrows():
            report_file.write(
                "{}{}".format(
                    "[c]" if row_num == 0 else "",
                    archaeologist_row(
                        row["rank_bin"],
                        int(row["rank"]),
                        row["date"],
                        row["gameid"],
                        row["name"],
                    ),
                )
            )

        report_file.write("[/c]\n")

    click.echo("Report was output to: {}".format(filename))


if __name__ == "__main__":
    cli()
