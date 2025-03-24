#!/usr/bin/env python3
import collections
import datetime
import sqlite3
import sys

months = {
    'January':   '01',
    'February':  '02',
    'March':     '03',
    'April':     '04',
    'May':       '05',
    'June':      '06',
    'July':      '07',
    'August':    '08',
    'September': '09',
    'October':   '10',
    'November':  '11',
    'December':  '12',
}

# I should probably just redownload the data I have that uses old names
currency_overrides = {
    "U.K. Pound Sterling": "U.K. pound",
    "U.S. Dollar": "U.S. dollar",
}

class CurrencyTSV:
    def __init__(self, filename):
        self.file = open(filename)
        self.columns = []
        self.q = collections.deque()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.file.close()

    def __iter__(self):
        return self

    def __next__(self):
        while not self.q:
            for line in self.file:
                bits = line.strip('\n').split('\t')

                if len(bits) == 1:
                    continue

                if bits[0] == 'Currency':
                    self.columns = bits[1:]
                    continue

                for date, value in zip(self.columns, bits[1:]):
                    month, day, year = date.split(' ')
                    day = day.rstrip(',')
                    date = "{}-{}-{}".format(year, months[month], day)
                    if value == 'NA':
                        continue
                    value = value.replace(',', '')
                    currency = currency_overrides.get(bits[0], bits[0])
                    self.q.append((date, currency, float(value)))
                break
            else:
                raise StopIteration

        return self.q.pop()


def update(db, tsvs):
    cursor = db.cursor()
    for tsv in tsvs:
        with CurrencyTSV(tsv) as tsv:
            cursor.executemany('INSERT INTO exchange_rates \
                VALUES (?,?,?)', tsv)


def create_cmd():
    dbname = sys.argv[2]
    with sqlite3.connect(dbname) as db:
        db.execute("CREATE TABLE IF NOT EXISTS \
            exchange_rates (date text, currency text, value real)")
        update(db, sys.argv[3:])


def update_cmd():
    dbname = sys.argv[2]
    with sqlite3.connect(dbname) as db:
        update(db, sys.argv[3:])


def date_of_string(s):
    return datetime.datetime.strptime(s, '%Y-%m-%d')


def weekdays(start: datetime.date, stop: datetime.date):
    for ordinal in range(start.toordinal(), stop.toordinal()):
        current = datetime.date.fromordinal(ordinal)
        if current.weekday() < 5:
            yield current


def get_rate_or_exit(db: sqlite3.Connection, date: str, from_: str, to: str) -> float:
    cur = db.cursor()
    results: dict[str, float] = {}

    for currency in [from_, to]:
        cur.execute(
            "SELECT value FROM exchange_rates WHERE date = ? AND currency = ?",
            (date, currency),
        )
        rows = cur.fetchall()
        if not rows:
            sys.stderr.write(f"{currency} does not have a value on {date}\n")
        if len(rows) > 1:
            sys.stderr.write(f"{currency} has ambiguous value on {date}\n")
        (results[currency],) = rows[0]

    # don't exit until we've seen all errors
    for currency in [from_, to]:
        if currency not in results:
            sys.exit(1)

    return results[to] / results[from_]

def convert_cmd() -> None:
    try:
        dbname, date, from_, to, amount_str = sys.argv[2:]
        amount = float(amount_str)
    except ValueError:
        sys.stderr.write("Usage:\n\
            {0} convert DBNAME DATE FROM TO AMOUNT\n".format(sys.argv[0]))
        sys.exit(1)

    with sqlite3.connect(dbname) as db:
        rate = get_rate_or_exit(db=db, date=date, from_=from_, to=to)

    print(amount * rate)


def missing_dates_cmd():
    dbname = sys.argv[2]
    start = None
    stop  = None
    try:
        start = date_of_string(sys.argv[3])
        stop  = date_of_string(sys.argv[4])
    except IndexError:
        pass

    with sqlite3.connect(dbname) as db:
        query = "SELECT DISTINCT date FROM exchange_rates ORDER BY date ASC"
        dates = set(r[0] for r in db.execute(query))

    if not dates:
        sys.stderr.write("No dates!\n")
        sys.exit(1)
    if start is None:
        start = date_of_string(min(dates))
    if stop is None:
        stop = date_of_string(max(dates))
    for weekday in weekdays(start, stop):
        if weekday.isoformat() not in dates:
            print(weekday)


def list_currencies_cmd() -> None:
    dbname = sys.argv[2]
    with sqlite3.connect(dbname) as db:
        query = "SELECT DISTINCT currency FROM exchange_rates"
        for (r,) in db.execute(query):
            print(repr(r))


def main() -> None:
    subcommands = {
        'create': create_cmd,
        'update': update_cmd,
        'convert': convert_cmd,
        'missing-dates': missing_dates_cmd,
        'list-currencies': list_currencies_cmd,
    }

    if len(sys.argv) < 3 or sys.argv[1] not in subcommands:
        cmd = sys.argv[0]
        sys.stderr.write("\n    ".join([
            "Usage:",
            "{cmd} create DBNAME [TSV]...",
            "{cmd} update DBNAME [TSV]...",
            "{cmd} convert DBNAME DATE FROM TO AMOUNT",
            "{cmd} missing-dates DBNAME [START [STOP]]",
            "{cmd} list-currencies DBNAME",
        ]))
        sys.stderr.write("\n")
        sys.exit(1)

    subcommands[sys.argv[1]]()

if __name__ == '__main__':
    main()
