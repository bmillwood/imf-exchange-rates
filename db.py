#!/usr/bin/python3
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
                    self.q.append((date, bits[0], float(value)))
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


def weekdays(start, stop):
    for ordinal in range(start.toordinal(), stop.toordinal()):
        current = datetime.date.fromordinal(ordinal)
        if current.weekday() < 5:
            yield current


def convert_cmd():
    try:
        dbname, date, from_, to, amount = sys.argv[2:]
        amount = float(amount)
    except ValueError:
        sys.stderr.write("Usage:\n\
            {0} convert DBNAME DATE FROM TO AMOUNT\n".format(sys.argv[0]))
        sys.exit(1)

    with sqlite3.connect(dbname) as db:
        cur = db.cursor()
        cur.execute('SELECT value FROM exchange_rates WHERE \
            date = ? AND currency = ?', (date, from_))
        from_result = cur.fetchall()
        cur.execute('SELECT value FROM exchange_rates WHERE \
            date = ? AND currency = ?', (date, to))
        to_result = cur.fetchall()

    if not from_result:
        sys.stderr.write("{0} is not a known currency\n".format(from_))
    if not to_result:
        sys.stderr.write("{0} is not a known currency\n".format(to))
    if not from_result or not to_result:
        sys.exit(1)

    print(amount * to_result[0][0] / from_result[0][0])


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
        start = dates[0]
    if stop is None:
        stop = dates[-1]
    for weekday in weekdays(start, stop):
        if weekday.isoformat() not in dates:
            print(weekday)


def main():
    subcommands = {
        'create': create_cmd,
        'update': update_cmd,
        'convert': convert_cmd,
        'missing-dates': missing_dates_cmd,
    }

    if len(sys.argv) < 3 or sys.argv[1] not in subcommands:
        sys.stderr.write("Usage:\n\
            {0} create DBNAME [TSV]...\n\
            {0} update DBNAME [TSV]...\n\
            {0} convert DBNAME DATE FROM TO AMOUNT\n\
            {0} missing-dates DBNAME [START [STOP]]\n".format(sys.argv[0]))
        sys.exit(1)

    subcommands[sys.argv[1]]()

if __name__ == '__main__':
    main()
