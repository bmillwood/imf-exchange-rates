# Tools for downloading and parsing IMF exchange rates data

`./download.sh 201{1..6}-{01..12}` will fetch you five years of tsv files into
the current directory, in files named things like `2011-01.tsv`.

`./db.py create rates.db *.tsv` will then create a sqlite3 database `rates.db`
with all the data in it.

## Terms of use of IMF data

See https://www.imf.org/external/terms.htm and in particular:

> When Data is distributed or reproduced, it must appear accurately and 
> attributed to the IMF as the source, e.g. “Source: International 
> Monetary Fund.”. This source attribution requirement is attached to 
> any use of IMF Data, whether obtained directly from the IMF or from a 
> User. Users who make IMF Data available to other users through any 
> type of distribution or download environment agree to take reasonable 
> efforts to communicate and promote compliance by their End Users with 
> these terms. 
