# DB4S Download Status updater

This is a simple command line utility to perform country code
lookups on the historical DB4S download records, storing the
country code with each entry.

In theory (hopefully) that will give us a good source of
data for creating sample Redash [Choropleth Maps](https://en.wikipedia.org/wiki/Choropleth_map)
with.

## References

The country code lookups are being done with the database
[here](https://dbhub.io/justinclift/Geo-IP.sqlite), which obtained it's data from:

&nbsp; &nbsp; http://software77.net/geo-ip/
