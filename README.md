A script that helps you import old CoreProtect SqlLite databases into a MySql database.
Note: this will only migrate block modifications at the moment.

### Code notes
Any references to SqlLite mean the old database, any references to MySql mean the new database.
Set environ `CACHE_MYSQL=1` to cache info while testing.