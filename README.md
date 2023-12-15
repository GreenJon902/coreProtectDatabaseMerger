A script that helps you import old CoreProtect SqlLite databases into a MySql database.
Note: this will only migrate block modifications at the moment.
Note: You will need to add an imported column on the co_block table, this should be boolean and default to 0.

### Code notes
Any references to SqlLite mean the old database, any references to MySql mean the new database.
Set environ `CACHE_MYSQL=1` to cache info while testing.