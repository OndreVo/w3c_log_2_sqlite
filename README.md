# W3C log to SQLite

Simple utility to parse W3C HTTP log files (from IIS) to SQLite database so you can analyze logs with SQL queries.

## Config file
Utility looks for config.json in current directory of in a file specified by --config option on command line. For config file syntaxe (it's JSON) look at config.sample

## Usage
log2sqlite.py *list of log files*

### More options
-h
: print help
--config filepath
: path to config file
--db dbname
: path to sqlite file
--table tablename
: name of a table where logs will be imported to
--qpar queryparam
: name of url query parametr which will be extracted to separate column
