from typing import List
import sqlite3
from argparse import ArgumentParser, Namespace
from traceback import print_exc
from re import sub as re_sub
from json import loads as json_loads
from os.path import exists as file_exists

DB_FILE = 'log.sqlite'
TABLE_NAME = 'log'
QUERY_PARAMS_TO_COLUMNS = []

_FIELDSCONST = '#Fields:'

def _connect_db() -> sqlite3.Connection:
    db = sqlite3.connect(DB_FILE)
    db.execute('PRAGMA encoding="UTF-8";')    
    db.commit()
    return db

# returns url query param value or None if not found
def _parse_query_param_value(param:str, vals:List, query_column_index:int) -> str:
    if query_column_index < 0:
        return ''
    query_params = vals[query_column_index].split('&')
    for qp in query_params:
        if qp.startswith(param):
            return qp.split('=')[1]
    return ''

# writes line of log to db
# extracts params from url query if necessary
def _add_line(db:sqlite3.Connection, line:str, query_column_index:int) -> None:
    vals = [v for v in line.split(' ') if v]
    vals += [_parse_query_param_value(p, vals, query_column_index) for p in QUERY_PARAMS_TO_COLUMNS]
    pars = ', '.join(['?'] * len(vals))
    db.execute(f'INSERT INTO {TABLE_NAME} VALUES ({pars})', vals)

def _col_type(col:str) -> str:
    if col in ['time-taken']:
        return 'INTEGER'
    return 'TEXT'

def _col_name(col:str) -> str:
    return re_sub('[^a-zA-Z0-9]', '_', col)        

# creates log table if it doesn't exist
# returns index of query column or -1 if not found
def _ensure_table(db:sqlite3.Connection, line:str) -> int:
    columns = [_col_name(c) +' ' + _col_type(c) for c in line[len(_FIELDSCONST):].split(' ') if c]
    columns += [_col_name(c) +' TEXT' for c in QUERY_PARAMS_TO_COLUMNS]
    colstr = ', '.join(columns)
    query = f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({colstr})'
    #print('Creating table:', query)
    db.execute(query)
    return columns.index(_col_name('cs-uri-query') + ' TEXT')

# loads settings from json file
def _settings_from_config(configfile:str) -> None:
    global DB_FILE, TABLE_NAME
    if file_exists(configfile):
        with open(configfile, 'r', encoding='utf8') as f:
            json_data = json_loads(f.read())
            if 'db' in json_data:
                DB_FILE = json_data['db']
            if 'table' in json_data:
                TABLE_NAME = json_data['table']
            if 'qpar' in json_data:
                QUERY_PARAMS_TO_COLUMNS.extend(json_data['qpar'])

# settings from command line arguments
def _settings_from_args(args:Namespace) -> None:
    global DB_FILE, TABLE_NAME
    if args.db:
        DB_FILE = args.db
    if args.table:
        TABLE_NAME = args.table        
    if args.qpar:
        QUERY_PARAMS_TO_COLUMNS.extend(args.qpar)

# imports data from log file
def import_file(filepath:str) -> None:
    db = _connect_db()
    query_column_index = -1
    with open(filepath, 'r', encoding='utf8') as f:
        for line in f:            
            line = line.strip()            
            if not line:
                continue
            elif line.startswith(_FIELDSCONST):
                query_column_index = _ensure_table(db, line)
            elif line.startswith('#'):
                continue
            else:
                _add_line(db, line, query_column_index)
    db.commit()
    db.close()

parser = ArgumentParser()
parser.add_argument("--db", help="SQLite database file", type=str, nargs='?')
parser.add_argument("--table", help="Table name", type=str, nargs='?')
parser.add_argument("--config", default='config.json', help="Config file", type=str, nargs='?')
parser.add_argument("--qpar", metavar='query param', help="URL query params to be extracted to separate columns", type=str, nargs='*')
parser.add_argument('files', metavar='file', type=str, nargs='+', help='Files to import')
args = parser.parse_args()

_settings_from_config(args.config)
_settings_from_args(args)

for f in args.files:
    try:
        import_file(f)
    except Exception as e:
        print(f'Error importing {f}: {e}')
        print_exc()