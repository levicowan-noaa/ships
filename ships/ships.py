__all__ = ['Ships']

from datetime import datetime
import logging
import os, sys
from pprint import pprint
import sqlite3

# Default working directory is the parent directory of this file (package root)
workdir = os.path.dirname(__file__)
# Setup logging
logger = logging.getLogger(__name__)
logfile = os.path.join(workdir, 'ships.log')
logging.basicConfig(filename=logfile, level=logging.INFO)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)
# Create custom hook for uncaught exceptions
def exc_hook(Type, value, tb):
    logger.exception(msg='', exc_info=(Type, value, tb))
sys.excepthook = exc_hook


class Ships:
    def __init__(self):
        self.logfile = logfile
        self.datadir = os.path.join(workdir, 'data')
        self.rawtext_filename = os.path.join(self.datadir, 'ships.txt')
        self.documentation_filename = os.path.join(self.datadir, 'ships_predictor_file_2020.txt')
        if not os.path.exists(self.datadir):
            os.makedirs(self.datadir, 0o755)
        self.db_filename = os.path.join(self.datadir, 'ships.db')
        self.db = sqlite3.connect(self.db_filename)
        self.tablename = 'diagnostics' # SQL table name

    def load_documentation(self):
        """
        Parse the documentation for SHIPS parameters and load it into a dict of form:
        {parameter_name: description}
        """
        self.parameter_descriptions = {}
        with open(self.documentation_filename) as f:
            for line in f.readlines():
                parts = [p.strip() for p in line.split(':')]
                param = parts[0]
                descr = ':'.join(parts[1:]) if len(parts) > 1 else ''
                self.parameter_descriptions[param] = descr
        pprint(self.parameter_descriptions)

    def get_diag_names(self):
        """Get names of all diagnostic parameters in the SHIPS file, in order"""
        with open(self.rawtext_filename) as f:
            # Initialize using header line
            line = f.readline().strip()
            fields = line.split()
            # Some rows have an additional number following the parameter name
            getname = lambda fields: fields[-2] if all(c.isdigit() for c in fields[-1]) else fields[-1]
            # Loop through first storm block
            names = []
            while line and fields[-1] != 'LAST':
                name = getname(fields)
                if name != 'HEAD':
                    names.append(name)
                line = f.readline().strip()
                fields = line.split()
            return names

    def parse_and_save_to_db(self):
        """Parse raw text file and save obs to SQL database"""

        # Create diagnostics table. If it already exists, replace it.
        c = self.db.cursor()
        c.execute(f'DROP TABLE IF EXISTS {self.tablename}')
        blacklist = ['TIME'] # Some parameters we don't want or are redundant
        colnames = [name for name in self.get_diag_names() if name not in blacklist]
        # Table has a couple identifying columns, followed by all diagnostic parameters
        # stored as integers (as in the raw file)
        cols = 'ATCF_ID CHAR(8), TIME DATETIME, ' + ','.join(f'{name} INT' for name in colnames)
        c.execute(f'CREATE TABLE {self.tablename}({cols})')
        self.db.commit()

        logger.info('Parsing raw SHIPS text file...')
        # Some rows have an additional number following the parameter name
        getname = lambda fields: fields[-2] if all(c.isdigit() for c in fields[-1]) else fields[-1]
        with open(self.rawtext_filename) as f:
            rows = []
            row = []
            linenum = 1
            line = f.readline().strip()
            # No blank lines expected until the end of the file
            while len(line) > 0:
                print(f'Parsing line {linenum}', end='\r')
                fields = line.split()
                name = getname(fields) # Row name
                # File should start with a header line
                if linenum == 1:
                    assert getname(fields) == 'HEAD', 'expected header line'

                if name == 'HEAD':
                    # Get storm info from block header
                    curID = fields[-2] # ATCF ID
                    yymmdd = fields[1]
                    utchour = fields[2]
                    # 2-digit year is ambiguous - careful! Use 4-digit year from ATCF ID
                    yyyymmddhh = curID[-4:]+yymmdd[2:]+utchour
                    curtime = datetime.strptime(yyyymmddhh, '%Y%m%d%H')
                    # Record last ob and start a new observation
                    if row:
                        rows.append(row)
                    row = [curID, curtime.strftime('%Y-%m-%d %H:%M:%S')]
                elif name == 'LAST':
                    # Collect and insert 100 rows at a time to avoid loading the entire file at once
                    if len(rows) >= 100:
                        c.executemany(f'INSERT INTO {self.tablename} VALUES ({",".join("?"*len(rows[0]))})', rows)
                        self.db.commit()
                        rows.clear()
                else:
                    # Diagnostic parameter from this line for hour 0 (same time as curtime)
                    param_name, param_val = name, fields[2]
                    if param_name != 'LAST' and param_name not in blacklist:
                        row.append(param_val)

                # Read next line
                line = f.readline().strip()
                linenum += 1
        print()

    def get_storm_obs(self, ATCF_ID):
        """
        Fetch all SHIPS obs for the given storm from the SQL database.
        This only works for storms with an assigned ATCF ID (Atlantic, EPAC, CPAC storms)

        Args:
            ATCF_ID: e.g., 'AL132005'

        Returns:
            A dict of form {parameter: [list of time-ordered values]}
        """
        rows = list(self.db.execute(f'SELECT * FROM {self.tablename} WHERE ATCF_ID="{ATCF_ID}" ORDER BY TIME'))
        colnames = [info[1] for info in self.db.execute(f'PRAGMA table_info("{self.tablename}")')]
        values = list(zip(*rows))
        data = {colname: values for colname, values in zip(colnames, values)}
        return data

    def load_from_db(self):
        """Parse SQL database and (re)construct Storm objects"""
        logger.info('Loading all TCs from database...')
        self.storms.clear()
        rows = list(self.db.execute(f'SELECT * FROM {self.tablename} ORDER BY ID,time'))
        # Group rows by TC ID
        rows_by_TC = {}
        for row in rows:
            rows_by_TC.setdefault(row[0], []).append(row)
        # Format into mapping of column names to column data
        colnames = [info[1] for info in self.db.execute(f'PRAGMA table_info("{self.tablename}")')]
        for storm_rows in rows_by_TC.values():
            values = list(zip(*storm_rows))
            data = {colname: values for colname, values in zip(colnames, values)}
            self.storms.append(Storm(data, datatype='db'))
        self.resolve_duplicates()

    def load_all_storms(self, source='db'):
        """
        Load all Storm objects into self.storms

        Args:
            source: If 'db': load from SQL database generated by self.save_to_db()
                    If 'json': load from JSON files generated by self.save_to_json()
                    If 'csv': load from the raw IBTrACS CSV file
        """
        if source == 'db':
            assert os.path.exists(self.db_filename), 'database file does not exist'
            self.load_from_db()
        elif source == 'json':
            assert os.path.exists(os.path.join(workdir, 'data/json')), 'JSON files do not exist'
            self.load_from_json()
        elif source == 'csv':
            assert os.path.exists(os.path.join(self.datadir, 'ibtracs.csv'))
            self.load_from_csv()
        else:
            raise ValueError(f'Unrecognized source: {source}')
        # Sort storm list by genesis time
        self.storms.sort(key=lambda tc: tc.genesis)

    def save_to_db(self):
        """
        Save all storm objects to an sqlite3 database
        """
        if not self.storms:
            logger.info('Parsing storm data...')
            self.load_all_storms(source='csv')
        c = self.db.cursor()
        # Create storm table. If it already exists, replace it.
        c.execute(f'DROP TABLE IF EXISTS {self.tablename}')
        c.execute(f"""
            CREATE TABLE {self.tablename}(
                ID CHAR(13),      ATCF_ID CHAR(8),
                name VARCHAR,     season INT,
                basin CHAR(2),    subbasin CHAR(2),
                lat FLOAT,        lon FLOAT,
                time DATETIME,    wind INT,
                mslp INT,         classification CHAR(2),
                speed FLOAT,      dist2land INT,
                genesis DATETIME, agency VARCHAR,
                R34_SE FLOAT,     R34_NE FLOAT,
                R34_SW FLOAT,     R34_NW FLOAT,
                R50_SE FLOAT,     R50_NE FLOAT,
                R50_SW FLOAT,     R50_NW FLOAT,
                R64_SE FLOAT,     R64_NE FLOAT,
                R64_SW FLOAT,     R64_NW FLOAT
        )""")
        self.db.commit()

        # Insert each track point as a row
        rows = []
        radii_attrs = [f'R{v}_{q}' for v in (34,50,64) for q in ('NE','SE','SW','NW')]
        for tc in self.storms:
            genesis = tc.genesis.strftime('%Y-%m-%d %H:%M:%S')
            for i in range(len(tc.time)):
                t = tc.time[i].item().strftime('%Y-%m-%d %H:%M:%S')
                vals = (
                    tc.ID, tc.ATCF_ID, tc.name, tc.season, tc.basins[i], tc.subbasins[i],
                    tc.lat[i], tc.lon[i], t, tc.wind[i], tc.mslp[i],
                    tc.classification[i], tc.speed[i], tc.dist2land[i], genesis, tc.agencies[i]
                )
                # Wind radii values
                rvals = tuple(getattr(tc, attr)[i] for attr in radii_attrs)
                row = vals + rvals
                rows.append(row)
        logger.info(f'Inserting {len(rows)} rows into database...')
        c.executemany(f'INSERT INTO {self.tablename} VALUES ({",".join("?"*len(rows[0]))})', rows)
        self.db.commit()


    def get_storm(self, name, season, basin):
        """
        Fetch a TC from the SQL database and construct a Storm object.
        This can only be used to get storms with a defined name
        """
        rows = list(self.db.execute(f'SELECT * FROM {self.tablename} WHERE name = "{name.upper()}" AND season = {season} AND basin = "{basin}" ORDER BY time'))
        if not rows:
            raise ValueError(f'Storm not found in database: name={name}, season={season}, basin={basin}')
        colnames = [info[1] for info in self.db.execute(f'PRAGMA table_info("{self.tablename}")')]
        values = list(zip(*rows))
        data = {colname: values for colname, values in zip(colnames, values)}
        return Storm(data, datatype='db')
