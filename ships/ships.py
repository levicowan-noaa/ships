__all__ = ['Ships']

from datetime import datetime
import logging
import numpy as np
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

    # Raw file missing value
    MISSING = 9999

    def __init__(self):
        self.logfile = logfile
        self.datadir = os.path.join(workdir, 'data')
        self.rawtext_filename = os.path.join(self.datadir, 'ships.txt')
        self.doc_filename = os.path.join(self.datadir, 'ships_predictor_file_2020.txt')
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
        with open(self.doc_filename) as f:
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
        # Some parameters we don't want, or are not actually defined as time-dependent
        # (e.g., satellite-derived parameters and TPW distribution, which are an array
        # of values for one time, and not easily stored in the database the same way)
        blacklist = ['TIME', 'MTPW', 'IRXX', 'IR00', 'IRM1', 'IRM3', 'PC00', 'PCM1', 'PCM3'
                     'PSLV', ]
        blacklist += [f'PW{x:02}' for x in range(0, 20)]
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

    def get_storm_obs(self, ATCF_ID, time=None):
        """
        Fetch all SHIPS obs for the given storm from the SQL database.
        This only works for storms with an assigned ATCF ID (Atlantic, EPAC, CPAC storms)

        Args:
            ATCF_ID: e.g., 'AL132005'

            time:    (datetime or None) If provided, only data from the requested time is returned.
                                        If None (default), data for all times is returned.

        Returns:
            A dict of form {parameter: values}. If `time` is None, values is an array. If `time`
            is specified, values is a single value.
        """
        if time is None:
            query = f'SELECT * FROM {self.tablename} WHERE ATCF_ID="{ATCF_ID}" ORDER BY TIME'
        else:
            t = time.strftime('%Y-%m-%d %H:%M:%S')
            query = f'SELECT * FROM {self.tablename} WHERE ATCF_ID="{ATCF_ID}" AND TIME="{t}"'
        rows = list(self.db.execute(query))
        colnames = [info[1] for info in self.db.execute(f'PRAGMA table_info("{self.tablename}")')]
        values = list(zip(*rows))
        dtypes = {'ATCF_ID': 'U8', 'TIME': 'datetime64[m]'}
        data = {}
        for colname, vals in zip(colnames, values):
            # All but a couple parameters will be numerical - use float so we can set missing to NaN
            dtype = dtypes.get(colname, float)
            # Time needs special reformatting
            if colname == 'TIME':
                vals = [datetime.strptime(s, '%Y-%m-%d %H:%M:%S') for s in vals]
            arr = np.array(vals, dtype=dtype)
            if dtype is float:
                arr[np.isclose(arr, self.MISSING)] = np.nan
            # Whether we store an array or a single value depends on whether a time was specified
            if time is not None and arr.size:
                data[colname] = arr[0]
            else:
                data[colname] = arr
        data = self._convert_units(data)
        return data

    def _convert_units(self, data):
        """
        Convert some commonly-used diagnostics (not all of them) to their standard units.
        These changes and the encoded units of all other parameters are reflected in
        self.doc_filename

        Args:
            data: dict of form {param_name: 1D array}

        Returns:
            Same data structure as input
        """
        conversion_factors = {
            'LAT': 0.1, 'LON': 0.1, 'CSST': 0.1, 'RSST': 0.1, 'DSST': 0.1, 'DSTA': 0.1,
            'NSST': 0.1, 'XDST': 0.1, 'U200': 0.1, 'T150': 0.1, 'T200': 0.1, 'T250': 0.1,
            'SHRD': 0.1, 'SHRS': 0.1,
        }
        for param, fac in conversion_factors.items():
            data[param] *= fac
        return data
