# Python SHIPS API

A Python interface to the SHIPS tropical cyclone diagnostic dataset from CIRA (<http://rammb.cira.colostate.edu/research/tropical_cyclones/ships/developmental_data.asp>). Analysis values at t=0 for all time-dependent parameters are included. Currently, array-like parameters defined at a single time, such as precipitable water radial distribution and satellite parameters, are not included. Additionally, since storms are only identifiable by their ATCF ID in the raw data, only storms from the AL, EP, and CP basins are included.

## Dependencies
- Numpy
- Python >= 3.6

## Installation

Ensure your desired Python environment is activated, then:
```
git clone https://www.github.com/levicowan/ships /tmp/ships
cd /tmp/ships
python setup.py install
```

This will download the raw SHIPS text file from CIRA for each basin. The file will then get parsed, and an SQLite database will be created in which each row stores the diagnostics from a single storm observation time.

## Usage in a Python script or interactive interpreter

```
from ships import Ships
S = Ships()
```

### Obtain all diagnostic parameters for a single storm
These are organized as a dictionary, mapping parameter names to time-ordered sequences of values.
```
data = S.get_storm_obs('AL052019') # Hurricane Dorian
print(data['TIME'])
> array(['2019-08-24T06:00', '2019-08-24T12:00', '2019-08-24T18:00', ...])
print(data['VMAX'])
> [25., 30., 35., ...]
print(data['SHRD'])
> [13.5, 12.0, 8.1, ...]
```

### Obtain diagnostic parameters for a specific time
from datetime import datetime
data = S.get_storm_obs('AL052019', time=datetime(2019, 9, 5, 12))
print(data['VMAX'], data['SHRD'])
> 100.0 19.7
```

### Or use the SQLite database directly! Each track point is a row in the "diagnostics" table
```
query = 'SELECT TIME,SHRD FROM diagnostics WHERE ATCF_ID="AL052019" ORDER BY TIME'
for row in S.db.execute(query):
    print(row)
> ('2019-08-24 06:00:00', 135),
> ('2019-08-24 12:00:00', 120),
> ('2019-08-24 18:00:00', 81),
> ...
```
Note that units are different when using the database directly, since it stores the raw scaled values from CIRA, and no unit conversions have been made. See below for more information about units.

### Parameter descriptions and units:
```
S.load_documentation()
```
This prints the parameter descriptions from the CIRA documentation. A specific parameter may be queried like this: `print(S.parameter_descriptions['SHRD']`. Many parameters were scaled by CIRA to be stored as integers and thus may have non-standard units.

For data returned by functions like S.get_storm_obs(), an effort was made to convert commonly-used parameters such as shear and SST to their standard units, and these modifications are reflected in the documentation. The full, original documentation with original units can be found [here](http://rammb.cira.colostate.edu/research/tropical_cyclones/ships/docs/ships_predictor_file_2020.doc).

### If you need to remake the SQL database or re-parse the raw text file for any reason
```
S.parse_and_save_to_db()
```

### If you ever want to read/modify/replace the data files directly
```
print(S.datadir)
# Will be something similar to this
> ${workdir}/anaconda3/envs/${envname}/lib/python3.7/site-packages/ships/data
```

### View all attributes and methods available on the Ships object (S)
```
print([a for a in dir(S) if not a.startswith('_')])
> ['MISSING', 'datadir', 'db', 'db_filename', 'doc_filename', 'get_diag_names', 'get_storm_obs', 'load_documentation', 'logfile', 'parameter_descriptions', 'parse_and_save_to_db', 'rawtext_filename', 'tablename']
```

### Get log file path
```
print(S.logfile)
```
