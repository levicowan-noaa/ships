#!/usr/bin/env python3

"""
Setup data directory, download raw SHIPS text files, and generate database
"""

import os, sys
# Ensure we import the *installed* SHIPS package, not file from local git repo
thisdir = os.path.dirname(os.path.realpath(__file__))
if thisdir in sys.path:
    sys.path.remove(thisdir)
from ships import Ships
from shutil import copy2
from urllib.request import urlopen


S = Ships()

# Download raw text files from CIRA if they don't exist
baseurl = 'http://rammb.cira.colostate.edu/research/tropical_cyclones/ships/docs'
urls = {
    'North Atlantic': baseurl+'/AL/lsdiaga_1982_2019_sat_ts.dat',
    'East Pacific': baseurl+'/EP/lsdiage_1982_2019_sat_ts.dat',
    'Central Pacific': baseurl+'/CP/lsdiagc_1982_2019_sat_ts.dat',
    'Western Pacific': baseurl+'/WP/lsdiagw_1990_2017.dat',
    'North Indian': baseurl+'/IO/lsdiagi_1990_2017.dat',
    'Southern Hemisphere': baseurl+'/SH/lsdiags_1998_2017.dat',
}
filename = os.path.join(S.datadir, 'ships.txt')
def progressbar(progress):
    print("\tProgress: [{0:50s}] {1:.1f}% ".format('#'*int(progress*50), progress*100), end='\r')
if not os.path.exists(filename):
    try:
        with open(filename, 'a') as lf:
            for basin, url in urls.items():
                with urlopen(url) as rf:
                    size = int(rf.getheader('Content-length'))
                    retrieved = 0
                    chunksize = 1024
                    print(f'Downloading raw {basin} SHIPS file from CIRA ({size/(1024*1024):.1f} MB)...')
                    while True:
                        chunk = rf.read(chunksize)
                        if not chunk:
                            # Component files are expected to have newlines at the end of the file,
                            # so we don't need to add one here
                            break
                        retrieved += len(chunk)
                        lf.write(chunk.decode('utf-8'))
                        progressbar(retrieved/size)
                    print()
    except ConnectionError:
        os.remove(filename)

# Create database
S.parse_and_save_to_db()

# Install predictor description file
copy2('ships/data/ships_predictor_file_2020.txt', S.datadir)
