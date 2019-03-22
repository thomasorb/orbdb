#!/usr/bin/python
# *-* coding: utf-8 *-*
# Author: Thomas Martin <thomas.martin.1@ulaval.ca>
# File: core

## Copyright (c) 2010-2018 Thomas Martin <thomas.martin.1@ulaval.ca>
## 
## This file is part of ORBDB
##
## ORBDB is free software: you can redistribute it and/or modify it
## under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
##
## ORBDB is distributed in the hope that it will be useful, but WITHOUT
## ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
## or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
## License for more details.
##
## You should have received a copy of the GNU General Public License
## along with ORBDB.  If not, see <http://www.gnu.org/licenses/>.

import os
import numpy as np
from orb.core import Tools, TextColor
import orb.utils.io
import orbdb.version
import warnings
import logging
import MySQLdb
import pandas as pd

__version__ = orbdb.version.__version__

class OrbDB(Tools):

    base_keys = 'OBJECT', 'FILTER', 'RUNID', 'PI_NAME', 'SITSTEP'
    
    def __init__(self, db_name, **kwargs):
        Tools.__init__(self, **kwargs)
        
        self.recorded_keys = self.get_keys()

        self.db = MySQLdb.connect('127.0.0.1', 'orbdb', 'orbdb-passwd',
                                  db_name, use_unicode=True, charset='utf8')
        self.cur = self.db.cursor()
        
        # check if database is populated
        self.cur.execute('show tables')
        tables = self.cur.fetchall()
        self.keys = []
        if len(tables) == 0:
            warnings.warn("Database is empty. To populate it use 'append' operation")
            self.cur.execute("CREATE TABLE files ( fitsfilepath TEXT )")
        else:
            a = [table[0] for table in tables]
            
            if u'files' in [table[0] for table in tables]:
                self.cur.execute("desc files")
                self.keys = [col[0] for col in self.cur.fetchall()]
            
            else:
                warnings.warn("Files table does not exist. To populate it use 'append' operation")
                self.cur.execute("CREATE TABLE files ( fitsfilepath TEXT )")

    
    def __del__(self):
        if self.db is not None:
            self.db.commit()
            self.db.close()
        if self.cur is not None:
            self.cur.close()

    def _init_dataframe(self):
        if hasattr(self, 'df'): return
            
        # create pandas dataframe
        alldata = dict()
        alldata['path'] = list()
        alldata['odo'] = list()
        for key in self.base_keys:
            alldata[key] = list()

        self.cur.execute("SELECT fitsfilepath,{} from files".format(
            ','.join([self._get_formatted_key(key) for key in self.base_keys])))
        
        for row in self.cur.fetchall():
            alldata['path'].append(row[0])
            try: odo = int(os.path.split(row[0])[-1][:-6])
            except ValueError: odo = 0
            alldata['odo'].append(odo)
            for i in range(len(self.base_keys)):
                alldata[self.base_keys[i]].append(row[i+1])

        self.df = pd.DataFrame(alldata)

    def _get_formatted_key(self, key):
        key_formatted = 'key' + str(key).lower()
        if '-' in key:
            key_formatted = ''.join(key_formatted.split('-'))
        return key_formatted

    def append(self, list_path, force_update=False):
        with open(list_path, 'r') as f:
            counts = 0
            for line in f:
                filepath = os.path.abspath(line.strip().split()[0])
                self.cur.execute("SELECT fitsfilepath from files WHERE fitsfilepath='{}'".format(filepath))
                checked_files = self.cur.fetchall()
                if len(checked_files) == 0 or force_update:
                    counts += 1
                    hdu = orb.utils.io.read_fits(
                        filepath, return_hdu_only=True)
                    hdu.verify('fix')
                    hdr = hdu[0].header
                    
                    logging.info('Updating database with {}'.format(filepath))
                    if force_update:
                        self.cur.execute("DELETE FROM files WHERE fitsfilepath='{}'".format(filepath))
                    self.cur.execute("INSERT INTO files SET fitsfilepath='{}'".format(filepath))
                   
                   
                    keys_formatted = list()
                    values = list()
                    to_set_string = list()
                    for key in self.recorded_keys:
                        if key in hdr :
                            key_formatted = self._get_formatted_key(key)

                            if key_formatted not in self.keys:

                                if isinstance(hdr[key], str):
                                    logging.info('Creating new column for key {}'.format(key))
                                    self.cur.execute("ALTER TABLE files ADD {} VARCHAR(80)".format(key_formatted))
                                elif isinstance(hdr[key], float):
                                    logging.info('Creating new column for key {}'.format(key))
                                    self.cur.execute("ALTER TABLE files ADD {} FLOAT".format(key_formatted))

                                elif isinstance(hdr[key], bool):
                                    logging.info('Creating new column for key {}'.format(key))
                                    self.cur.execute("ALTER TABLE files ADD {} BOOL".format(key_formatted))

                                elif isinstance(hdr[key], int):
                                    logging.info('Creating new column for key {}'.format(key))
                                    self.cur.execute("ALTER TABLE files ADD {} INT".format(key_formatted))
                                elif isinstance(hdr[key], long):
                                    logging.info('Creating new column for key {}'.format(key))
                                    self.cur.execute("ALTER TABLE files ADD {} LONG".format(key_formatted))

                                self.keys.append(key_formatted)
                            keys_formatted.append(key_formatted)
                            value = hdr[key]
                            if isinstance(value, bool):
                                value = int(value)
                            if value == 'nan': value = -9999
                            values.append(value)
                            to_set_string.append("{}='{}'".format(key_formatted, value))
                            
                                                
                    try: 
                        to_set_string = ", ".join(to_set_string)
                        self.cur.execute("UPDATE files SET {} WHERE fitsfilepath='{}'".format(to_set_string, filepath))
                        
                    except Exception, e:
                        warnings.warn('Error: {}'.format(e))
                        
                    if counts > 20:
                        self.db.commit()
                        print '> commit'
                        counts = 0
                        
                else:
                    print '{} already in database'.format(filepath)
            self.db.commit()
            print '> commit'

    def clean(self):
        warnings.warn('Cleaning database')
        self.cur.execute('DROP TABLE IF EXISTS files')


    def list_between(self, odo1, odo2):
        """List scans between two odometers"""

        self._init_dataframe()
        if odo1 >= odo2: raise ValueError('first odometer must be lower than last odometer')

        subdf = self.df[(self.df['odo'] <= odo2)
                        * (self.df['odo'] >= odo1)]

        for i in range(len(subdf)):
            irow = subdf.iloc[i]
            strl = list()
            strl.append(irow['path'])
            for key in self.base_keys:
                val = irow[key]
                if key in ['SITSTEP',]:
                    try:
                        val = str(int(val))
                    except:
                        val = str(None)
                else:
                    val = str(val)
                strl.append(val)
            print ' '.join(strl)
            
        

    def print_rows(self, key, expr=None):
        if expr is not None:
            if '=' in expr:
                expr = expr.split('=')
                operation = '='
                fexpr = "WHERE {}{}'{}'".format(self._get_formatted_key(expr[0]), operation, expr[1])
            else: raise StandardError('Bad expression format')
        else:
            fexpr = ''
        
        for ikey in key:
            if self._get_formatted_key(ikey) not in self.keys:
                warnings.warn('{} not a valid keyword'.format(ikey))
        keys = [',' + self._get_formatted_key(ikey) for ikey in key if self._get_formatted_key(ikey) in self.keys]
        keys = ' '.join(keys)
        self.cur.execute("SELECT fitsfilepath{} from files {}".format(
            keys, fexpr))
        rows_list = list()
        for row in self.cur.fetchall():
            row = [str(irow) for irow in row]
            rows_list.append(row)

        rows_list = sorted(rows_list, key=lambda irow: irow[0])
        for irow in rows_list:
            print ' '.join(irow)
            
    def get_keys(self):
        """get keys to record"""
        recorded_keys = list()
        rec_keys_path = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'rec_keys.orbdb')
        
        with open(rec_keys_path, 'r') as f:
            for line in f:
                recorded_keys.append(line.strip())
        return recorded_keys

    

