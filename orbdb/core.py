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
import orbdb.version
import warnings
import logging
import MySQLdb

__version__ = orbdb.version.__version__

class OrbDB(Tools):

    recorded_keys = None
    db = None
    cur = None

    def __init__(self, db_name, **kwargs):
        Tools.__init__(self, **kwargs)
        
        self.recorded_keys = self.get_keys()
        
        
        self.db = MySQLdb.connect('localhost', 'orbdb', 'orbdb-passwd', db_name, use_unicode=True, charset='utf8')
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
                    hdu = self.read_fits(
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

    def list_rows(self, expr, order_key, file_type):
        file_types = ['o', 'a', 'x', 'f', 'c']
        if file_type is not None:
            if file_type not in file_types:
                raise StandardError('File type must be in {}'.format(file_types))
                
        if '=' in expr:
            expr = expr.split('=')
            operation = '='    
        else: raise StandardError('Bad expression format')

        if order_key is not None:
            order_key = '{} ASC'.format(self._get_formatted_key(order_key))
        else: order_key = 'NULL'

        self.cur.execute("SELECT fitsfilepath from files WHERE {}{}'{}' ORDER BY {}".format(
            self._get_formatted_key(expr[0]), operation, expr[1],
            order_key))
        
        for row in self.cur.fetchall():
            row = [str(irow) for irow in row]
            file_name = ' '.join(row)
            if file_type is not None:
                if file_type + '.fits' in file_name:
                    print file_name
            else:
                print file_name


    def list_targets(self):
        """List scans by target"""
        
        def format_name(_current_scan):
            _current_scan = [str(_i) for _i in _current_scan]
            return ' '.join(_current_scan)
        
        self.cur.execute("SELECT {},{},{},{},{},{},{} from files ORDER BY {} ASC".format(
            self._get_formatted_key('OBJECT'),
            self._get_formatted_key('FILENAME'),
            self._get_formatted_key('FILTER'),
            self._get_formatted_key('RUNID'),
            self._get_formatted_key('SITSTEP'),
            self._get_formatted_key('PI_NAME'),
            self._get_formatted_key('DATE'),
            self._get_formatted_key('FILENAME')))
        
        current_scan = None, None
        scans = dict()
        for row in self.cur.fetchall():
            if row[1] is None:
                continue
            
            if row[1][-1] in ('a', 'f', 'x') or 'twostep' in row[1]:
                continue
            
            if np.all((row[0], row[2], row[1][-1]) != current_scan):
                if len(scans) > 0:
                    # update last scan
                    scans[format_name(current_scan)] += list(
                        ['end step: {}, file: {}, at {} '.format(
                            last_step, last_filename, last_date)])
                    ## del scans[-1]
                    ## scans.append(list(last_scan)
                    ##              + list([current_date]))

                current_scan = (row[0], row[2], row[1][-1])
                    
                if format_name(current_scan) not in scans:
                    scans[format_name(current_scan)] = [
                        'start step: {}, file: {}, at {} '.format(
                            row[4], row[1], row[6])]
                else:
                    scans[format_name(current_scan)] += list(
                        ['start step: {}, file: {}, at {} '.format(
                            row[4], row[1], row[6])])
                    
            last_date = row[6]
            last_filename = row[1]
            last_step = row[4]

        lines = list()
        for scan in scans:
            lines.append((scan, ''.join(
                ' > ' + iscan + '\n' for iscan in scans[scan])))
            
        lines = sorted(lines, key=lambda line: line[0])
        for line in lines:
            if line[0].strip().split()[-1] == 'o':
                color = TextColor.GREEN
            elif line[0].strip().split()[-1] == 'c':
                color = TextColor.CYAN
            else: color = ''
            
            print color + line[0] + TextColor.END
            print line[1]

    def list_dates(self):
        """List scans by dates"""
        
        def format_name(_current_scan):
            return ' '.join(_current_scan)
        
        self.cur.execute("SELECT {},{},{},{},{},{},{} from files ORDER BY {} ASC".format(
            self._get_formatted_key('OBJECT'),
            self._get_formatted_key('FILENAME'),
            self._get_formatted_key('FILTER'),
            self._get_formatted_key('RUNID'),
            self._get_formatted_key('SITSTEP'),
            self._get_formatted_key('PI_NAME'),
            self._get_formatted_key('DATE'),
            self._get_formatted_key('DATE')))
        
        current_scan = None, None, None
        current_date = None
        last_date = None
        for row in self.cur.fetchall():
            if row[1] is None:
                continue
            
            if row[1][-1] in ('a', 'f', 'x') or 'twostep' in row[1]:
                continue

            if row[6].split('T')[0] != current_date:
                if last_date is not None:
                    print ' > end step: {}, file: {}, at {} '.format(
                        last_step, last_filename, last_date)

                print '\n===== {} ====='.format(current_date)                
                current_date = row[6].split('T')[0]
                last_date = None
                current_scan = None
                
            if np.all((row[0], row[2], row[1][-1]) != current_scan):
                # update last scan
                if last_date is not None:
                    print ' > end step: {}, file: {}, at {} '.format(
                        last_step, last_filename, last_date.split('T')[1])
                    
                current_scan = (row[0], row[2], row[1][-1])
                if current_scan[-1] == 'o':
                    color = TextColor.GREEN
                elif current_scan[-1] == 'c':
                    color = TextColor.CYAN
                else:
                    color = TextColor.END
                print color + '{} {} {}'.format(*current_scan) + TextColor.END


                print ' > start step: {}, file: {}, at {} '.format(
                    row[4], row[1], row[6].split('T')[1])
                    
            last_date = row[6]
            last_filename = row[1]
            last_step = row[4]



    def get_keys(self):
        """get keys to record"""
        recorded_keys = list()
        rec_keys_path = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'rec_keys.orbdb')
        
        with open(rec_keys_path, 'r') as f:
            for line in f:
                recorded_keys.append(line.strip())
        return recorded_keys

    


    def __del__(self):
        if self.db is not None:
            self.db.commit()
            self.db.close()
        if self.cur is not None:
            self.cur.close()
