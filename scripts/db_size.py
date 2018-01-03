#!/usr/bin/env python

from os.path import dirname, abspath, join, isdir
import os
import plyvel

if __name__ == '__main__':
    db_names = list()
    db_dir = join(dirname(dirname(abspath(__file__))), 'database')
    for sub_dir in os.listdir(db_dir):
        if isdir(join(db_dir, sub_dir)):
            db_names.append(sub_dir)
    for db_name in db_names:
        num = 0
        db = plyvel.DB(join(db_dir, sub_dir))
        for _, __ in db:
            num += 1
        print('%s contains %s entries.' % (db_name, num))
 