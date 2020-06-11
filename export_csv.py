#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import sys
import os
import traceback

import psycopg2
import etl


TABLES = [
    'venda',
    'tempo',
    'cliente',
    'vendedor',
    'loja'
]


def extract(target, cursor):
    if not os.path.exists(target):
        print('INFO: Target directory does not exists, creating it...')
        os.mkdir(target)

    os.chdir(target)

    progress_mask = 'INFO: Progress> %d/{}\r'.format(len(TABLES)) 

    for index, table in enumerate(TABLES):
        with open('{}.csv'.format(table), 'w') as csv_w:
            expr = 'copy (select * from {}) to stdout with csv header'
            cursor.copy_expert(expr.format(table), csv_w)
        print(progress_mask % (index + 1), end='')
    print('\nINFO: Finished exporting!')


def handle_args(args):
    if '--help' in args:
        print("""
Usage: {name} [--help] [CONFIG_FILE] TARGET_CSV_PATH

Arguments:
    CONFIG_FILE (optional) Specify a database conneciton configuration. (default={default_config})
    TARGET_PATH            Specify the directory to export the csv files into.

Options:
    --help              Shows this message.
""".format(name=sys.argv[0], default_config=etl.DEFAULT_CONFIG))
        return 128

    if not args:
        print('ERROR: Missing the target csv file.')
        return 1

    return args[:1], args[-1]


def main(args):
    result = handle_args(args)

    # stop here if we received and exit code
    if isinstance(result, int):
        return result

    # unpack received arguments
    config, target_path = result

    # connect to destination sgbd
    dst_conn = etl.deduce_connection(config, from_src=False)
    dst_cursor = dst_conn.cursor()

    try:
        extract(target_path, dst_cursor)
    except:
        traceback.print_exc()
        return 1
    finally:
        print('Closing destination connection...')
        dst_cursor.close()
        dst_conn.close()
    
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))