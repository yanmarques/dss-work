#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import json
import sys
import traceback

import psycopg2


DEFAULT_CONFIG = 'config.json'


def getcon(from_src=True, **kwargs):
    conn_args = load_config(**kwargs)['src_conn' if from_src else 'dst_conn']
    return psycopg2.connect(**conn_args)


def init_etl(conn, cursor):
    cursor.execute('select * from test1')
    print(cursor.fetchone())


def load_config(config=DEFAULT_CONFIG):
    with open(config, 'rb') as bytes_reader:
        return json.load(bytes_reader)


def deduce_connection(cli_args, **kwargs):
    if cli_args:
        kwargs['config'] = cli_args[0]
    return getcon(**kwargs) 


def main(args):
    conn = deduce_connection(args)
    cursor = conn.cursor()
    
    try:
        init_etl(conn, cursor)
    except:
        traceback.print_exc()
    finally:
        print('Closing connection...')
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main(sys.argv[1:])