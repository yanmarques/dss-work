#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import json
import sys
import traceback

import psycopg2

from unittest.mock import Mock


DEFAULT_CONFIG = 'config.json'


def getcon(from_src=True, **kwargs):
    conn_args = load_config(**kwargs)['src_conn' if from_src else 'dst_conn']
    return psycopg2.connect(**conn_args)


def handle_summarization(src_cursor, dst_conn, dst_cursor, sales_data):
    cd_ven, cd_loj, cd_cli, cd_vdd, dt_ven, nm_vdd, nm_loj, nm_cli = sales_data

    src_cursor.execute(installment_list_from_sale_expr(), (cd_ven, cd_loj))

    # calculate the metrics
    instmnt_preddicted_count, instmnt_late_count, instmnt_paid_count, \
            instmnt_preddicted_value = 0, 0, 0, 0
  
    for installment in src_cursor.fetchall():
        dt_vcto, vl_par, dt_pagto, vl_pago = installment
        
        if vl_pago:
            instmnt_paid_count += 1
        else:
            instmnt_preddicted_count += 1
            instmnt_preddicted_value += vl_par

        # isolate check for late payments, because it may also be considered as paid
        if dt_pagto and dt_vcto < dt_pagto:
            instmnt_late_count += 1

    dst_cursor.execute('select cd_tempo from tempo where ano = %s and mes = %s', 
                    (str(dt_ven.year), str(dt_ven.month)))
    time_dimension = dst_cursor.fetchone() 
    if time_dimension is None:
        dst_cursor.execute('insert into tempo (ano, mes) values (%s, %s) returning cd_tempo', 
                        (dt_ven.year, dt_ven.month))
        time_dimension = dst_cursor.fetchone()

    dst_cursor.execute('select cd_loja from loja where cd_loja = %s', [cd_loj])
    if dst_cursor.fetchone() is None:
        dst_cursor.execute('insert into loja (cd_loja, nm_loja) values (%s, %s)', 
                            (cd_loj, nm_loj))

    dst_cursor.execute('select cd_cliente from cliente where cd_cliente = %s', [cd_cli])
    if dst_cursor.fetchone() is None:
        dst_cursor.execute('insert into cliente (cd_cliente, nm_cliente) values (%s, %s)', 
                            (cd_cli, nm_cli))

    dst_cursor.execute('select cd_vendedor from vendedor where cd_vendedor = %s', [cd_vdd])
    if dst_cursor.fetchone() is None:
        dst_cursor.execute('insert into vendedor (cd_vendedor, nm_vendedor) values (%s, %s)', 
                            (cd_vdd, nm_vdd))

    # actually insert data into fact table
    dst_cursor.execute("""
insert into venda (
    cd_venda, 
    cd_tempo, 
    cd_loja, 
    cd_cliente, 
    cd_vendedor, 
    nr_par_previstas, 
    nr_par_atrasadas,
    nr_par_pagas, 
    vlr_par_previstas)
values
(%s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (
    cd_ven, 
    time_dimension[0],
    cd_loj,
    cd_cli,
    cd_vdd, 
    instmnt_preddicted_count,
    instmnt_late_count,
    instmnt_paid_count,
    instmnt_preddicted_value))
    
    # commit whole transaction
    dst_conn.commit()


def sales_list_expr():
    return """
select 
    ven.cd_ven,
    ven.cd_loj,
    ven.cd_cli,
    ven.cd_vdd,
    ven.dt_ven,
    vdd.nm_vdd,
    loj.nm_loj,
    cli.nm_cli
from venda as ven
    left join vendedor as vdd on (ven.cd_vdd = vdd.cd_vdd)
    left join loja as loj on (ven.cd_loj = loj.cd_loj)
    left join cliente as cli on (ven.cd_cli = cli.cd_cli)
"""


def installment_list_from_sale_expr():
    return """
select 
    par.dt_vcto,
    par.vl_par,
    par.dt_pagto,
    par.vl_pago
from parcela as par where par.cd_ven = %s and par.cd_loj = %s    
"""


def init_etl(src_cursor, dst_conn, dst_cursor, initdb=None):
    # count the number of sales which will be processed
    src_cursor.execute('select count(*) from venda')
    sales_count = src_cursor.fetchone()[0]
    print('INFO: Total> {}'.format(sales_count))
    
    # handle database helper file
    if initdb is not None:
        print('INFO: Initializing destination database from: {}'.format(initdb))
        with open(initdb) as sql_reader:
            dst_cursor.execute(sql_reader.read())
        print('INFO: Database initialization finished!')

    # create a default mask to bump the progress
    progress_mask = 'INFO: Progress> %d/{}\r'.format(sales_count) 
    index = 1

    # fetch all records and process it
    src_cursor.execute(sales_list_expr())
    for data in src_cursor.fetchall():
        print(progress_mask % index, end='')
        handle_summarization(src_cursor, dst_conn, dst_cursor, data)
        index += 1
    print('\nINFO: ETL has fineshed')


def test_cursor_execute_function(*args):
    print('rcv from cursor.execute(): {}'.format(args))


def test_cursor_fetchone_function():
    print('rcv a cursor.fetchone()')
    return [666]


def load_config(config=DEFAULT_CONFIG):
    with open(config, 'rb') as bytes_reader:
        return json.load(bytes_reader)


def deduce_connection(cli_args, **kwargs):
    if cli_args:
        kwargs['config'] = cli_args[0]
    return getcon(**kwargs)


def handle_args(args):
    if '--help' in args:
        print("""
Usage: {name} [--help] [--test] [--initdb SQL_FILE] [CONFIG_FILE]

Arguments:
    CONFIG_FILE (optional) Specify a database conneciton configuration. (default={default_config})

Options:
    --initdb SQL_FILE   Specify a SQL file to populate database before actually processing the ETL.
    --test              Read data from source connection, summarize, but just print what it would do in destination
                        connection.
    --help              Shows this message.
""".format(name=sys.argv[0], default_config=DEFAULT_CONFIG))
        return 128

    isatest = '--test' in args
    if isatest:
        args.remove('--test')

    kwargs = {}
    if '--initdb' in args:
        try:
            kwargs['initdb'] = args.pop(args.index('--initdb') + 1)
        except IndexError:
            print('ERROR: Option argument is missing for: --initdb')
            return 1
        args.remove('--initdb')
    return args, kwargs, isatest


def main(args):
    result = handle_args(args)

    # stop here if we received and exit code
    if isinstance(result, int):
        return result

    # unpack received data
    parsed_args, etl_kwargs, isatest = result

    # connect to source sgbd
    src_conn = deduce_connection(parsed_args)
    src_cursor = src_conn.cursor()
    
    # connect to destination sgbd
    dst_conn = deduce_connection(parsed_args, from_src=False)
    dst_cursor = dst_conn.cursor()

    # set that we want a 'read committed' level, so that we can commit by ourself
    dst_conn.set_isolation_level(1)

    test_cursor = Mock()
    test_cursor.execute = test_cursor_execute_function 
    test_cursor.fetchone = test_cursor_fetchone_function

    try:
        init_etl(
            src_cursor,
            dst_conn, 
            test_cursor if isatest else dst_cursor, 
            **etl_kwargs)
    except:
        traceback.print_exc()
        return 1
    finally:
        print('Closing source connection...')
        src_cursor.close()
        src_conn.close()

        print('Closing destination connection...')
        dst_cursor.close()
        dst_conn.close()
    
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))