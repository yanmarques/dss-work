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


def handle_summarization(src_cursor, dst_conn, dst_cursor, installment):
    # unpack all received information
    dt_vcto, vl_par, dt_pagto, vl_pago, cd_ven, cd_loj, cd_cli, cd_vdd, \
        nm_vdd, nm_loj, nm_cli = installment

    # calculate the metrics
    instmnt_preddicted_count, instmnt_late_count, instmnt_paid_count, \
            instmnt_preddicted_value = 0, 0, 0, 0

    # deduce which date to put in time dimension
    if vl_pago:
        date_in_dimension = dt_pagto
        
        # also define that this installment is paid
        instmnt_paid_count = 1

        # was it paid after the scheduled date
        if dt_vcto < dt_pagto:
            instmnt_late_count = 1
    else:
        date_in_dimension = dt_vcto

        # also define this installment as preddicted  
        instmnt_preddicted_count = 1
        instmnt_preddicted_value = vl_par

    dst_cursor.execute('select cd_tempo from tempo where ano = %s and mes = %s', 
                    (str(date_in_dimension.year), str(date_in_dimension.month)))
    time_dimension = dst_cursor.fetchone() 
    if time_dimension is None:
        dst_cursor.execute('insert into tempo (ano, mes) values (%s, %s) returning cd_tempo', 
                        (date_in_dimension.year, date_in_dimension.month))
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

    # store the parameters to the sql operation for change during runtime
    operation_params = (cd_ven, 
                        time_dimension[0],
                        cd_loj,
                        cd_cli,
                        cd_vdd,
                        instmnt_preddicted_count,
                        instmnt_late_count,
                        instmnt_paid_count,
                        instmnt_preddicted_value)

    # lets check on db for an existing sale 
    dst_cursor.execute(check_sale_existence(), operation_params[:5])
    if dst_cursor.fetchone() is None:
        # insert data into fact table, the parameters order are ok
        sql_to_run = save_sale()
    else:
        # make sure summary is uptaded, change the parameters order because the integers came first
        sql_to_run = update_sale()

        # swap params because to update we must put installment info. in first positions 
        operation_params = operation_params[5:] + operation_params[:5]

    # execute then commit whole transaction
    dst_cursor.execute(sql_to_run, operation_params)
    dst_conn.commit()


def update_sale():
    return """
update venda 
set
    nr_par_previstas = nr_par_previstas + %s,
    nr_par_atrasadas = nr_par_atrasadas + %s,
    nr_par_pagas = nr_par_pagas + %s,
    vlr_par_previstas = vlr_par_previstas + %s
where
    cd_venda = %s and 
    cd_tempo = %s and
    cd_loja = %s and
    cd_cliente = %s and
    cd_vendedor = %s
"""


def save_sale():
    return """
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
"""


def check_sale_existence():
    return """
select
    cd_venda 
from 
    venda 
where
    cd_venda = %s and
    cd_tempo = %s and
    cd_loja = %s and
    cd_cliente = %s and
    cd_vendedor = %s
"""


def instmnts_list_expr():
    return """
select
	par.dt_vcto,
	par.vl_par,
	par.dt_pagto,
	par.vl_pago,
	par.cd_ven,
	par.cd_loj,
	ven.cd_cli,
	ven.cd_vdd,
	vdd.nm_vdd,
	loja.nm_loj,
	cli.nm_cli
from 
	parcela as par 
inner join venda ven on (par.cd_ven = ven.cd_ven and par.cd_loj = ven.cd_loj)
inner join cliente cli on ven.cd_cli = cli.cd_cli 
inner join vendedor vdd on ven.cd_vdd = vdd.cd_vdd 
inner join loja on ven.cd_loj = loja.cd_loj 
"""


def init_etl(src_cursor, dst_conn, dst_cursor, initdb=None):
    # handle database helper file
    if initdb is not None:
        print('INFO: Initializing destination database from: {}'.format(initdb))
        with open(initdb) as sql_reader:
            dst_cursor.execute(sql_reader.read())
        print('INFO: Database initialization finished!')

    # count the number of installments that we will fetch
    # this is done separate to avoid holding all fetched data
    # in memory so instead we just count it on the db
    src_cursor.execute('select count(*) from parcela')
    instmnts_count = src_cursor.fetchone()[0]

    # count the number of sales which will be processed
    print('INFO: Total> {}'.format(instmnts_count))

    # create a default mask to bump the progress
    progress_mask = 'INFO: Progress> %d/{}\r'.format(instmnts_count) 
    index = 1
    
    # fetch all records and process it
    src_cursor.execute(instmnts_list_expr())
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