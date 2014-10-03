#!/usr/bin/env python
'''

Author:  Larry Buza
'''
import sys
import argparse
import json
from utilities.db_interface import MyClient
from utilities.get_process_params import get_process_params
import multiprocessing


def handle_undefined_codes(tc):
    '''
    Inserts all undefined codes found in fact table into dimension table, using
    the 'code_source' table for mapping.

    Logs entries into the 'undefined_log' table.

    handle_undefined_codes does the lookup in the code_source table, starts a
    queue for multipprocessing runs of the 'get_undefined_codes' function.

    :param tc: MyClient instance
    :returns: None
    '''
    conn, cur = tc.dbi.get_connection(tc.conn_name, tc.environ, tc.logger)

    sql = '''
    select table_name, column_name,
    ref_table_name, ref_code_column_name, ref_desc_column_name
    from code_source
    where nvl(active_flag,'Y') = 'Y'
    and substr(table_name,1,2) <> 'T_'
    '''
    tc.dbi.exec_sql(cur, sql, tc.logger)
    rows = cur.fetchall()
    conn.close()
    q = multiprocessing.Queue()

    for row in rows:
        p = multiprocessing.Process(target=get_undefined_codes, args=(tc, row, q))
        p.start()
    p.join()
    while not q.empty():
        update_dim_table(tc, q.get())


def get_undefined_codes(tc, row, q):
    '''
    Finds unreferenced codes for each record in the 'row' result set.

    :param tc: MyClient instance
    :param row: A query result set (list of tuples) containing the dim and fact
    table values
    :param q: a queue to hold the return values
    :returns: Creates a dictionary containing the dim and fact values and a list
    with any found unreferenced codes.  These are added ('put') to the queue.
    '''
    (table_name, column_name, ref_table_name,
    ref_code_column_name, ref_desc_column_name) = row

    conn, cur = tc.dbi.get_connection(tc.conn_name, tc.environ, tc.logger)
    sql = '''
    select distinct t.%(column_name)s
    from %(table_name)s t
    left join %(ref_table_name)s r
    on t.%(column_name)s = r.%(ref_code_column_name)s
    where t.%(column_name)s is not null
    and r.%(ref_code_column_name)s is null
    ''' % {'table_name':table_name, 'column_name':column_name,
           'ref_table_name':ref_table_name,
           'ref_code_column_name':ref_code_column_name,
           'ref_desc_column_name':ref_desc_column_name}
    tc.dbi.exec_sql(cur, sql, tc.logger)
    rows = cur.fetchall()
    if rows:
        bad_codes = [i[0] for i in rows]
        undef_codes = {'table_name':table_name,
             'ref_table_name':ref_table_name,
             'ref_code_column_name':ref_code_column_name,
             'ref_desc_column_name':ref_desc_column_name,
             'bad_codes':bad_codes}
        q.put(undef_codes)


def update_dim_table(tc, undef_codes):
    '''
    Inserts the provided undefined code data into the dim table and the
    undefined_log table.

    :param tc: MyClient instance
    :param undef_codes: a dictionary containing the dim and fact values and a list
    with any found unreferenced codes.
    '''
    conn, cur = tc.dbi.get_connection(tc.conn_name, tc.environ, tc.logger)

    bad_sql = '''
    insert into %(ref_table_name)s (%(ref_code_column_name)s, %(ref_desc_column_name)s)
    values ('%(bad_code)s', 'undefined code (''%(bad_code)s'') from client')
    '''
    log_sql = '''
    insert into undefined_log(timestamp,table_name, code_column, desc_column, code,descript)
    values(sysdate, '%(ref_table_name)s', '%(ref_code_column_name)s',
    '%(ref_desc_column_name)s', '%(bad_code)s',
    'undefined code (''%(bad_code)s'') from client')
    '''

    for bad_code in undef_codes['bad_codes']:
        undef_codes['bad_code'] = bad_code

        for sql in (bad_sql, log_sql):
            sql = sql % undef_codes
            #print sql
            tc.dbi.exec_sql(cur, sql, tc.logger)
            conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='''Handle undefined reference codes.''',
        epilog='Author: Larry Buza\
                07/16/2014'
    )

    parser.add_argument('client', metavar='<client code>', help='Client Code')
    parser.add_argument('--debug', metavar='<debug>', default='INFO',
                        help='Optional Debug level runtime variable')

    pargs = parser.parse_args()
    client = pargs.client.lower()
    debug = pargs.debug

    tc = MyClient(client, 'dev', debug)
    tc.conn_name = tc.root_name

    process_params = get_process_params(client, 'handle_undefined_codes')

    for run_flag, params in process_params:
        if run_flag == 'Y':
            params = json.loads(params)
            handle_undefined_codes(tc, **params)

if __name__ == "__main__":
    sys.exit(main())



