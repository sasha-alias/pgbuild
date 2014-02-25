import sys

import os
import psycopg2
import pgbuild
from pgbuild import builder


def green(text):
    return '\033[92m'+text+'\033[0m'


def red(text):
    return '\033[91m'+text+'\033[0m'


def deploy(src, dest):
    """ Deploy table to destination """
    table = pgbuild.Table.load_from_location(src)
    conn_uri = dest.rstrip('/')
    conn = psycopg2.connect(conn_uri)
    table.drop_on_connection(conn)
    table.create_on_connection(conn)
    conn.commit()
    print green('OK'), 'deployed at %s' % conn_uri + '/' + table.name


def build(src, dest, format='psql'):
    """ Build sql scripts for roles """

    dest = os.path.abspath(dest)
    os.makedirs(dest)
    roles = pgbuild.roles.load_from_file(src)
    for role in roles:
        build_func = builder.builders.get(format)
        build_func(role, dest)

    print green('OK'), 'build created at %s' % dest


if __name__ == '__main__':

    #try:

        if sys.argv[1] == 'ddl':  # show yaml table DDL
                table = pgbuild.Table.load_from_location(sys.argv[2])
                print table.create_clause()

        elif sys.argv[1] == 'diff':  # shows ALTER 1st to 2nd

            if len(sys.argv) == 4:
                table1 = pgbuild.Table.load_from_location(sys.argv[2])
                table2 = pgbuild.Table.load_from_location(sys.argv[3])
                print table1.alter_to(table2)

        elif sys.argv[1] == 'deploy':
            deploy(sys.argv[2], sys.argv[3])

        elif sys.argv[1] == 'build':
            build(sys.argv[2], sys.argv[3])

        else:
            print red('Error:'), 'unknown command %s' % sys.argv[1]

    #except Exception, e:
    #    print red('Error'), e
