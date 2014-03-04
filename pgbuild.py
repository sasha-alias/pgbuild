import sys
import os
from optparse import OptionParser
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


def build(src, dest, build_format='psql'):
    """ Build sql scripts for roles """

    dest = os.path.abspath(dest)
    os.makedirs(dest)
    roles = pgbuild.roles.load_from_file(src)
    for role in roles:
        build_func = builder.builders.get(build_format)
        build_func(role, dest)

    print green('OK'), 'build created at %s' % dest


if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option('--format', dest='build_format', default='psql')
    (options, args) = parser.parse_args()

    #try:

    if args[0] == 'ddl':  # show yaml table DDL
            table = pgbuild.Table.load_from_location(args[1])
            print table.create_clause()

    elif args[0] == 'diff':  # shows ALTER 1st to 2nd

        if len(args) == 3:
            table1 = pgbuild.Table.load_from_location(args[1])
            table2 = pgbuild.Table.load_from_location(args[2])
            print table1.alter_to(table2)

    elif args[0] == 'deploy':
        deploy(args[1], args[2])

    elif args[0] == 'build':
        build(args[1], args[2], options.build_format)

    else:
        print red('Error:'), 'unknown command %s' % args[1]

    #except Exception, e:
    #    print red('Error'), e
