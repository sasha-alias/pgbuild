import sys
import os
from optparse import OptionParser
import traceback
import psycopg2
import pgbuild
from pgbuild import builder
import yaml


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
    if not os.path.exists(dest):
        os.makedirs(dest)
    roles = pgbuild.roles.load_from_file(src)
    for role in roles:
        build_func = builder.builders.get(build_format)
        build_func(role, dest)

    print green('OK'), 'build created at %s' % dest


if __name__ == '__main__':

    usage = """Usage: pgbuild command argumetns [options]

Commands:
    build - make a build of application
    deploy - deploy application to database
    diff - diff two tables
    ddl - print out a DDL of a table
    yaml - print out yaml definition of a table"""

    parser = OptionParser(usage=usage)
    parser.add_option('--format', dest='build_format', default='psql')
    parser.add_option('-o', '--overwrite', action="store_true", dest='overwrite', default=False)
    parser.add_option('-t', '--traceback', action="store_true", dest='show_traceback', default=False)
    (options, args) = parser.parse_args()

    try:

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
            if len(args) < 3:
                print red("No destination path pointed:\nUsage:\n  pgbuild build descriptor.yaml destination_path")
                sys.exit(-1)
            if os.path.exists(args[2]) and not options.overwrite:
                print red("Destination path already exists. To overwrite use -o (--overwrite) option:\nUsage:\n  pgbuild build %s %s --overwrite" % (args[1], args[2]))
                sys.exit(-1)
            build(args[1], args[2], options.build_format)

        elif args[0] == 'yaml':
            table = pgbuild.Table.load_from_location(args[1])
            print yaml.dump(yaml.load(str(table)), default_flow_style=False)

        else:
            print red('Error:'), 'unknown command %s' % args[1]

    except Exception, e:
        if options.show_traceback:
            traceback.print_exc(file=sys.stdout)
        else:
            print red('Error'), e
