import sys

import os
import psycopg2
import pgbuild

basic_role_tasks = """
- name: create .pgbuild/run directory
  file: path=/home/dev/.pgbuild/run state=directory

- name: copy install.sql
  copy: src=install.sql dest=/home/dev/.pgbuild/run/install.sql

- name: deploy cdr.yaml
  command: psql -f /home/dev/.pgbuild/run/install.sql -d {{cluster_name}} -p {{port}} --set=ON_ERROR_STOP=1
  sudo: yes
  sudo_user: postgres

- name: delete .pgbuild/run directory
  file: path=/home/dev/.pgbuild state=absent
"""

shard_role_tasks = """
- name: create .pgbuild/run directory
  file: path=/home/dev/.pgbuild/run state=directory

- name: transfer install.sql
  copy: src=install.sql dest=/home/dev/.pgbuild/run/install.sql

- name: run install.sql
  command: psql -f /home/dev/.pgbuild/run/install.sql -d {{cluster_name}}{{'_%02d'|format(item)}} -p {{port}} --set=ON_ERROR_STOP=1
  with_items: host.shards
  sudo: yes
  sudo_user: postgres

- name: delete .pgbuild/run directory
  file: path=/home/dev/.pgbuild state=absent
"""


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


def build(src, dest):
    """ Build sql scripts for roles """

    dest = os.path.abspath(dest)
    os.makedirs(dest)
    roles = pgbuild.roles.load_from_file(src)
    for role in roles:
        os.makedirs(os.path.join(dest, role.name, 'files'))
        os.makedirs(os.path.join(dest, role.name, 'tasks'))
        install_path = os.path.join(dest, role.name, 'files', 'install.sql')
        install = role.build()
        install_file = file(install_path, 'w')
        install_file.write(install.encode('utf8'))
        install_file.close()
        tasks_path = os.path.join(dest, role.name, 'tasks', 'main.yml')
        tasks_file = file(tasks_path, 'w')
        if role.name == 'shard':
            tasks_file.write(shard_role_tasks)
        else:
            tasks_file.write(basic_role_tasks)
        tasks_file.close()

    print green('OK'), 'build created at %s' % dest


if __name__ == '__main__':

    try:

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

    except Exception, e:
        print red('Error'), e
