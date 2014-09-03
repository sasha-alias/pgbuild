import os
import yaml
import tables
import functions
import types

def full_path(path):
    """
    return full path
    """
    if path is None:
        return None
    return os.path.normcase(os.path.realpath(os.path.abspath(os.path.expanduser(path))))


def absrelpath(path, start):
    """
    Return an absolute filepath of the path relative to the start point
    """

    init_path = full_path(os.getcwd())
    os.chdir(full_path(start))
    ret_path = full_path(path)
    os.chdir(init_path)
    return ret_path


def load_from_file(path):

    ret = []
    content = file(path).read()
    yaml_content = yaml.load(content)

    if isinstance(yaml_content, list):
        for module in yaml_content:
            if isinstance(module, str):
                module = absrelpath(module, os.path.dirname(path))
                module_content = yaml.load(file(module).read())
                ret += get_roles(module_content, module)
            else:
                ret += get_roles(module, path)

    else:
        ret += get_roles(yaml_content, path)
    return ret


def get_roles(content, path):
    ret = []
    for role_name in content.keys():
        role = Role(role_name, content[role_name], os.path.dirname(os.path.abspath(path)))
        ret.append(role)
    return ret


class RoleError(Exception):
    pass

class Role(dict):

    def __init__(self, name, descriptor, relpath_start):
        self.name = name
        self.descriptor = descriptor
        self.relpath_start = relpath_start
        self.tasks = []

        self._build_tasks()


    def _build_tasks(self):

        for idx, item in enumerate(self.descriptor):
            item_type = item.keys()[0]

            if item_type == 'schema':
                sql = 'CREATE SCHEMA IF NOT EXISTS %s;\n' % item[item_type]
                self.tasks.append(SQLTask(idx, item_type, sql))

            elif item_type == 'table':

                table_path = item[item_type]
                table_path = absrelpath(table_path, self.relpath_start)
                table = tables.Table.load_from_yaml_file(table_path)
                sql = table.create_clause()
                self.tasks.append(SQLTask(idx, item_type, sql))

            elif item_type == 'function':
                func_path = item[item_type]
                func_path = absrelpath(func_path, self.relpath_start)
                function = functions.Function.load_from_file(func_path)
                sql = unicode(function.script, 'utf-8')
                self.tasks.append(SQLTask(idx, item_type, sql))

            elif item_type == 'sql':
                sql = item[item_type].rstrip().rstrip(';')+';\n'
                self.tasks.append(SQLTask(idx, item_type, sql))

            elif item_type == 'type':
                item_path = item[item_type]
                item_path = absrelpath(item_path, self.relpath_start)
                custom_type = types.Type.load_from_yaml_file(item_path)
                sql = custom_type.drop_clause() + custom_type.create_clause()
                self.tasks.append(SQLTask(idx, item_type, sql))

            #elif item_type == 'job':
            #    self.jobs.append(item[item_type])

            elif item_type == 'copy':
                table= item[item_type]['table']
                columns = item[item_type]['columns']
                copy_from = item[item_type].get('from')
                copy_from = absrelpath(copy_from, self.relpath_start)
                copy_format = item[item_type].get('format')
                delimiter = item[item_type].get('delimiter')
                quote = item[item_type].get('quote')
                task = CSVTask(idx, item_type, table, columns,
                    copy_from = copy_from,
                    copy_format = copy_format,
                    delimiter = delimiter,
                    quote = quote
                    )
                self.tasks.append(task)

            else:
                raise RoleError('Unknown role item type "%s"' % item_type)


class SQLTask(object):

    def __init__(self, number, task_type, sql_content):
        self.number = number
        self.task_type = task_type
        self.sql_content = sql_content

    @property
    def transfer_entry(self):
        return """
- name: transfer {0}.sql
  copy: src={0}.sql dest=/tmp/.pgbuild/run/
""".format(self.number)


    @property
    def shards_entry(self):

        return """
- name: deploy {0}.sql
  command: psql -f /tmp/.pgbuild/run/{0}.sql -d {{{{cluster_name}}}}{{{{'_%02d'|format(item)}}}} -p {{{{port}}}} --set=ON_ERROR_STOP=1
  with_items: hostvars[inventory_hostname].shards
  sudo: yes
  sudo_user: postgres
""".format(self.number)


    @property
    def basic_entry(self):

        return """
- name: deploy {0}.sql
  command: psql -f /tmp/.pgbuild/run/{0}.sql -d {{{{cluster_name}}}} -p {{{{port}}}} --set=ON_ERROR_STOP=1
  sudo: yes
  sudo_user: postgres
""".format(self.number)


class CSVTask(object):
    def __init__(self, number, task_type, table, columns,
        copy_from, copy_format, delimiter, quote):
        self.number = number
        self.task_type = task_type
        self.table = table
        self.columns = columns
        self.copy_from = copy_from
        self.copy_format = copy_format
        self.delimiter = delimiter
        self.quote = quote

    @property
    def transfer_entry(self):
        return """
- name: transfer {0}.csv
  copy: src={0}.csv dest=/tmp/.pgbuild/run/
""".format(self.number)

    @property
    def shards_entry(self):
        return """
- name: deploy {number}.csv
  command: psql -c "\COPY {table} ({columns}) FROM '/tmp/.pgbuild/run/{number}.csv' (FORMAT '{copy_format}', DELIMITER '{delimiter}')" -d {{{{cluster_name}}}}{{{{'_%02d'|format(item)}}}} -p {{{{port}}}} --set=ON_ERROR_STOP=1
  sudo: yes
  sudo_user: postgres
""".format(
    number=self.number,
    table=self.table,
    columns=', '.join(self.columns),
    copy_format=self.copy_format,
    delimiter=self.delimiter
    )

    @property
    def basic_entry(self):
        return """
- name: deploy {number}.csv
  command: psql -c "\COPY {table} ({columns}) FROM '/tmp/.pgbuild/run/{number}.csv' (FORMAT '{copy_format}', DELIMITER '{delimiter}')" -d {{{{cluster_name}}}} -p {{{{port}}}} --set=ON_ERROR_STOP=1
  sudo: yes
  sudo_user: postgres
""".format(
    number=self.number,
    table=self.table,
    columns=', '.join(self.columns),
    copy_format=self.copy_format,
    delimiter=self.delimiter
    )

    @property
    def sql_content(self):
        return """
COPY {table} ({columns})
    FROM '{copy_from}'
    WITH
        {copy_format}
        DELIMITER '{delimiter}'
        QUOTE '{quote}';
""".format(
    number=self.number,
    table=self.table,
    copy_from=self.copy_from,
    columns=', '.join(self.columns),
    copy_format=self.copy_format,
    delimiter=self.delimiter,
    quote=self.quote
)
