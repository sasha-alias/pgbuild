import os
import yaml
import tables
import functions
import types


def load_from_file(path):

    ret = []
    content = '\n'.join(file(path).readlines())
    yaml_content = yaml.load(content)
    for role_name in yaml_content.keys():
        role = Role(role_name, yaml_content[role_name], os.path.dirname(os.path.abspath(path)))
        ret.append(role)
    return ret


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

class RoleError(Exception):
    pass

class Role(dict):

    def __init__(self, name, descriptor, relpath_start):
        self.name = name
        self.descriptor = descriptor
        self.relpath_start = relpath_start

    def build(self):

        ret = ''
        for item in self.descriptor:
            item_type = item.keys()[0]
            if item_type == 'schema':
                ret += 'CREATE SCHEMA IF NOT EXISTS %s;\n' % item[item_type]

            elif item_type == 'table':

                table_path = item[item_type]
                table_path = absrelpath(table_path, self.relpath_start)
                table = tables.Table.load_from_yaml_file(table_path)
                ret += table.create_clause()

            elif item_type == 'function':
                func_path = item[item_type]
                func_path = absrelpath(func_path, self.relpath_start)
                function = functions.Function.load_from_file(func_path)
                ret += function.script

            elif item_type == 'sql':
                ret += item[item_type].rstrip().rstrip(';')+';\n'

            elif item_type == 'type':
                item_path = item[item_type]
                item_path = absrelpath(item_path, self.relpath_start)
                custom_type = types.Type.load_from_yaml_file(item_path)
                ret += custom_type.drop_clause() + custom_type.create_clause()

            else:
                raise RoleError('Unknown role item type "%s"' % item_type)

        return ret
