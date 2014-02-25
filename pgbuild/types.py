import os
import yaml

class Type(object):
    """ Definition of custom database type """

    @classmethod
    def load_from_yaml_file(cls, path):
        lines = file(os.path.abspath(path)).readlines()
        type_yaml = ''.join(lines)
        return cls(type_yaml)


    def __init__(self, typedef):

        if isinstance(typedef, str):
            typedef = yaml.load(typedef)
        self.name = typedef['type']
        self.attributes = typedef['attributes']

    def create_clause(self):

        ret = 'CREATE TYPE %s AS (%s);\n'

        attrs = ''
        for attr in self.attributes:
            attr_name = attr.keys()[0]
            attr_type = attr[attr_name]
            attrs += '%s %s, ' % (attr_name, attr_type)
        attrs = attrs[:-2]

        ret = ret % (self.name, attrs)
        return ret

    def drop_clause(self):
        return 'DROP TYPE IF EXISTS %s CASCADE;\n' % self.name
