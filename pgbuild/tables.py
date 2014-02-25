#!/usr/bin/python
"""
Postgresql tables definition in YAML.

Definition example:

    table: myschema.mytable
    description: table of tables
    columns:
        - col1:
            type: int
            default: 0
            not_null: true
            description: test
        - col2: text
        - name: col3
          type: text
          default: ""
    primary_key: [col1, col2]
    indexes:
        - idx1: [col1, col2]
        - idx2: (lower(col1 || col2))
        - idx3:
            fields: [col1, col2]
            method: btree
            predicate: blabla
            unique: true
        - name: idx4
          fields: [col1, col2]
          method: gin
    check:
        - check_col3: ...
        - ...
"""
import sys
import os
import yaml
import psycopg2
from psycopg2.extensions import adapt

query_table_info = """
SELECT description
FROM pg_description
WHERE objoid = %s::regclass
AND objsubid = 0;
"""

query_columns_info = """
SELECT
    a.attname "name",
    a.atttypid::regtype "type",
    a.attnotnull "not_null",
    a.atthasdef "has_default",
    c.adsrc "default_value",
    b.description "description"
FROM pg_attribute a
    LEFT JOIN pg_description b
        ON b.objoid = a.attrelid AND b.objsubid = a.attnum
    LEFT JOIN pg_attrdef c
        ON c.adrelid = a.attrelid AND c.adnum = a.attnum
WHERE a.attrelid = %s::regclass
    AND a.attnum > 0 ;
"""

query_pk_info = """
SELECT conname, conindid::regclass, array_agg(b.attname ORDER BY attnum)
FROM pg_constraint a
JOIN pg_attribute b ON b.attrelid = a.conindid
WHERE conrelid = %s::regclass
AND contype = 'p'
GROUP BY conname, conindid;
"""

query_check_info = """
SELECT conname, consrc
FROM pg_constraint
WHERE conrelid = %s::regclass
AND contype = 'c';
"""

query_indexes_info = """
SELECT
    a.indexrelid::regclass "name",
    a.indisunique "unique",
    d.amname "method",
    array_agg(pg_get_indexdef(a.indexrelid, b.attnum, TRUE) ORDER BY b.attnum) "fields",
    pg_get_expr(a.indpred, a.indrelid, TRUE) predicate,
    pg_get_indexdef(a.indexrelid, 0, TRUE) indexdef
FROM
    pg_index a,
    pg_attribute b,
    pg_class c,
    pg_am d
WHERE
        a.indrelid = %s::regclass
    AND NOT a.indisprimary
    AND b.attrelid = a.indexrelid
    AND c.OID = a.indexrelid
    AND d.OID = c.relam
GROUP BY a.indexrelid, a.indisunique, d.amname, a.indrelid, a.indpred;
"""


def split_name(name):
    """ Split schema qualified name into tuple of schema name and object name """
    split = name.split('.')
    if len(split) == 2:
        schema = split[0]
        name = split[1]
    else:
        schema = None
        name = ".".join(split[1:])

    return (schema, name)


class YamlTableError(Exception):
    pass


class _DBObject(object):
    """ Two DBObjects are the same if their properties are equal """

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(frozenset(self.__dict__))


class Column(_DBObject):
    """ Table column """

    @classmethod
    def load_from_yaml(cls, origin_yaml):
        column_name = None
        column_type = None
        column_default = None
        column_not_null = False
        column_description = None

        if len(origin_yaml.keys()) == 1:
            column_name = origin_yaml.keys()[0]
            if isinstance(origin_yaml[column_name], str):  # - col: text
                column_type = origin_yaml[column_name]
            elif isinstance(origin_yaml[column_name], dict):  # - col: {type: text, default: '', ...}
                column_type = origin_yaml[column_name]['type']
                column_default = origin_yaml[column_name].get('default', column_default)
                column_not_null = origin_yaml[column_name].get('not_null', column_not_null)
                column_description = origin_yaml[column_name].get('description', column_description)
            column = cls(column_name, column_type, column_default, column_not_null, column_description)
        else:  # - {name: col, type: text, ...}
            column = cls(**origin_yaml)

        return column

    @staticmethod
    def adjust_value(value, dtype):
        """
        Adjust value to SQL type (embrace text to quotes and escape inline quotes)
        """
        text_types = ['character varying', 'varchar', 'character', 'char', 'text', 'hstore', 'bit varying']
        dtype = dtype[0:dtype.find('(')].rstrip() if dtype.find('(') > 0 else dtype  # remove length spec
        if dtype.lower() in text_types:
            return adapt(str(value))
        else:
            return value

    def __init__(self, name, type, default=None, not_null=False, description=None):
        self.name = name
        self.type = type
        self.default = default
        self.not_null = not_null
        self.description = description

    def __repr__(self):
        return str({
            'name': self.name,
            'type': self.type,
            'default': self.default,
            'not_null': self.not_null
        })

    def create_clause(self):
        ret = 4*' ' + self.name + ' ' + self.type
        if self.default is not None:
            ret += ' DEFAULT %s' % self.adjust_value(self.default, self.type)
        if self.not_null:
            ret += ' NOT NULL'

        return ret

    def alter_to(self, table_name, other):

        alter_column = "ALTER TABLE %s ALTER COLUMN %s " % (table_name, self.name)

        statements = ''
        if self.type != other.type:
            statements += alter_column + "TYPE %s;\n" % other.type
        if self.default != other.default:
            if other.default is not None:
                statements += alter_column + "SET DEFAULT %s;\n" % other.default
            else:
                statements += alter_column + "DROP DEFAULT;\n" % other.default
        if self.not_null != other.not_null:
            if other.not_null:
                statements += alter_column + "SET NOT NULL;\n"
            else:
                statements += alter_column + "DROP NOT NULL;\n"
        if self.description != other.description:
            statements += "COMMENT ON COLUMN %s.%s IS '%s';\n" % (table_name, self.name, other.description)

        return statements

    def drop_clause(self, table_name):
        return "ALTER TABLE %s DROP COLUMN IF EXISTS %s;\n" % (table_name, self.name)

    def add_clause(self, table_name):
        statements = "ALTER TABLE %s ADD COLUMN %s %s;\n" % (table_name, self.name, self.type)
        alter_column = "ALTER TABLE %s ALTER COLUMN %s " % (table_name, self.name)
        if self.default:
            statements += alter_column + "SET DEFAULT %s;\n" % self.default
        if self.not_null:
            statements += alter_column + "SET NOT NULL;\n"
        if self.description:
            statements += "COMMENT ON COLUMN %s.%s IS '%s';\n" % (table_name, self.name, self.description)

        return statements


class Index(_DBObject):
    """ Index on table """

    @classmethod
    def load_from_yaml(cls, table, origin_yaml):
        # default values
        index_name = None
        index_method = 'btree'
        index_fields = []
        index_unique = False
        index_predicate = None

        if len(origin_yaml.keys()) == 1:
            index_name = origin_yaml.keys()[0]
            if isinstance(origin_yaml[index_name], str):  # - idx: column1
                index_fields = [origin_yaml[index_name]]
            elif isinstance(origin_yaml[index_name], list):  # - idx: [column1, column2, (lower(column3))]
                index_fields = origin_yaml[index_name]
            elif isinstance(origin_yaml[index_name], dict):  # - idx: {fields: [...], method: ...}
                index_method = origin_yaml[index_name].get('method', origin_yaml[index_name].get('access_method', index_method))
                index_fields = origin_yaml[index_name].get('fields', index_fields)
                index_unique = origin_yaml[index_name].get('unique', index_unique)
                index_predicate = origin_yaml[index_name].get('predicate', index_predicate)
            index = cls(table, index_name, index_method, index_fields, index_unique, index_predicate)
        else:  # - {name: idx, method: btree, ...}
            origin_yaml['method'] = origin_yaml.get('method', origin_yaml.pop('access_method', index_method))
            index = cls(table, **origin_yaml)

        return index

    def __init__(self, table, name, method='btree', fields=[], unique=False, predicate=None):
        self.table = table
        self.name = name
        self.method = method
        self.fields = fields
        self.unique = unique
        self.predicate = predicate
        self.dict = {
            'table': self.table,
            'name': self.name,
            'method': self.method,
            'fields': self.fields,
            'unique': self.unique,
            'predicate': self.predicate
        }

    def __repr__(self):
        return str(self.dict)

    def create_clause(self):
        unique = ' UNIQUE ' if self.unique else ' '
        ret = 'CREATE%sINDEX ' % unique
        ret += '%(name)s ON %(table)s USING %(method)s\n' % self.dict
        fields = ', '.join(self.fields)
        ret += '    (' + fields + ')'
        if self.predicate is not None:
            ret += '\n    WHERE ' + self.predicate
        ret += ';\n'
        return ret

    def drop_clause(self):
        schema = split_name(self.table)[0]
        if schema:
            name = '%s.%s' % (schema, self.name)
        else:
            name = self.name
        ret = 'DROP INDEX CONCURRENTLY IF EXISTS %s;\n' % name
        return ret


class Check(_DBObject):
    """ Check constraint """

    @classmethod
    def load_from_yaml(cls, table, original_yaml):
        name = original_yaml.keys()[0]
        expression = original_yaml[name]

        return cls(table, name, expression)

    def __init__(self, table, name, expression):
        self.table = table
        self.name = name
        self.expression = expression

    def create_clause(self):
        ret = "ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);\n" % (self.table, self.name, self.expression)
        return ret

    def drop_clause(self):
        ret = "ALTER TABLE %s DROP CONSTRAINT %s;\n" % (self.table, self.name)
        return ret


class ColumnsList(list):

    def create_clause(self):
        ret = ''
        for c in self:
            ret += c.create_clause() + ',\n'
        ret = ret[0:-2]
        return ret

    def comments_clause(self, table_name):
        ret = ''
        for c in self:
            if c.description is not None:
                ret += "COMMENT ON COLUMN %s.%s IS '%s';\n" % (table_name, c.name, c.description)
        return ret

    def __eq__(self, other):

        return set(self) == set(other)  # columns order doesn't matter when comparing

    def __ne__(self, other):

        return not set(self) == set(other)

    def has_column(self, column):

        if isinstance(column, Column):
            search_name = column.name
        if isinstance(column, str):
            search_name = column
        ret = [c for c in self if c.name == search_name]

        return len(ret) > 0

    def get_column(self, column):

        if isinstance(column, Column):
            search_name = column.name
        if isinstance(column, str):
            search_name = column
        ret = [c for c in self if c.name == search_name]

        if len(ret) > 0:
            return ret[0]
        else:
            return None


class IndexesList(list):

    def create_clause(self):
        ret = ''
        for i in self:
            ret += i.create_clause()
        return ret

    def drop_clause(self):
        ret = ''
        for i in self:
            ret += i.drop_clause()
        return ret

    def has_index(self, index):

        if isinstance(index, Index):
            search_name = index.name
        if isinstance(index, str):
            search_name = index
        ret = [c for c in self if c.name == search_name]

        return len(ret) > 0


class ConstraintsList(list):

    def create_clause(self):
        ret = ''
        for c in self:
            ret += c.create_clause()
        return ret


class Table(object):

    @classmethod
    def load_from_yaml_file(cls, filepath):
        lines = file(os.path.abspath(filepath)).readlines()
        table = '\n'.join(lines)
        return cls(table)

    @classmethod
    def load_from_connection(cls, connection, table_name):
        """
        connection - open DBAPI2 connection
        table_name - name of a table to make instance of
        """

        cur = connection.cursor()
        cur.execute(query_table_info, (table_name,))
        table_info = cur.fetchone()

        cur = connection.cursor()
        cur.execute(query_columns_info, (table_name,))
        columns_info = cur.fetchall()

        cur = connection.cursor()
        cur.execute(query_pk_info, (table_name,))
        pk_info = cur.fetchone()

        cur = connection.cursor()
        cur.execute(query_check_info, (table_name,))
        check_info = cur.fetchall()

        cur = connection.cursor()
        cur.execute(query_indexes_info, (table_name,))
        indexes_info = cur.fetchall()

        columns = []
        for c in columns_info:
            col_name = c[0]
            col_type = c[1]
            col_not_null = c[2]
            col_has_default = c[3]
            col_default_value = c[4]
            col_description = c[5]
            if col_has_default:
                col_dict = {
                    'name': col_name,
                    'type': col_type,
                    'not_null': col_not_null,
                    'default': col_default_value,
                    'description': col_description
                }
            else:
                col_dict = {
                    'name': col_name,
                    'type': col_type,
                    'not_null': col_not_null,
                    'description': col_description
                }
            columns.append(col_dict)

        indexes = []
        for i in indexes_info:
            ind_dict = {
                'name': i[0],
                'unique': i[1],
                'method': i[2],
                'fields': i[3]
            }
            indexes.append(ind_dict)

        if pk_info is None:
            pk_info = []
        else:
            pk_info = pk_info[2]

        table = {
            'table': table_name,
            'description': table_info[0],
            'columns': columns,
            'indexes': indexes,
            'primary_key': pk_info,
            'check': [{c[0]:c[1]} for c in check_info]
        }

        return cls(table)

    @classmethod
    def load_from_location(cls, location):
        if location.startswith('postgresql://'):
            connstr, table_name = location.rsplit('/', 1)
            conn = psycopg2.connect(connstr)
            table = cls.load_from_connection(conn, table_name)
        else:
            table = cls.load_from_yaml_file(location)
        return table

    def create_on_connection(self, connection):
        cur = connection.cursor()
        cur.execute(self.create_clause())
        cur.close()

    def drop_on_connection(self, connection):
        cur = connection.cursor()
        cur.execute(self.drop_clause())
        cur.close()

    def load_from_pgdump(cls, dumppath, table_name):
        # TODO: implement
        pass

    def load_from_git(cls, filepath, revision):
        # TODO: implement
        pass

    def __init__(self, table):
        """
        table yaml respresentation representation with following keys:
            table,
            description,
            columns,
            primary_key,
            indexes,
            check
        """

        self.name = None
        self.description = None
        self.columns = []
        self.primary_key = []
        self.indexes = []
        self.check = []

        self._load(table)

    def _load(self, table):

        if isinstance(table, str):
            origin_yaml = yaml.load(table)
        elif isinstance(table, dict):
            origin_yaml = table
        else:
            raise YamlTableError("Unknown table representation type, dict or str expected")

        self.name = origin_yaml['table']
        self.description = origin_yaml.get('description')

        # load columns
        cols = [Column.load_from_yaml(c) for c in origin_yaml.get('columns', self.columns)]
        self.columns = ColumnsList(cols)

        # load indexes
        idxs = [Index.load_from_yaml(self.name, i) for i in origin_yaml.get('indexes', self.indexes)]
        self.indexes = IndexesList(idxs)

        # primary key
        cols = [self.columns.get_column(c) for c in origin_yaml.get('primary_key', self.primary_key)]
        self.primary_key = ColumnsList(cols)

        # check constraints
        cons = [Check.load_from_yaml(self.name, c) for c in origin_yaml.get('check', self.check)]
        self.check = ConstraintsList(cons)

    def __repr__(self):
        return str({
            'name': self.name,
            'description': self.description,
            'columns': self.columns,
            'indexes': self.indexes,
            'primary_key': self.primary_key,
            'check': self.check
        })

    def create_clause(self):

        create_clause = "CREATE TABLE IF NOT EXISTS %s (\n%s\n);\n" % (self.name, self.columns.create_clause())

        if self.primary_key:
            pk_clause = "ALTER TABLE %s ADD PRIMARY KEY (%s);\n" % (self.name, ', '.join(c.name for c in self.primary_key))
        else:
            pk_clause = ""

        comments_clause = ''
        if self.description is not None:
            comments_clause += "COMMENT ON TABLE %s IS '%s';\n" % (self.name, self.description)

        comments_clause += self.columns.comments_clause(self.name)

        indexes_clause = self.indexes.drop_clause() + self.indexes.create_clause()

        check_clause = self.check.create_clause()

        return create_clause + pk_clause + comments_clause + indexes_clause + check_clause

    def alter_to(self, other):
        """ Return alter script for getting own state to other """

        statements = ''
        for column in self.columns:
            if column in other.columns:  # column is the same
                pass
            elif other.columns.has_column(column):  # column differs
                statements += column.alter_to(self.name, other.columns.get_column(column))
            else:  # column doesn't exist
                statements += column.drop_clause(self.name)

        for other_column in other.columns:
            if not self.columns.has_column(other_column):  # column to be added
                statements += other_column.add_clause(self.name)

        for index in self.indexes:  # drop or recreate existing indexes
            if index in other.indexes:
                pass
            elif other.indexes.has_index(index):
                statements += index.drop_clause()
                statements += index.create_clause()
            else:
                statements += index.drop_clause()

        for other_index in other.indexes:  # create new indexes
            if not self.indexes.has_index(other_index):
                statements += other_index.create_clause()

        # primary key
        if self.primary_key != other.primary_key:
            statements += "ALTER TABLE %s DROP %s_pkey;\n" % (self.name, split_name(self.name)[1])
            statements += "ALTER TABLE %s ADD PRIMARY KEY (%s);\n" % (self.name, ', '.join(c.name for c in other.primary_key))

        # description
        if self.description != other.description:
            statements += "COMMENT ON TABLE %s IS '%s';\n" % (self.name, other.description)

        return statements

    def drop_clause(self):
        return "DROP TABLE IF EXISTS %s CASCADE;\n" % self.name


if __name__ == '__main__':

    if len(sys.argv) == 2:  # one table source shows CREATE statement
        table = Table.load_from_location(sys.argv[1])
        print table.create_clause()
    if len(sys.argv) == 3:  # two table sources shows ALTER 1st to 2nd
        table1 = Table.load_from_location(sys.argv[1])
        table2 = Table.load_from_location(sys.argv[2])
        print table1.alter_to(table2)
    if len(sys.argv) == 4:  # 1st arg is a command and next are table file and connection URL
        if sys.argv[1] == 'deploy':
            conn_uri = sys.argv[3].rstrip('/')
            table = Table.load_from_location(sys.argv[2])
            conn = psycopg2.connect(conn_uri)
            table.drop_on_connection(conn)
            table.create_on_connection(conn)
            conn.commit()
            print 'deployed at %s' % conn_uri + '/' + table.name
        else:
            print 'unknown command %s' % sys.argv[1]
