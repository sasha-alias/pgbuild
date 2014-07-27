import tables

str_table1 = """
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
primary_key: [col1]
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
    - col1_check: col1 > 0 and col1 < 100
"""

str_table2 = """
table: myschema.mytable
description: table of tables
columns:
    - col1:
        type: int
        default: 0
        not_null: true
        description: test
    - name: col3
      type: text
      default: ""
    - col2: text
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
"""

ddl1 = """CREATE TABLE myschema.mytable (
    col1 int DEFAULT 0 NOT NULL,
    col2 text,
    col3 text DEFAULT ''
);
ALTER TABLE myschema.mytable ADD PRIMARY KEY (col1);
COMMENT ON TABLE myschema.mytable IS 'table of tables';
COMMENT ON COLUMN myschema.mytable.col1 IS 'test';
CREATE INDEX CONCURRENTLY idx1 ON myschema.mytable USING btree
    (col1, col2);
CREATE INDEX CONCURRENTLY idx2 ON myschema.mytable USING btree
    ((lower(col1 || col2)));
CREATE UNIQUE INDEX CONCURRENTLY idx3 ON myschema.mytable USING btree
    (col1, col2)
    WHERE blabla;
CREATE INDEX CONCURRENTLY idx4 ON myschema.mytable USING gin
    (col1, col2);
ALTER TABLE myschema.mytable ADD CONSTRAINT col1_check CHECK (col1 > 0 and col1 < 100);
"""


def test_1():

    table = tables.Table(str_table1)

    assert table.create_clause() == ddl1


def test_2():

    table1 = tables.Table(str_table1)
    table2 = tables.Table(str_table2)

    assert table1.columns == table2.columns
    assert not table1.columns != table2.columns


def test_3():

    table1 = tables.Table(str_table1)
    table2 = tables.Table(str_table2)
    table2.columns.append(tables.Column(name='aaaa', type='text'))

    assert not table1.columns == table2.columns
    assert table1.columns != table2.columns


def test_4():

    st1 = """
table: my.table
columns:
    - col1: text
    - col2: int
    - col3: text
primary_key: [col2]
indexes:
    - idx1: col1
"""

    st2 = """
table: my.table
columns:
    - col1: text
    - col2: text
    - col4:
        type: date
        description: some date
primary_key: [col1, col2]
indexes:
    - idx2: col2
"""

    expected = """ALTER TABLE my.table ALTER COLUMN col2 TYPE text;
ALTER TABLE my.table DROP COLUMN IF EXISTS col3;
ALTER TABLE my.table ADD COLUMN col4 date;
COMMENT ON COLUMN my.table.col4 IS 'some date';
DROP INDEX CONCURRENTLY IF EXISTS my.idx1;
CREATE INDEX CONCURRENTLY idx2 ON my.table USING btree
    (col2);
ALTER TABLE my.table DROP table_pkey;
ALTER TABLE my.table ADD PRIMARY KEY (col1, col2);
"""
    t1 = tables.Table(st1)
    t2 = tables.Table(st2)
    assert expected == t1.alter_to(t2)


