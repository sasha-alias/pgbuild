# pgbuild 
simple deployment for Postgresql

Pgbuild allows to describe database application using simple YAML syntax.

### Database Description

Every database objects should be described in a separate yaml or sql file.

Example of file describing a table:

    
    table: myschema.mytable
    description: table of tables
    columns:
        - col1:
            type: int
            default: 0
            not_null: true
            description: test
        - col2: text
        ...
    primary_key: [col1, col2]
    indexes:
        - idx1: [colN, colM]
        - idx2: (lower(colN || colM))
        - idx3:
            fields: [colN, colM]
            method: btree
            unique: true
    check:
        - check_col3: ...
        - ...

Description of custom types using yaml syntax:

    type: myschema.mytype
    attributes:
        - attr1: int
        - attr2: text
        ...

For stored functions it is even simpler, just store them in sql files with CREATE OR REPLACE statement

    CREATE OR REPLACE FUNCTION myschema.myfunction() 
    RETURNS void AS 
    $$
    BEGIN
        RETURN;
    END;
    $$
    LANGUAGE plpgsql

Now to put it all together:
    
    - myapp:
        - schema: myschema
        - table: path/to/mytable.yaml
        - type: path/to/mytype.yaml
        - function: path/to/myfunction.sql

### Objects Deployment and Diffing

In order to deploy a table described in yaml file to a single instance of database use the following command:

    pgbuild deploy path/to/mytable.yaml postgresq://user@host:port/dbname

This will drop a table if such exists and create new one according to description from file.


In order to take a look a DDL statement of table from yaml file execute:

    pgbuild ddl path/to/mytable.yaml

It's possible to print out the difference between two tables.

For example diff local table from file and existing from database:

    pgbuild diff path/to/mytable.yaml postgresql://user@host:port/dbname/myschema.mytable

The similar way by defining different targets it's possible to compare remote and local tables in any combination.

### Application or Component Deployment

In order to deploy a database application or a single component you have to describe it first using yaml syntax as described above.
Then you can create a build.

    pgbuild path/to/myapp.yaml local/destination/path

Build contains ready to deploy sql scripts.
By default scripts are created for being run with psql.

It's possible though to create playbooks for Ansible by defining a builder:

    pgbuild path/to/myapp.yaml local/destination/path --format=ansible

So you can deploy them either using psql or Ansible.


