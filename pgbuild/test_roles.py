import roles
import yaml

descr = """
role1:
  - schema: myschema
  - table: path/to/table.yaml
  - function: path/to/function.sql
role2:
  - schema: myschema
  - proxy: path/to/function.sql
"""


def test_1():

    d = yaml.load(descr)
    for rname in d.keys():
        role = roles.Role(rname, d[rname])
        print role.build()
