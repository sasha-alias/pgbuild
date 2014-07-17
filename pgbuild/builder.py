import os

basic_role_tasks = """
- name: create .pgbuild/run directory
  file: path=/tmp/.pgbuild/run state=directory

- name: transfer install.sql
  copy: src=install.sql dest=/tmp/.pgbuild/run/install.sql

- name: deploy install.sql
  command: psql -f /tmp/.pgbuild/run/install.sql -d {{{{cluster_name}}}} -p {{{{port}}}} --set=ON_ERROR_STOP=1
  sudo: yes
  sudo_user: postgres

{jobs}

- name: delete .pgbuild/run directory
  file: path=/tmp/.pgbuild state=absent
"""

shard_role_tasks = """
- name: create .pgbuild/run directory
  file: path=/tmp/.pgbuild/run state=directory

- name: transfer install.sql
  copy: src=install.sql dest=/tmp/.pgbuild/run/install.sql

- name: run install.sql
  command: psql -f /tmp/.pgbuild/run/install.sql -d {{{{cluster_name}}}}{{{{'_%02d'|format(item)}}}} -p {{{{port}}}} --set=ON_ERROR_STOP=1
  with_items: hostvars[inventory_hostname].shards
  sudo: yes
  sudo_user: postgres

{jobs}

- name: delete .pgbuild/run directory
  file: path=/tmp/.pgbuild state=absent
"""



def ansible_build(role, dest):
    os.makedirs(os.path.join(dest, role.name, 'files'))
    os.makedirs(os.path.join(dest, role.name, 'tasks'))
    install_path = os.path.join(dest, role.name, 'files', 'install.sql')
    install = role.build()
    shard_tasks = inject_jobs(shard_role_tasks, role.jobs, shards=True)
    basic_tasks = inject_jobs(basic_role_tasks, role.jobs, shards=False)
    install_file = file(install_path, 'w')
    install_file.write(install.encode('utf8'))
    install_file.close()
    tasks_path = os.path.join(dest, role.name, 'tasks', 'main.yml')
    tasks_file = file(tasks_path, 'w')
    if role.name.endswith('_shard'):
        tasks_file.write(shard_tasks)
    else:
        tasks_file.write(basic_tasks)
    tasks_file.close()


def psql_build(role, dest):
    os.makedirs(os.path.join(dest, role.name))
    install_path = os.path.join(dest, role.name, 'install.sql')
    install = role.build()
    install_file = file(install_path, 'w')
    install_file.write(install.encode('utf8'))
    install_file.close()

def inject_jobs(tasks, jobs, shards):
    if shards:
        task = """
- cron: %s
  with_items: hostvars[inventory_hostname].shards
  sudo: yes
  sudo_user: postgres
"""
    else:
        task = """
- cron: %s
  sudo: yes
  sudo_user: postgres
"""

    ret = [task % j for j in jobs]
    ret = '\n\n'.join(ret)
    return tasks.format(jobs=ret)



builders = {
    'ansible': ansible_build,
    'psql': psql_build
}
