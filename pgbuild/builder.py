import os
import shutil
import sys
role_tasks = """

- name: create .pgbuild/run directory
  file: path=/tmp/.pgbuild/run state=directory

{tasks}

- name: delete .pgbuild/run directory
  file: path=/tmp/.pgbuild state=absent
"""

def ansible_build(role, dest):
    if os.path.exists(os.path.join(dest, role.name)):
        shutil.rmtree(os.path.join(dest, role.name))
    os.makedirs(os.path.join(dest, role.name, 'templates'))
    os.makedirs(os.path.join(dest, role.name, 'files'))
    os.makedirs(os.path.join(dest, role.name, 'tasks'))

    entries = []
    for task in role.tasks:
        if task.task_type == 'copy':
            install_path = os.path.join(dest, role.name, 'files', str(task.number)+'.csv')
            shutil.copyfile(task.copy_from, install_path)
        else:
            install_path = os.path.join(dest, role.name, 'files', str(task.number)+'.sql')
            install_file = file(install_path, 'w')
            install_file.write(task.sql_content.encode('utf8'))
            install_file.close()

        entries.append(task.transfer_entry)
        if role.name.endswith('_shard'):
            entries.append(task.shards_entry)
        else:
            entries.append(task.basic_entry)

    tasks_path = os.path.join(dest, role.name, 'tasks', 'main.yml')
    tasks_file = file(tasks_path, 'w')
    tasks = role_tasks.format(tasks = ''.join(entries))
    tasks_file.write(tasks)
    tasks_file.close()


def psql_build(role, dest):
    if os.path.exists(os.path.join(dest, role.name)):
        shutil.rmtree(os.path.join(dest, role.name))
    os.makedirs(os.path.join(dest, role.name, 'templates'))
    os.makedirs(os.path.join(dest, role.name, 'files'))
    entries = []
    for task in role.tasks:
        fpath = os.path.join(dest, role.name, 'files', '{}.sql'.format(task.number))
        print fpath
        open(fpath, 'w').write(task.sql_content.encode('utf-8'))
        entries.append(fpath)
    install_sql = os.path.join(dest, role.name, 'install.sql')
    install_yaml = os.path.join(dest, role.name, 'install.yaml')
    open(install_sql, 'w').write(';\n'.join(["\i '{}'".format(e) for e in entries]) + ';\n')
    open(install_yaml, 'w').write('\n'.join([" - '{}'".format(e) for e in entries]))

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
