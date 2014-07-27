import os
import shutil

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
            install_path = os.path.join(dest, role.name, 'templates', str(task.number)+'.sql')
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
