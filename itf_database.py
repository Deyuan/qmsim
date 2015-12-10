# QoS Database Accessing Interface
# Author: Deyuan Guo
# Date: Dec 9, 2015

import os
import itf_status
import itf_spec

##### Data Structures #####

# A container record: a line in the container_list.txt
class ContainerRecord:
    def __init__(self):
        self.container_id = ''
        self.container_addr = ''

    # parse a line
    def parse(self, s):
        info = s.split('#')[0].split(',')
        if len(info) != 2:
            return False
        info[0] = info[0].strip()
        info[1] = info[1].strip()
        if info[0] == '' or info[1] == '':
            return False
        self.container_id = info[0]
        self.container_addr = info[1]

    def dump(self):
        print '[itf_database] Container: id=' + self.container_id \
                + ' (addr:' + self.container_addr + ') [' + self.container_id + '.txt]'

# A spec record: a line in the spec_list.txt
class SpecRecord:
    def __init__(self):
        self.spec_id = ''

    # parse a line
    def parse(self, s):
        info = s.split('#')[0].strip()
        if info == '':
            return False
        self.spec_id = info

    def dump(self):
        print '[itf_database] Spec:' + self.spec_id + ' [' + self.spec_id + '.txt]'

##### DB READ #####

# Get the complete container list
# Data entry: [container_id, container_addr, filename_in_db]
def get_container_list():
    # read and parse database/meta/container_list.txt
    container_list = []
    with open('database/meta/container_list.txt', 'r') as f:
        data = f.read().splitlines()
        for line in data:
            entry = ContainerRecord()
            if entry.parse(line) != False:
                container_list.append(entry)
            else:
                print '[itf_database] cannot parse line: ' + line
    return container_list

# Get the complete container id list
def get_container_id_list():
    container_list = get_container_list()
    container_id_list = []
    for record in container_list:
        container_id_list.append(record.container_id)
    return container_id_list

# Get the complete spec list
# Data entry: [spec_id, filename_in_db]
def get_spec_list():
    # read and parse database/meta/spec_list.txt
    spec_list = []
    with open('database/meta/spec_list.txt', 'r') as f:
        data = f.read().splitlines()
        for line in data:
            entry = SpecRecord()
            if entry.parse(line) != False:
                spec_list.append(entry)
    return spec_list

# Get the complete spec id list
def get_spec_id_list():
    spec_list = get_spec_list()
    spec_id_list = []
    for record in spec_list:
        spec_id_list.append(record.spec_id)
    return spec_id_list

# Given a container_id, return all related spec_ids
def get_spec_ids_on_container(container_id):
    # read database/meta/container_to_spec.txt

    return []

# Given a spec_id, return all related container_ids
def get_container_ids_for_spec(spec_id):
    # read database/meta/spec_to_container.txt

    return []

# Given a container_id, get container status
def get_status(container_id):
    path = os.path.join('database/container/', container_id + '.txt')
    status = itf_status.ContainerStatus()
    succ = status.read_from_file(path)
    if succ == True:
        return status
    else:
        return None

# Given a spec_id, get client qos spec
def get_spec(spec_id):
    path = os.path.join('database/spec/', spec_id + '.txt')
    spec = itf_spec.QosSpec()
    succ = spec.read_from_file(path)
    if succ == True:
        return spec
    else:
        return None

##### DB WRITE #####

# Add a new container to the database
def add_new_container(container_addr):
    # write addr, filename, size to database/meta/container_list.txt

    # write to database/meta/container_to_spec.txt

    # wait for container-monitor to update status.txt
    return True

# Add a new scheduled spec and containers into db
def add_scheduled_spec(spec, containers):
    # write spec_id and filename to database/meta/spec_list.txt

    # save spec to a file

    # update spec_to_container.txt

    # update container_to_spec.txt

    # update container_list.txt for reservable size
    return

# Update the status file in db
def update_container_status(container_id, new_status):
    path = os.path.join('database/container/', container_id + '.txt')
    new_status.write_to_file(path)
    # TODO: do not overwrite reservable size

# Update a spec in db
def update_container_status(spec_id, new_spec):
    path = os.path.join('database/spec/', spec_id + '.txt')
    new_spec.write_to_file(path)
    # TODO: do not overwrite used size


if __name__ == '__main__':
    print '[itf_database] QoS Database Accessing Interface.'
    container_list = get_container_list()
    for record in container_list:
        record.dump()
        status = get_status(record.container_id)

    spec_list = get_spec_list()
    for record in spec_list:
        record.dump()
        spec = get_spec(record.spec_id)


