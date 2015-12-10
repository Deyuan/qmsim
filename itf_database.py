# QoS Database Accessing Interface
# Author: Deyuan Guo
# Date: Dec 9, 2015

import os
import itf_status

##### Data Structures #####

# A container record: a line in the container_list.txt
class ContainerRecord:
    def __init__(self):
        self.container_addr = ''
        self.filename = ''
        self.reservable_size = ''

    # parse a line
    def parse(self, s):
        info = s.split('#')[0].split(',')
        if len(info) != 3:
            return False
        for i in range(0, 3):
            info[i] = info[i].strip()
            if info[i] == '':
                return False
        self.container_addr = info[0]
        self.filename = info[1]
        self.reservable_size = info[2]

# A spec record: a line in the spec_list.txt
class SpecRecord:
    def __init__(self):
        self.spec_id = ''
        self.filename = ''

    # parse a line
    def parse(self, s):
        info = s.split('#')[0].split(',')
        if len(info) != 2:
            return False
        info[0] = info[0].strip()
        info[1] = info[1].strip()
        if info[0] == '' or info[1] == '':
            return False
        self.spec_id = info[0]
        self.filename = info[1]

##### DB READ #####

# Get the complete container_addr list
# Data entry: [container_addr, status_file_name_in_db, reservable_size]
def get_container_addrs():
    # read and parse database/meta/container_list.txt
    container_addr_list = []
    with open('database/meta/container_list.txt', 'r') as f:
        data = f.read().splitlines()
        for line in data:
            entry = ContainerRecord()
            if entry.parse(line) != False:
                container_addr_list.append(entry)
    return container_addr_list

# Get the complete spec_id list
# Data entry: [spec_id, spec_file_name_in_db]
def get_spec_ids();
    # read and parse database/meta/spec_list.txt
    spec_id_list = []
    with open('database/meta/spec_list.txt', 'r') as f:
        data = f.read().splitlines()
        for line in data:
            entry = SpecRecord()
            if entry.parse(line) != False:
                spec_id_list.append(entry)
    return spec_id_list

# Given a container_addr, return all related spec_id
def get_spec_ids_on_container(container_addr):
    # read database/meta/container_to_spec.txt

    return []

# Given a spec_id, return all related container_addr
def get_container_addrs_for_spec(spec_id):
    # read database/meta/spec_to_container.txt

    return []

# Given a container_addr, return status file name in db
def get_container_status_filename(container_addr):
    # read database/meta/container_list.txt and get status file path
    filename = None
    container_addrs = get_container_addrs()
    for addr in container_addrs:
        if container_addr == addr[0]:
            filename = addr[1]
            break;
    return filename

# Given a container_addr, get container status
def get_container_status(container_addr):
    filename = get_container_status_filename(container_addr)
    path = os.path.join('database/container/', filename)
    if filename != None:
        status = itf_status.ContainerStatus()
        status.read_from_file(path)
        return status
    else:
        return None

# Given a spec_id, return client qos spec file name in db
def get_spec_filename(spec_id):
    # read database/meta/spec_list.txt and get spec file path
    filename = None
    spec_ids = get_spec_ids()
    for sid in spec_ids:
        if sped_id == sid[0]:
            filename = sid[1]
            break;
    return filename

# Given a spec_id, get client qos spec
def get_spec(spec_id):
    filename = get_spec_filename(spec_id)
    if filename != None:
        # read path (database/spec/xxx.txt)
        #spec = QosSpec()
        #spec.read_in_db(filename)
        return 0 # a class
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

# Given a container_addr and new status class, update the status file in db
def udpate_container_status(container_addr, new_status):
    filename = get_container_status_filename(container_addr)
    path = os.path.join('database/container/', filename)
    new_status.write_to_file(path)

