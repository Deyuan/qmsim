# QoS Database Accessing Interface
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 9, 2015

import os
import shutil
import itf_status
import itf_spec

##### Data Structures #####

# A container record: a line in the container_list.txt
class ContainerRecord:
    def __init__(self):
        self.container_id = ''
        self.container_addr = ''

    def parse(self, line):
        info = line.split('#')[0].split(',')
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
                + ' (addr:' + self.container_addr + ') [' \
                + self.container_id + '.txt]'

# A spec record: a line in the spec_list.txt
class SpecRecord:
    def __init__(self):
        self.spec_id = ''

    def parse(self, line):
        info = line.split('#')[0].strip()
        if info == '':
            return False
        self.spec_id = info

    def dump(self):
        print '[itf_database] Spec:' + self.spec_id + ' [' \
                + self.spec_id + '.txt]'

# A map from a spec to a list of containers
class SpecToContainers:
    def __init__(self):
        self.spec_id = ''
        self.container_ids = []

    def parse(self, line):
        info = line.split('#')[0].split(',')
        if len(info) < 2:
            return False
        self.spec_id = info[0].strip()
        for i in range(1, len(info)):
            self.container_ids.append(info[i].strip())

    def dump(self):
        print '[itf_database] Spec: ' + self.spec_id + ' -> ' \
                + str(self.container_ids)

# A map from a container to a list of specs
class ContainerToSpecs:
    def __init__(self):
        self.container_id = ''
        self.spec_ids = []

    def parse(self, line):
        info = line.split('#')[0].split(',')
        if len(info) < 2:
            return False
        self.container_id = info[0].strip()
        for i in range(1, len(info)):
            self.spec_ids.append(info[i].strip())

    def dump(self):
        print '[itf_database] Container: ' + self.container_id + ' -> ' \
                + str(self.spec_ids)


##### DB READ #####

# Get the complete container list
# Data entry: [container_id, container_addr, filename_in_db]
def get_container_list():
    # read and parse database/meta/container_list.txt
    container_list = []
    try:
        with open('database/meta/container_list.txt', 'r') as f:
            data = f.read().splitlines()
            for line in data:
                record = ContainerRecord()
                if record.parse(line) != False:
                    container_list.append(record)
                else:
                    print '[itf_database] cannot parse line: ' + line
    except:
        print '[itf_database] container_list.txt does not exist.'
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
            record = SpecRecord()
            if record.parse(line) != False:
                spec_list.append(record)
    return spec_list

# Get the complete spec id list
def get_spec_id_list():
    spec_list = get_spec_list()
    spec_id_list = []
    for record in spec_list:
        spec_id_list.append(record.spec_id)
    return spec_id_list

# Get the complete spec-to-container map
def get_spec_to_container_map():
    s2c_map = []
    with open('database/meta/spec_to_container.txt', 'r') as f:
        data = f.read().splitlines()
        for line in data:
            record = SpecToContainers()
            if record.parse(line) != False:
                s2c_map.append(record)
    return s2c_map

# Get the complete container-to-spec map
def get_container_to_spec_map():
    c2s_map = []
    with open('database/meta/container_to_spec.txt', 'r') as f:
        data = f.read().splitlines()
        for line in data:
            record = ContainerToSpecs()
            if record.parse(line) != False:
                c2s_map.append(record)
    return c2s_map

# Given a spec_id, return all related container_ids
def get_container_ids_for_spec(spec_id):
    s2c_map = get_spec_to_container_map()
    for entry in s2c_map:
        if spec_id == entry.spec_id:
            return entry.container_ids
    return []

# Given a container_id, return all related spec_ids
def get_spec_ids_on_container(container_id):
    c2s_map = get_container_to_spec_map()
    for entry in c2s_map:
        if container_id == entry.container_id:
            return entry.spec_ids
    return []

# Given a container_id, get container status
def get_status(container_id):
    path = os.path.join('database/container/', container_id + '.txt')
    if not os.path.isfile(path):
        return None
    status = itf_status.ContainerStatus()
    succ = status.read_from_file(path)
    if succ == True:
        return status
    else:
        return None

# Given a spec_id, get client qos spec
def get_spec(spec_id):
    path = os.path.join('database/spec/', spec_id + '.txt')
    if not os.path.isfile(path):
        return None
    spec = itf_spec.QosSpec()
    succ = spec.read_from_file(path)
    if succ == True:
        return spec
    else:
        return None

##### DB WRITE #####

# Add a new container to the database
def add_to_container_list(container_id, container_addr):
    # write to database/meta/container_list.txt
    container_list = get_container_list()
    for record in container_list:
        if record.container_id == container_id:
            print '[itf_database] Container ID ' + container_id \
                    + ' already exists.'
            return False

    record = ContainerRecord()
    record.container_id = container_id
    record.container_addr = container_addr
    container_list.append(record)

    string = ''
    for record in container_list:
        string += record.container_id + ', ' + record.container_addr + '\n'

    f = open('database/meta/container_list.txt', 'w')
    f.write(string)
    f.close()

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
    # caller should make sure that new_status.StorageReserved is updated
    path = os.path.join('database/container/', container_id + '.txt')
    new_status.write_to_file(path)

# Update a spec in db
def update_client_spec(spec_id, new_spec):
    # caller should make sure that new_spec.UsedSize is updated
    path = os.path.join('database/spec/', spec_id + '.txt')
    new_spec.write_to_file(path)

# Initialize the QoS database
def init():
    if not os.path.isdir('database'):
        os.mkdir('database')
    if not os.path.isdir('database/container'):
        os.mkdir('database/container')
    if not os.path.isdir('database/spec'):
        os.mkdir('database/spec')
    if not os.path.isdir('database/meta'):
        os.mkdir('database/meta')
    with open('database/container/readme.txt', 'w') as f:
        info = 'Container status files. Generated by qmsim.\n'
        f.write(info)
    with open('database/spec/readme.txt', 'w') as f:
        info = 'Client QoS spec files. Generated by qmsim.\n'
        f.write(info)
    with open('database/meta/readme.txt', 'w') as f:
        info = 'Meta data for QoS management. Generated by qmsim.\n'
        f.write(info)
    with open('database/meta/container_list.txt', 'w') as f:
        f.write('')
    with open('database/meta/container_to_spec.txt', 'w') as f:
        f.write('')
    with open('database/meta/spec_list.txt', 'w') as f:
        f.write('')
    with open('database/meta/spec_to_container.txt', 'w') as f:
        f.write('')

# Clean all contents in the QoS database
def clean():
    shutil.rmtree('database')
    init()

# Show the summary of the QoS database
def summary(verbose):
    print '--------------------'
    print '[itf_database] Container list:'
    container_list = get_container_list()
    if len(container_list) == 0:
        print 'Empty'
    else:
        for record in container_list:
            record.dump()
            if verbose:
                status = get_status(record.container_id)
                print status.to_string()

    print '--------------------'
    print '[itf_database] Spec list:'
    spec_list = get_spec_list()
    if len(spec_list) == 0:
        print 'Empty'
    else:
        for record in spec_list:
            record.dump()
            if verbose:
                spec = get_spec(record.spec_id)
                print spec.to_string()

    print '--------------------'
    print '[itf_database] Container-to-spec mapping:'
    container_map = get_container_to_spec_map()
    if len(container_map) == 0:
        print 'Empty'
    else:
        for record in container_map:
            record.dump()

    print '--------------------'
    print '[itf_database] Spec-to-container mapping:'
    spec_map = get_spec_to_container_map()
    if len(spec_map) == 0:
        print 'Empty'
    else:
        for record in spec_map:
            record.dump()

    print '--------------------'

# Only for testing
if __name__ == '__main__':
    print '[itf_database] QoS Database Accessing Interface.'
    summary()

