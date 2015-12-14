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

    def to_string(self):
        return self.container_id + ', ' + self.container_addr

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

    def to_string(self):
        return self.spec_id

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

    def to_string(self):
        string = self.spec_id
        for container_id in self.container_ids:
            string += ', ' + container_id
        return string

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

    def to_string(self):
        string = self.container_id
        for spec_id in self.spec_ids:
            string += ', ' + spec_id
        return string

    def dump(self):
        print '[itf_database] Container: ' + self.container_id + ' -> ' \
                + str(self.spec_ids)


##### DB READ #####

# [container_list.txt]
# Get the complete container list
# Data entry: [container_id, container_addr, filename_in_db]
def get_container_record_list():
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

# [container_list.txt]
# Get the complete container id list
def get_container_id_list():
    container_list = get_container_record_list()
    container_id_list = []
    for record in container_list:
        container_id_list.append(record.container_id)
    return container_id_list

# [spec_list.txt]
# Get the complete spec list
# Data entry: [spec_id, filename_in_db]
def get_spec_record_list():
    # read and parse database/meta/spec_list.txt
    spec_list = []
    with open('database/meta/spec_list.txt', 'r') as f:
        data = f.read().splitlines()
        for line in data:
            record = SpecRecord()
            if record.parse(line) != False:
                spec_list.append(record)
    return spec_list

# [spec_list.txt]
# Get the complete spec id list
def get_spec_id_list():
    spec_list = get_spec_record_list()
    spec_id_list = []
    for record in spec_list:
        spec_id_list.append(record.spec_id)
    return spec_id_list

# [spec_to_container.txt]
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

# [container_to_spec.txt]
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

# [spec_to_container.txt]
# Given a spec_id, return all related container_ids
def get_container_ids_for_spec(spec_id):
    s2c_map = get_spec_to_container_map()
    for entry in s2c_map:
        if spec_id == entry.spec_id:
            return entry.container_ids
    return []

# [container_to_spec.txt]
# Given a container_id, return all related spec_ids
def get_spec_ids_on_container(container_id):
    c2s_map = get_container_to_spec_map()
    for entry in c2s_map:
        if container_id == entry.container_id:
            return entry.spec_ids
    return []

# [container/]
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

# [spec/]
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

# [container/]
# Update the status file in db
def update_container_status(new_status):
    # caller should make sure that new_status.StorageReserved is updated
    path = os.path.join('database/container/', new_status.ContainerId + '.txt')
    new_status.write_to_file(path)

# [spec/]
# Update a spec in db
def update_client_spec(new_spec):
    # caller should make sure that new_spec.UsedSize is updated
    path = os.path.join('database/spec/', new_spec.SpecId + '.txt')
    new_spec.write_to_file(path)

# [container_list.txt, container/]
# Add a new container to the database
def add_to_container_list(container_id, container_addr):
    # write to database/meta/container_list.txt
    container_list = get_container_record_list()
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
        string += record.to_string() + '\n'

    with open('database/meta/container_list.txt', 'w') as f:
        f.write(string)

    return True


# Input a list of SpecRecord, save to spec_list.txt
def save_spec_record_list(spec_record_list):
    string = ''
    for record in spec_record_list:
        string += record.to_string() + '\n'
    with open('database/meta/spec_list.txt', 'w') as f:
        f.write(string)


# [spec_to_container.txt, container_to_spec.txt, container/]
# Algorithm:
# 1. For a spec, compare existing containers and newly scheduled containers
# 2. If the spec is no longer on a container, remove it in container_to_spec,
#    and update the reserved size of the container
# 3. If the spec is newly scheduled to a container, add it to the map, and
#    update the reserved size of the container
# 4. For reschedule, call 3rd-party file transfer for used storage.
def update_map_for_spec(spec, new_container_ids):
    # Use two dict to represent the two-way mapping
    s2c_map = get_spec_to_container_map()
    c2s_map = get_container_to_spec_map()
    s2c_dict = {}
    c2s_dict = {}
    for record in s2c_map:
        s2c_dict[record.spec_id] = record.container_ids
    for record in c2s_map:
        c2s_dict[record.container_id] = record.spec_ids

    if spec.SpecId not in s2c_dict:
        # No existing files, so only update the mapping and reserved size
        s2c_dict[spec.SpecId] = new_container_ids
        for container_id in new_container_ids:
            # Add to container-to-spec map
            if container_id not in c2s_dict:
                c2s_dict[container_id] = [spec.SpecId]
            else:
                c2s_dict[container_id].append(spec.SpecId)
            # Update container status
            status = get_status(container_id)
            status.StorageReserved += spec.ReservedSize
            update_container_status(status)
            print '[itf_database] Container {' + container_id \
                    + '} reserved size increase ' + str(spec.ReservedSize) \
                    + ' MB'
    else:
        old_container_ids = s2c_dict[spec.SpecId]
        old_spec = get_spec(spec.SpecId)
        copy_source_id = 0

        for container_id in new_container_ids:
            if container_id in old_container_ids:
                # Assume file contents are not changed, only check reserved storage
                status = get_status(container_id)
                status.StorageReserved += spec.ReservedSize - old_spec.ReservedSize
                update_container_status(status)
                print '[itf_database] Container {' + container_id \
                        + '} reserved size increase ' \
                        + str(spec.ReservedSize - old_spec.ReservedSize) + ' MB'
            else:
                # Add to container-to-spec map
                if container_id not in c2s_dict:
                    c2s_dict[container_id] = [spec.SpecId]
                else:
                    c2s_dict[container_id].append(spec.SpecId)

                # Update container status
                status = get_status(container_id)
                status.StorageReserved += spec.ReservedSize
                update_container_status(status)
                print '[itf_database] Container {' + container_id \
                        + '} reserved size increase ' + str(spec.ReservedSize) \
                        + ' MB'

                # 3rd-party file copy from old containers
                print '[itf_database] 3rd-party file transfer from {' \
                        + old_container_ids[copy_source_id] + '} to {' \
                        + container_id + '} with ' + str(spec.UsedSize) + ' MB'
                copy_source_id = (copy_source_id + 1) % len(old_container_ids)

        for container_id in old_container_ids:
            if container_id not in new_contianer_ids:
                # Remove spec from those containers
                c2s_dict[container_id].remove(spec.SpecId)
                if len(c2s_dict[container_id]) == 0:
                    c2s_dict.pop(container_id, None)

                # Update container status
                status = get_status(container_id)
                status.StorageReserved -= spec.ReservedSize
                update_container_status(status)
                print '[itf_database] Container {' + container_id \
                        + '} reserved size increase -' \
                        + str(spec.ReservedSize) + ' MB'

    # Save to file
    string = ''
    for s_id in s2c_dict:
        c_ids = s2c_dict[s_id]
        string += s_id
        for c_id in c_ids:
            string += ', ' + c_id
        string += '\n'
    with open('database/meta/spec_to_container.txt', 'w') as f:
        f.write(string)

    string = ''
    for c_id in c2s_dict:
        s_ids = c2s_dict[c_id]
        string += c_id
        for s_id in s_ids:
            string += ', ' + s_id
        string += '\n'
    with open('database/meta/container_to_spec.txt', 'w') as f:
        f.write(string)


# [spec_list.txt, spec_to_container.txt, container_to_spec.txt, spec/]
# Add a new scheduled spec and containers into db
def add_scheduled_spec(spec, new_container_ids):
    # write spec_id to database/meta/spec_list.txt
    rescheduled = False
    spec_list = get_spec_record_list()
    for record in spec_list:
        if record.spec_id == spec.SpecId:
            rescheduled = True
            break
    if not rescheduled:
        record = SpecRecord()
        record.spec_id = spec.SpecId
        spec_list.append(record)
        save_spec_record_list(spec_list)
        print '[itf_database] Insert {' + spec.SpecId + '} to spec list'

    # Update spec_to_container.txt and container_to_spec.txt
    update_map_for_spec(spec, new_container_ids)

    # save spec to database/spec/
    update_client_spec(spec)
    print '[itf_database] Save spec to {database/spec/' + spec.SpecId + '.txt}'

# [spec_list.txt, spec_to_container.txt, container_to_spec.txt, spec/]
# Remove a spec from the database
def remove_spec(spec_id):
    spec = get_spec(spec_id)

    # remove from spec_list.txt
    spec_list = get_spec_record_list()
    for record in spec_list:
        if record.spec_id == spec_id:
            spec_list.remove(record)
    save_spec_record_list(spec_list)

    # remove from spec_to_container.txt and container_to_spec.txt
    # Use two dict to represent the two-way mapping
    s2c_map = get_spec_to_container_map()
    c2s_map = get_container_to_spec_map()
    s2c_dict = {}
    c2s_dict = {}
    for record in s2c_map:
        s2c_dict[record.spec_id] = record.container_ids
    for record in c2s_map:
        c2s_dict[record.container_id] = record.spec_ids

    if spec_id in s2c_dict:
        container_ids = s2c_dict[spec_id]
        for container_id in container_ids:
            status = get_status(container_id)
            status.StorageReserved -= spec.ReservedSize
            update_container_status(status)
            print '[itf_database] Container {' + container_id \
                    + '} reserved size decrease ' + str(spec.ReservedSize) \
                    + ' MB'
            c2s_dict[container_id].remove(spec_id)
            if len(c2s_dict[container_id]) == 0:
                c2s_dict.pop(container_id, None)
        s2c_dict.pop(spec_id, None)

        # Save to file
        string = ''
        for s_id in s2c_dict:
            c_ids = s2c_dict[s_id]
            string += s_id
            for c_id in c_ids:
                string += ', ' + c_id
            string += '\n'
        with open('database/meta/spec_to_container.txt', 'w') as f:
            f.write(string)

        string = ''
        for c_id in c2s_dict:
            s_ids = c2s_dict[c_id]
            string += c_id
            for s_id in s_ids:
                string += ', ' + s_id
            string += '\n'
        with open('database/meta/container_to_spec.txt', 'w') as f:
            f.write(string)

    # remove the spec file in spec/
    spec_path = os.path.join('database/spec/', spec_id + '.txt')
    if os.path.isfile(spec_path):
        os.remove(spec_path)

##### DB MISC #####

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
    container_list = get_container_record_list()
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
    spec_list = get_spec_record_list()
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

