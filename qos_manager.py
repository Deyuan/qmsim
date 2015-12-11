# QoS Manager
# Managing all requests related to QoS
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 10, 2015

import sys
import monitor_container

# Wrapper for calling scheduler
def call_scheduler(spec, task):
    return

# Wrapper for calling container_monitor
def call_container_monitor(container_address, task):
    # insert record to db, etc.
    # notify container_monitor because container_monitor is always running
    # orelse we can run a new process to deal with this new request
    return

# Wrapper for receiving incoming request, currently we use command line arguments. This part can be socket interface
def get_request():
    if len(sys.argv) != 3:
        print 'Usage: qos_manager.py -command argument'
        print 'Commands:'
        print '    -new_container container_address'
        exit()
    command = sys.argv[1]
    argument = sys.argv[2]
    if command == '-new_spec' or command == '-update_spec' or command == '-reschedule':
        with open(argument, 'r') as f:
            info = f.read()
    else:
        info = argument

    # if we get request from Internet, we can get a command and some text information
    return command, info


# We will use command line to call QoS manager
# Example:
# qos_manager.py -new_spec client_spec.txt
# qos_manager.py -update_spec client_spec.txt
# qos_manager.py -reschedule client_spec.txt
# qos_manager.py -new_container container_network_address
# ...

if __name__ == '__main__':

    command, info = get_request()

    if command == '-new_spec':
        container_list = call_scheduler(info, task='new')
        if len(container_list) > 0:
            add_spec_to_db(info)
            update_mapping_in_db(info, container_list)
            authorize(client_id, container_list)
            exit_and_tell_client(container_address)
        else:
            exit_and_tell_client(fail)

    elif command == '-update_spec':
        prev_container_list = get_container_list_in_db(spec)
        container_list = call_scheduler(info, task='update')
        if len(container_list) > 0:
            update_spec_to_db(info)
            update_mapping_in_db(info, container_list)
            authorize(client_id, container_list)
            exit_and_tell_client(container_address)
            unauthorize(client_id, prev_container_list - container_list)
        else:
            exit_and_tell_client(fail)

    elif command == '-reschedule':
        prev_container_list = get_container_list_in_db(spec)
        # this reschedule can from QoS monitor or from client-side
        container_list = call_scheduler(info, task='reschedule')
        if len(container_list) > 0:
            update_mapping_in_db(info, container_list)
            authorize(client_id, container_list)
            exit_and_tell_client(container_address)
            unauthorize(client_id, prev_container_list - container_list)
        else:
            exit_and_do_nothing() #?

    elif command == '-new_container':
        print '[QoS Manager] Add a new container: ' + info
        # this request is from system admin when creating new container
        monitor_container.insert(info)  # insert status to db
        exit()

    else:
        print 'Invalid command', command

