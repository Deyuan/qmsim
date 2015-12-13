# QoS Manager
# Managing all requests related to QoS
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 10, 2015

import sys
import monitor_container
import qos_scheduler
import itf_spec
import itf_database


# Wrapper for receiving incoming request, currently we use command line
# arguments. This part can be socket interface
def get_request():
    usage = 'Usage: python qos_manager.py -command argument\n'
    usage += 'Commands:\n'
    usage += '    -schedule spec_file_path\n'
    usage += '    -new_container container_address\n'
    usage += '    -show_database\n'
    usage += '    -show_database_verbose\n'
    usage += '    -clean_database\n'

    if len(sys.argv) == 2:
        command = sys.argv[1]
        if command == '-show_database' or command == '-clean_database' \
                or command == '-show_database_verbose':
            return command, ''
    elif len(sys.argv) == 3:
        command = sys.argv[1]
        argument = sys.argv[2]
        if command == '-schedule':
            # if we get request from Internet, we can get a command and some
            # text information
            with open(argument, 'r') as f:
                param = f.read()
        else:
            param = argument
        return command, param

    print '=== QoS Manager ===\nDeveloped by Deyuan Guo and Chunkun Bo.\n'
    print usage
    exit()


# We will use command line to call QoS manager
# Example:
# qos_manager.py -new_spec client_spec.txt
# qos_manager.py -update_spec client_spec.txt
# qos_manager.py -reschedule client_spec.txt
# qos_manager.py -new_container container_network_address
# ...

if __name__ == '__main__':
    command, info = get_request()

    print '[QoS Manager] Incoming request: ' + command
    if info != '':
        print 'Information:\n--------------------'
        print info + '\n--------------------'

    if command == '-schedule':
        spec = itf_spec.QosSpec()
        err = spec.parse_string(info)
        if not err:
            spec_db = itf_database.get_spec(spec.SpecId)
            if spec_db == None:
                print '[QoS Manager] Add and schedule new spec: ' + spec.SpecId
                scheduled_containers, info = qos_scheduler.schedule(spec, task='new')
            else:
                print '[QoS Manager] Reschedule for spec: ' + spec.SpecId
                scheduled_containers, info = qos_scheduler.schedule(spec, task='update')

            if len(scheduled_containers) > 0:
                add_spec_to_db(info)
                update_mapping_in_db(info, scheduled_contaienrs)
                authorize(client_id, scheduled_containers)
                exit_and_tell_client(container_address)
            else:
                print '[QoS Manager] Fail to schedule new spec: ' + spec.SpecId + \
                        ' [' + info + ']'
                exit(-1)
        else:
            print '[QoS Manager] Error: Invalid QoS spec.'

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

    elif command == '-show_database':
        print '[QoS Manager] QoS database summary:'
        itf_database.summary(verbose=False)

    elif command == '-show_database_verbose':
        print '[QoS Manager] QoS database summary:'
        itf_database.summary(verbose=True)

    elif command == '-clean_database':
        print '[QoS Manager] Clearing all contents in the QoS database?'
        prompt = raw_input('[y/n]:')
        if prompt == 'y' or prompt == 'Y':
            itf_database.clean()

    else:
        print 'Invalid command', command

