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
    usage += '    -rm_spec spec_id\n'
    usage += '    -add_container container_address\n'
    usage += '    -show_db\n'
    usage += '    -show_db_verbose\n'
    usage += '    -destory_db\n'

    if len(sys.argv) == 2:
        return sys.argv[1], ''
    elif len(sys.argv) == 3:
        if sys.argv[1] == '-schedule':
            # if we get request from Internet, we can get a command and some
            # text information
            with open(sys.argv[2], 'r') as f:
                info = f.read()
        else:
            info = sys.argv[2]
        return sys.argv[1], info

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

    print '################################################################################'
    print '[QoS Manager] Incoming request: ' + command
    if info != '':
        print 'Information:\n--------------------'
        print info + '\n--------------------'

    if command == '-schedule':
        spec = itf_spec.QosSpec()
        err = spec.parse_string(info)
        if not err:
            spec_db = itf_database.get_spec(spec.SpecId)

            if spec_db == None: # spec does not exist
                print '[QoS Manager] Schedule for new spec {' + spec.SpecId + '}'
                scheduled_containers, info = qos_scheduler.schedule(spec, task='new')

                if len(scheduled_containers) > 0:
                    itf_database.add_scheduled_spec(spec, scheduled_containers)
                    print '[QoS Manager] Successfully scheduled {' + spec.SpecId + '}'

                else:
                    print '[QoS Manager] Fail to schedule new spec {' \
                            + spec.SpecId + '} {' + info + '}'
                    exit(-1)

            else: # reschedule for updated spec
                print '[QoS Manager] Reschedule for spec {' + spec.SpecId + '}'
                scheduled_containers, info = qos_scheduler.schedule(spec, task='update')

                if len(scheduled_containers) > 0:
                    itf_database.add_scheduled_spec(spec, scheduled_containers)
                    print '[QoS Manager] Successfully rescheduled {' + spec.SpecId + '}'

                else:
                    print '[QoS Manager] Fail to reschedule spec {' \
                            + spec.SpecId + '} {' + info + '}'
                    exit(-1)
        else:
            print '[QoS Manager] Error: Invalid QoS spec.'

    elif command == '-rm_spec':
        spec_id = info
        print '[QoS Manager] Remove spec {' + spec_id + '} from database'
        itf_database.remove_spec(spec_id)

    elif command == '-add_container':
        print '[QoS Manager] Add a new container: ' + info
        # this request is from system admin when creating new container
        monitor_container.insert(info)  # insert status to db

    elif command == '-show_db':
        print '[QoS Manager] QoS database summary:'
        itf_database.summary(verbose=False)

    elif command == '-show_db_verbose':
        print '[QoS Manager] QoS database summary:'
        itf_database.summary(verbose=True)

    elif command == '-destory_db':
        print '[QoS Manager] Clearing all contents in the QoS database?'
        prompt = raw_input('[y/n]:')
        if prompt == 'y' or prompt == 'Y':
            itf_database.clean()

    else:
        print 'Unsupported command: ', command

