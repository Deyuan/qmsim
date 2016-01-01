# QoS Manager
# Managing all requests related to QoS
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 10, 2015

import sys
import os
import monitor_container
import qos_scheduler
import itf_spec
import itf_database


# Define a class for QoS requests
class QosRequest:
    def __init__(self):
        self.cmd = ''
        self.data = ''
        self.from_cmdline = True
        self.status = 0

def print_help():
    print '=== QoS Manager ===\nDeveloped by Deyuan Guo and Chunkun Bo.\n'
    usage = 'Usage: python qos_manager.py -command argument\n'
    usage += 'Commands:\n'
    usage += '    -schedule [spec_file_path or spec_id]\n'
    usage += '    -rm_spec spec_id\n'
    usage += '    -add_container container_address\n'
    usage += '    -show_db\n'
    usage += '    -show_db_verbose\n'
    usage += '    -destroy_db\n'
    print usage

def get_request_socket():
    print "get request from socket"
    request = QosRequest()
    request.from_cmdline = False
    return request

def get_request_cmdline():
    if len(sys.argv) == 1:
        print_help()
        exit()
    request = QosRequest()
    if len(sys.argv) >= 2:
        request.cmd = sys.argv[1]
    if len(sys.argv) >= 3:
        request.data = sys.argv[2]
        if request.cmd == '-schedule':
            # new spec --> input arg: spec file path
            # update spec --> input arg: spec file path, or spec id
            # Finally we need to store spec.to_string() in request.data
            if os.path.isfile(sys.argv[2]):
                with open(sys.argv[2], 'r') as f:
                    request.data = f.read()
            else:
                spec_id = sys.argv[2]
                spec = itf_database.get_spec(spec_id)
                if spec is not None:
                    request.data = spec.to_string()
                else:
                    print '[QoS Manager] Unrecognized spec ID: ' + spec_id
                    request.cmd = ''
                    request.data = ''
    return request

# Main loop for QoS Server
def qos_server_main():
    print '[QoS Manager] QoS service started.'
    err = 0
    while err != 1:
        request = get_request_socket()
        err = process_request(request)

# Process all incoming QoS requests
def process_request(request):
    cmd = request.cmd
    data = request.data
    print '################################################################################'
    print '[QoS Manager] Incoming request: ' + cmd + ' from ' + \
            ('cmdline' if request.from_cmdline else 'socket')
    if data != '':
        print 'Data:\n--------------------'
        print data + '\n--------------------'

    if cmd == '-schedule':
        spec = itf_spec.QosSpec()
        err = spec.parse_string(data)
        if not err:
            spec_db = itf_database.get_spec(spec.SpecId)

            if spec_db == None: # spec does not exist
                print '[QoS Manager] Schedule for new spec {' + spec.SpecId + '}'
                scheduled_containers, data = qos_scheduler.schedule(spec, task='new')

                if len(scheduled_containers) > 0:
                    itf_database.add_scheduled_spec(spec, scheduled_containers, init=True)
                    print '[QoS Manager] Successfully scheduled {' + spec.SpecId + '}'

                else:
                    print '[QoS Manager] Fail to schedule new spec {' \
                            + spec.SpecId + '} {' + data + '}'
                    exit(-1)

            else: # reschedule for updated spec
                print '[QoS Manager] Reschedule for spec {' + spec.SpecId + '}'
                scheduled_containers, data = qos_scheduler.schedule(spec, task='update')

                if len(scheduled_containers) > 0:
                    itf_database.add_scheduled_spec(spec, scheduled_containers)
                    print '[QoS Manager] Successfully rescheduled {' + spec.SpecId + '}'

                else:
                    print '[QoS Manager] Fail to reschedule spec {' \
                            + spec.SpecId + '} {' + data + '}'
                    exit(-1)
        else:
            print '[QoS Manager] Error: Invalid QoS spec.'

    elif cmd == '-rm_spec':
        spec_id = data
        print '[QoS Manager] Remove spec {' + spec_id + '} from database'
        itf_database.remove_spec(spec_id)

    elif cmd == '-add_container':
        print '[QoS Manager] Add a new container: ' + data
        # this request is from system admin when creating new container
        monitor_container.insert(data)  # insert status to db

    elif cmd == '-show_db':
        print '[QoS Manager] QoS database summary:'
        itf_database.summary(verbose=False)

    elif cmd == '-show_db_verbose':
        print '[QoS Manager] QoS database summary:'
        itf_database.summary(verbose=True)

    elif cmd == '-destroy_db':
        print '[QoS Manager] Clearing all contents in the QoS database?'
        prompt = raw_input('[y/n]:')
        if prompt == 'y' or prompt == 'Y':
            itf_database.init()

    elif cmd == '-start_qos_server': # cmd line only
        if request.from_cmdline:
            qos_server_main()

    elif cmd == '-stop_qos_server': # socket only
        if not request.from_cmdline:
            return 1 # use this value for stop the server main loop

    else:
        print '[QoS Manager] Unsupported QoS request command: ', cmd
        return -1

    return 0


# QoS Manager Command Line Main Entry
if __name__ == '__main__':
    request = get_request_cmdline()
    process_request(request)

