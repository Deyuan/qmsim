# QoS Monitor
# This monitor will be triggered when the status of a container changed
# Author: Deyuan Guo
# Date: Dec 9, 2015

import qos_checker
import itf_database


# Monitor a QoS spec
def monitor_spec(spec_id):
    print '[QoS Monitor] Monitoring QoS spec: ' + spec_id
    container_id_list = itf_database.get_container_ids_for_spec(spec_id)
    satisfied = qos_checker.check_satisfiability(spec_id, container_id_list)
    if not satisfied:
        #TODO: call qos manager:
        # qos_manager -reschedule spec_path_in_db
        print '[QoS Monitor] Reschedule for unsatisfied spec: ' + spec_id

# Monitor QoS specs related to a container
def monitor_container(container_id):
    print '[QoS Monitor] Monitoring container: ' + container_id
    spec_id_list = itf_database.get_spec_ids_on_container(container_id)
    for spec_id in spec_id_list:
        monitor_spec(spec_id)


# Run from command line: Check all specs in database
if __name__ == '__main__':
    print '[QoS Monitor] Check all QoS specs in database.'
    spec_id_list = itf_database.get_spec_id_list()
    for spec_id in spec_id_list:
        monitor_spec(spec_id)


