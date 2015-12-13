# QoS Scheduler
# Schedule for a spec, return scheduled containers
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 10, 2015

# some factors:
#  - task:
#            "new" - just looking for available containers for a spec
#            "update" - update qos spec, and consider actual_file_size
#            "reschedule" - consider actual_file_size
#            #update and reschedule are similar.
#  - bandwidth_factor: when searching higher level of location, bandwidth gets lower
#  - latency_factor: when searching higher level of location, latency gets higher
#  - actual_file_size: How much space the client is using. Large size will significantly slow down rescheduling. For update and reschedule, we need to consider this factor, e.g. prefer to reschedule small size

import itf_database
import qos_checker


# Schedule heuristic 0: only check the storage size requirement, and pick one
def schedule_0(spec):
    scheduled_containers = []
    info = ''
    container_id_list = itf_database.get_container_id_list()
    for container_id in container_id_list:
        status = itf_database.get_status(container_id)
        free_space = status.StorageTotal \
                - max(status.StorageReserved, status.StorageUsed)
        if free_space > spec.ReservedSize:
            scheduled_containers.append(container_id)
            cost = status.CostPerGBMonth / 1024.0 * spec.ReservedSize
            info = '$%.2f/month' % cost
            break
    return scheduled_containers, info


# Basic schedule strategy, 
def schedule_1(spec):
    scheduled_containers = []
    info = 'unimplemented'
    return scheduled_containers, info


# For a level of location, find best fit
def schedule_containers_at_location(hlocation, bandwidth_factor):
     container_candidates = get_container_list_at_location(hlocation)

     for container in container_candidates:
         satisfied = qos_checker.check_satisfiability(spec, container)

     # if no single container can satisfy, we may use multiple container to satisfy reliability
     # TODO

# Schedule with hierarchical location
def schedule_2(spec):
    container_list = []
    info = ''
    #hlocation_list = get_client_location(spec)
    bandwidth_factor = 1.0
    latency_factor = 1.0
    # go higher and higher level of location
    while (hlocation.level > 0):
         container_list = schedule_containers_at_location(hlocation, bandwidth_factor)
         if container_list.length == 0:
             hlocation.level = hlocation.level - 1
             bandwidth_factor = bandwidth_factor * 0.8 # assume other locations have lower bandwidth
         else:
             break
    return container_list, info


# Top level scheduler entry
# Input a QosSpec class instance, return a list of containers
def schedule (spec, task):
    print '[QoS Scheduler] Schedule spec {' + spec.SpecId + '} for task {' + \
            task + '}'

    # Pick a schedule strategy
    if task == 'new':
        scheduled_containers, info = schedule_0(spec)
    else:
        scheduled_containers = []
        info = 'unsupported task'

    print '[QoS Scheduler] Schedule results: ' + str(scheduled_containers) \
            + ' {' + info + '}'
    return scheduled_containers, info


# Only for testing
if __name__ == '__main__':
    print "[QoS Scheduler] Schedule a QoS spec to a set of containers."

