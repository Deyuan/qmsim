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

import itertools

import itf_database
import qos_checker


# Schedule heuristic 0: only check the storage size requirement, and pick one
def schedule_0(spec):
    scheduled_containers = []
    info = ''
    container_id_list = itf_database.get_container_id_list()
    container_id_list = [x for x in container_id_list if \
            qos_checker.check_space(spec, [itf_database.get_status(x)])]
    if len(container_id_list) > 0:
        container_id = container_id_list[0]
        status = itf_database.get_status(container_id)
        scheduled_containers.append(container_id)
        cost = status.CostPerGBMonth / 1024.0 * spec.ReservedSize
        info = '$%.2f/month' % cost
    return scheduled_containers, info


# Schedule heuristic 1: check storage, availability and reliability
def schedule_1(spec):
    scheduled_containers = []
    info = ''
    container_id_list_all = itf_database.get_container_id_list()
    # filter out some containers
    container_id_list = []
    for cid in container_id_list_all:
        status = itf_database.get_status(cid)
        if qos_checker.check_space(spec, [status]) and \
                status.StorageReliability > 0 and \
                status.ContainerAvailability > 0:
            container_id_list.append(cid)
    # check if k containers together can satisfy the spec
    for k in range(min(5, len(container_id_list))):
        cid_comb = list(itertools.combinations(container_id_list, k))
        for comb in cid_comb:
            cid_list = list(comb)
            status_list = [itf_database.get_status(x) for x in cid_list]
            if qos_checker.check_availability(spec, status_list) and \
                    qos_checker.check_reliability(spec, status_list):
                scheduled_containers = cid_list
                costs = [x.CostPerGBMonth for x in status_list]
                cost = sum(costs) / 1024.0 * spec.ReservedSize
                info = '$%.2f/month' % cost
                return scheduled_containers, info
    return scheduled_containers, info

# Chunkun: check storage, bandwidth, dataintegirty availability and reliability
def schedule_2(spec):
    scheduled_containers = []
    info = ''
    container_id_list_all = itf_database.get_container_id_list()
    # filter out some containers
    container_id_list = []
    for cid in container_id_list_all:
        status = itf_database.get_status(cid)
        if qos_checker.check_space(spec, [status]) and \
                qos_checker.check_dataintegrity(spec,[status]) and \
                status.StorageReliability > 0 and \
                status.ContainerAvailability > 0:
            container_id_list.append(cid)
    # check if k containers together can satisfy the spec
    for k in range(min(5, len(container_id_list))):
        cid_comb = list(itertools.combinations(container_id_list, k))
        for comb in cid_comb:
            cid_list = list(comb)
            status_list = [itf_database.get_status(x) for x in cid_list]
            if qos_checker.check_availability(spec, status_list) and \
                    qos_checker.check_reliability(spec, status_list) and \
                    qos_checker.check_bandwidth(spec, status_list) and \
                    qos_checker.check_latency(spec, status_list):
                scheduled_containers = cid_list
                costs = [x.CostPerGBMonth for x in status_list]
                cost = sum(costs) / 1024.0 * spec.ReservedSize
                info = '$%.2f/month' % cost
                return scheduled_containers, info
    return scheduled_containers, info

# Top level scheduler entry
# Input a QosSpec class instance, return a list of containers
def schedule (spec, task):
    print '[QoS Scheduler] Schedule spec {' + spec.SpecId + '} for task {' + \
            task + '}'

    # Pick a schedule strategy
    if task == 'new' or task == 'update':
        scheduled_containers, info = schedule_2(spec)
    else:
        scheduled_containers = []
        info = 'unsupported task'

    print '[QoS Scheduler] Schedule results: ' + str(scheduled_containers) \
            + ' {' + info + '}'
    return scheduled_containers, info


# Only for testing
if __name__ == '__main__':
    print "[QoS Scheduler] Schedule a QoS spec to a set of containers."

