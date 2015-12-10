# QoS Checker
# Input: a QoS spec id, and a list of scheduled contaier ids
# Output: If those containers together can satisfy the spec
# This part will be used by QoS monitor and scheduler
# Author: Chunkun Bo, Deyuan Guo
# Date: Dec 9, 2015

import itf_database

# Check if disk space is satisfied
# Rule: for every container, free space >= (client reserved - client used)
def check_space(spec, container_status):
    for status in container_status:
        if status.StorageReservable < spec.ReservedSize - spec.UsedSize:
            return False
    return True

# Reliability rule: All container together should satisfy the spec
def check_reliability(spec, container_status):
    spec_reliability = float('0.' + str(spec.Reliability))
    container_failure = 1.0
    for status in container_status:
        container_failure = container_failure * \
                (1 - float('0.' + str(status.StorageReliability)))
    if 1 - container_failure >= spec_reliability:
        return True
    else:
        return False

# Availability rule: All comtainer together should satisfy the spec
def check_availability(spec, container_status):
    spec_availability = float('0.' + str(spec.Availability))
    container_unavailable = 1.0
    for status in container_status:
        container_unavailable = container_unavailable * \
                (1 - float('0.' + str(status.ContainerAvailability)))
    if 1 - container_unavailable >= spec_availability:
        return True
    else:
        return False

# Bandwidth rule: Even if there are multiple replications, client only get 
# one container for the spec. Only check this prefered container.
def check_bandwidth(spec, container_status):
    if spec.Bandwidth == 'Low':
        return True
    else:
        BW_threshold = 5  # assume 5 MB/s is high enough
        prefered = container_status[0]
        free_RBW = prefered.StorageRBW - prefered.StorageRBW_dyn
        free_WBW = prefered.StorageWBW - prefered.StorageWBW_dyn
        if free_RBW >= BW_threshold and free_WBW >= BW_threshold:
            return True
        else:
            return False


# QoS Checker main entry: Check if a list of container can satisfy a spec
def check_satisfiability(spec_id, container_id_list):
    satisfied = True
    spec = itf_database.get_spec(spec_id)
    if len(container_id_list) == 0: # not scheduled
        return False
    container_status = []
    for container_id in container_id_list:
        status = itf_database.get_status(container_id)
        container_status.append(status)

    # Check disk space
    satisfied = satisfied and check_space(spec, container_status)
    if not satisfied:
        print '[QoS Checker] Disk space not satisfied for spec: ' + spec_id

    # Check reliability
    satisfied = satisfied and check_reliability(spec, container_status)
    if not satisfied:
        print '[QoS Checker] Reliability not satisfied for spec: ' + spec_id

    # Check availability
    satisfied = satisfied and check_availability(spec, container_status)
    if not satisfied:
        print '[QoS Checker] Availability not satisfied for spec: ' + spec_id

    # Check bandwidth
    satisfied = satisfied and check_bandwidth(spec, container_status)
    if not satisfied:
        print '[QoS Checker] Bandwidth not satisfied for spec: ' + spec_id

    return satisfied


if __name__ == '__main__':
    print '[QoS Checker] Should be called by QoS Monitor.'


