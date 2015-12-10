# QoS Scheduler
# Schedule for a spec, return scheduled containers
# Author: Deyuan Guo
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

import qos_checker

# Top level scheduler entry ------ need to rewrite
def schedule (spec):
    container_list = []
    hlocation_list = get_client_location(spec)
    bandwidth_factor = 1.0

    # go higher and higher level of location
    while (hlocation.level > 0):
         container_list = schedule_containers_at_location(hlocation, bandwidth_factor)
         if container_list.length == 0:
             hlocation.level = hlocation.level - 1
             bandwidth_factor = bandwidth_factor * 0.8 # assume other locations have lower bandwidth
         else:
             break
    return container_list

# For a level of location, find best fit
def schedule_containers_at_location(hlocation, bandwidth_factor):
     container_candidates = get_container_list_at_location(hlocation)

     for container in container_candidates:
         satisfied = qos_checker.check_satisfiability(spec, container)

     # if no single container can satisfy, we may use multiple container to satisfy reliability
     # TODO

if __name__ == '__main__':
    print "[QoS Scheduler] Schedule a QoS spec to a set of containers."
