# Status Monitor
# This monitor is a deamon on container server to udpate status.txt
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 10, 2015

import os
import time
import itf_status


# Update the status.txt for a container
def update_status(container_addr):
    status = itf_status.ContainerStatus()
    path = os.path.join(container_addr, 'status.txt')
    exist = status.read_from_file(path)
    if exist:
        container_id = status.ContainerId
        # TODO: Collect history data in the past time period and update
        print '[Status Monitor] Update status.txt for ' + container_id + \
                ' at ' + container_addr
        status.write_to_file(path)
    else:
        print '[Status Monitor] status.txt on ' + container_addr + ' not found'


# Assume this program is running on the root folder of each container, and the
# status.txt is already exist and configured.
# For simulation, we just let it scan the containers folder.
if __name__ == '__main__':
    while True:
        containers_root_path = 'containers/'
        dirs = [ d for d in os.listdir(containers_root_path) \
                if os.path.isdir(os.path.join(containers_root_path, d)) ]
        for d in dirs:
            path = os.path.join(containers_root_path, d)
            print path
            update_status(path)
            time.sleep(1)

        print '--------------------'
        time.sleep(10)

