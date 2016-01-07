# Container Monitor
# Monitoring container status, updating to database, and checking related specs
# Author: Chunkun Bo, Deyuan Guo
# Date: Dec 9, 2015

import os
import time

import itf_database
import itf_status
import monitor_qos

# Wrapper for pulling a file from remote container on Internet
def get_container_status(status_path):
    status = itf_status.ContainerStatus()
    succ = status.read_from_file(status_path)
    if succ:
        return status
    else:
        return None

# update a container information
def update(container_id):
    status_in_db = itf_database.get_status(container_id)
    assert status_in_db != None
    status_remote = get_container_status(status_in_db.StatusPath)
    if status_remote == None:
        print '[Container Monitor] Container', container_id, 'not available.'
        # TODO: update database and reschedule
    else:
        assert status_remote.ContainerId == container_id
        # Avoid overwriting the reservable size (container doesn't know this)
        status_remote.StorageReserved = status_in_db.StorageReserved
        itf_database.update_container(status_remote)
        # call QoS Monitor
        print '[Container Monitor] Call QoS Monitor for', container_id
        monitor_qos.monitor_container(container_id)

# Insert a new container status to database, called by QoS Manager
def insert(status_path):
    status_remote = get_container_status(status_path)
    if status_remote is not None:
        container_id = status_remote.ContainerId
        status_remote.StatusPath = status_path
        # Initial reserved size = used size
        status_remote.StorageReserved = status_remote.StorageUsed
        added = itf_database.update_container(status_remote, init=True)
    else:
        print "[Container Monitor] Status path " + status_path + \
                " is not available"


# Container Monitor Mainloop
if __name__ == '__main__':
    print '[Container Monitor] Start monitoring...'
    while True:
        records = itf_database.get_container_id_list()
        for record in records:
            print '--------------------'
            print '[Container Monitor] Check ' + record
	    
            update(record)
            time.sleep(3)
        time.sleep(10)

