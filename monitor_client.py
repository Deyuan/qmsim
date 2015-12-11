# Client Monitor
# This monitor is a deamon on client-side to check the spec
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 10, 2015

import os
import time
import itf_spec


# Update the status.txt for a container
def check_specs(spec_path):
    spec = itf_spec.QosSpec()
    exist = spec.read_from_file(spec_path)
    if exist:
        spec_id = spec.SpecId
        # TODO: Check used storage and bandwidth.
        print '[Client Monitor] Check ' + spec_id + ' at ' + spec_path
        spec.write_to_file(spec_path)
    else:
        print '[Client Monitor] ' + spec_path + ' is not a valid spec.'


# Assume this program is running on the root folder of each container, and the
# status.txt is already exist and configured.
# For simulation, we just let it scan the containers folder.
if __name__ == '__main__':
    while True:
        client_root_path = 'clients/'
        dirs = [ d for d in os.listdir(client_root_path) if \
                os.path.isdir(os.path.join(client_root_path, d)) ]
        for d in dirs:
            path = os.path.join(client_root_path, d)
            files = [ f for f in os.listdir(path) if \
                    os.path.isfile(os.path.join(path, f)) ]

            for f in files:
                if f.startswith('spec') and f.endswith('.txt'):
                    spec_path = os.path.join(path, f)
                    check_specs(spec_path)

            time.sleep(1)

        print '--------------------'
        time.sleep(10)

