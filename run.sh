#!/bin/bash

# Start from empty database
python qos_manager.py -destroy_db

python qos_manager.py -show_db

python qos_manager.py -add_container containers/container1
python qos_manager.py -add_container containers/container2
python qos_manager.py -add_container containers/container3
python qos_manager.py -add_container containers/container4

python qos_manager.py -show_db_verbose

python qos_manager.py -schedule clients/client1/spec1.txt
python qos_manager.py -show_db_verbose
python qos_manager.py -schedule clients/client1/spec2.txt
python qos_manager.py -show_db_verbose
python qos_manager.py -schedule clients/client2/spec1.txt
python qos_manager.py -show_db_verbose

python qos_manager.py -rm_spec client1-spec1
python qos_manager.py -show_db_verbose

