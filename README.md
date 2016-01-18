# QoS Manager
```
A tool for managing the quality of service in Genesis II for GFFS.

qos-manager [--init-db]
            [--show-db]
            [--show-db-verbose]
            [--add-container=<container-status-path>]
            [--rm-container=<container-id>]
            [--rm-spec=<qos-spec-id>]
            [--rm-directory=<direcotry-path>]
            [--monitor]
            [--clean-replicas]
            [--status-template=<rns-service-path>]
            [--spec-template]

Description:
A tool for managing the quality of service. A user can add some accessible
containers into the QoS database, and use 'mkdir' to create dynamically
scheduled folders.

The following options are available:
--init-db
    Initialize the QoS database. The QoS database will be stored in both the
    grid home directory and the local user directory.
--show-db
    Show summary of the QoS database.
--show-db-verbose
    Show details of the QoS database.
--add-container=<container-status-path>
    Add a new container to the QoS database.
--rm-container=<container-id>
    Remove a container from the QoS database. All directories that are related
    to this container will be rescheduled.
--rm-spec=<qos-spec-id>
    Remove a QoS specification from the QoS database. The records of all
    directories that are created based on this specification will be
    removed from the QoS DB, but actual directories will not be removed.
--rm-directory=<direcotry-path>
    Delete a directory from the QoS database. The actual directory will not
    be removed.
--monitor
    Monitor all directories in the QoS database, and reschedule unsatisfied
    ones.
--clean-replicas
    Cleaning all unused replicas. Users should make sure that file-copy
    operations are done. Otherwise when all source replicas are removed,
    there may be data loss risks.
--status-template=<rns-service-path>
    Generate a template of a container status file for a RNS service path.
--spec-template
    Generate a template of a QoS specification file.

Related tools:
mkdir <target-dir> [--specs=<qos-spec-path>]
    Create a dynamically scheduled folder with a QoS specification file.
```
