# QoS Database Accessing Interface
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 9, 2015

import sqlite3 as lite
import os

import itf_status
import itf_spec

QOS_ROOT_PATH = "./" # Set as absolute path
QOS_DB_PATH = os.path.join(QOS_ROOT_PATH, "database")
QOS_DB = os.path.join(QOS_DB_PATH, "qos.db")

# Destroy the QoS database
def destroy():
    if os.path.isfile(QOS_DB):
        os.remove(QOS_DB)

# Init the QoS database
def init():
    if not os.path.isdir(QOS_DB_PATH):
        os.mkdir(QOS_DB_PATH)
    destroy()
    con = None
    try:
        con = lite.connect(QOS_DB)
        cur = con.cursor()

        # Create three tables for QoS management
        cur.execute("CREATE TABLE" + itf_spec.get_sql_header())
        cur.execute("CREATE TABLE" + itf_status.get_sql_header())
        sql = "CREATE TABLE Relationships(SpecId TEXT, ContainerId TEXT," + \
              "UNIQUE(SpecId, ContainerId) ON CONFLICT REPLACE);"
        cur.execute(sql)
        con.commit()

        cur.execute("SELECT SQLITE_VERSION();")
        ver = cur.fetchone()
        print "[itf_database] Initialized with SQLite version %s" % ver
    except lite.Error, e:
        print "[itf_database] Error: %s" % e.args[0]
    finally:
        if con:
            con.close()

# Print the summary of the SQLite database
def summary(verbose):
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        tables = ["Specifications", "Containers", "Relationships"]
        print "--------------------"
        print "[itf_database] QoS Database Summary" + \
                (" - Verbose" if verbose else "")
        for table in tables:
            cur.execute("PRAGMA table_info(%s);" % table)
            cols = len(cur.fetchall())
            cur.execute("SELECT Count() FROM %s;" % table)
            rows = cur.fetchone()[0]
            print "Table %s: %d rows, %d cols" % (table, rows, cols)
        if verbose:
            for table in tables:
                cur.execute("SELECT * FROM %s;" % table)
                print table + ":"
                data = cur.fetchall()
                for row in data:
                    print row
        print "--------------------"

# Update a container status to the SQLite database
# init=True: insert a new container; Default: update a container
def update_container(new_status_obj, init=False):
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM Containers WHERE ContainerId = ?", \
                (new_status_obj.ContainerId,))
        data = cur.fetchone()
        if data is None:
            if init:
                # Insert a new container
                status_tuple = str(new_status_obj.to_tuple())
                cur.execute("INSERT INTO Containers VALUES" + status_tuple)
                print "[itf_database] Insert container " + \
                        new_status_obj.ContainerId + " into QoS database."
            else:
                print "[itf_database] Error: Container " + \
                        new_status_obj.ContainerId + " does not exist."
                return False
        else:
            if not init:
                # Update an existing container - except status.StorageReserved
                reserved = data[11] # def: ContainerStatus.StorageReserved
                new_status_obj.StorageReserved = reserved
                status_tuple = str(new_status_obj.to_tuple())
                cur.execute("REPLACE INTO Containers VALUES" + status_tuple)
                print "[itf_database] Update status of container " + \
                        new_status_obj.ContainerId + " into QoS database."
            else:
                print "[itf_database] Error: Container " + \
                        new_status_obj.ContainerId + " already exists."
                return False
        return True
    return False

# Add a scheduled specification to the SQLite database
# init=True: insert a new spec; Default: reschedule a spec
def add_scheduled_spec(new_spec_obj, scheduled_container_ids, init=False):
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM Specifications WHERE SpecId = ?", \
                (new_spec_obj.SpecId,))
        data = cur.fetchone()
        if data is None:
            if init:
                # Insert a new spec
                spec_tuple = str(new_spec_obj.to_tuple())
                cur.execute("INSERT INTO Specifications VALUES" + spec_tuple)
                for cid in scheduled_container_ids:
                    # Insert the mapping
                    map_tuple = str((str(new_spec_obj.SpecId), str(cid)))
                    print map_tuple
                    cur.execute("INSERT INTO Relationships VALUES" + map_tuple)
                    # Update reserved size
                    cur.execute("UPDATE Containers SET StorageReserved = " + \
                            "StorageReserved + ? WHERE ContainerId = ?", \
                            (new_spec_obj.ReservedSize, cid))

                print "[itf_database] Insert specification " + \
                        new_spec_obj.SpecId + " " + \
                        str(scheduled_container_ids) + \
                        " into QoS database."
            else:
                print "[itf_database] Error: Specification " + \
                        new_spec_obj.SpecId + " does not exist."
                return False
        else:
            if not init:
                # Update an existing spec
                spec_tuple = str(new_spec_obj.to_tuple())
                cur.execute("REPLACE INTO Specifications VALUES" + spec_tuple)
                # Update the mapping:
                # 1. a container both in old and new: not change
                cur.execute("SELECT * FROM Relationships WHERE SpecId = ?", \
                        (new_spec_obj.SpecId, ))
                old_containers = [x[1] for x in cur.fetchall()]
                # TODO: filter out unavailable old containers

                # 2. a container only in new: create and file copy
                nth = 0
                for cid in scheduled_container_ids:
                    if cid not in old_containers:
                        # Insert the mapping
                        map_tuple = str((str(new_spec_obj.SpecId), str(cid)))
                        cur.execute("INSERT INTO Relationships VALUES" + map_tuple)
                        # Update reserved size
                        cur.execute("UPDATE Containers SET StorageReserved = " + \
                                "StorageReserved + ? WHERE ContainerId = ?", \
                                (new_spec_obj.ReservedSize, cid))
                        # TODO: File copy
                        print "[itf_database] File copy for specification " + \
                                new_spec_obj.SpecId + " from " + \
                                old_containers[nth] + " to " + cid + "."
                        nth = (nth + 1) % len(old_containers)

                # 3. a container only in old: delete after finish file copy
                for cid in old_containers:
                    if cid not in scheduled_container_ids:
                        # Delete the mapping
                        map_tuple = (new_spec_obj.SpecId, cid)
                        cur.execute("DELETE FROM Relationships WHERE " + \
                                "SpecId = ? AND ContainerId = ?", map_tuple)
                        # Update reserved size
                        cur.execute("UPDATE Containers SET StorageReserved = " + \
                                "StorageReserved - ? WHERE ContainerId = ?", \
                                (new_spec_obj.ReservedSize, cid))
                        # TODO: Delete File
                        print "[itf_database] Delete storage of specification " + \
                                new_spec_obj.SpecId + " on " + cid + "."

                print "[itf_database] Update specification " + \
                        new_spec_obj.SpecId + " " + \
                        str(scheduled_container_ids) + \
                        " into QoS database."
            else:
                print "[itf_database] Error: Specification " + \
                        new_spec_obj.SpecId + " already exists."
                return False
        return True
    return False

# Remove a spec from the SQLite database
def remove_spec(spec_id):
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM Specifications WHERE SpecId = ?", \
                (spec_id,))
        data = cur.fetchone()
        if data is not None:
            # Delete entry in Specifications table
            reserved = data[3] # def: QosSpec.ReservedSize
            cur.execute("DELETE FROM Specifications WHERE SpecId = ?", \
                    (spec_id,))
            # Delete mapping in Relationships table
            cur.execute("SELECT * FROM Relationships WHERE SpecId = ?", \
                        (spec_id, ))
            related_containers = [x[1] for x in cur.fetchall()]
            cur.execute("DELETE FROM Relationships WHERE SpecId = ?", \
                    (spec_id,))
            # Update the reserved size in Containers table and delete file
            for cid in related_containers:
                cur.execute("UPDATE Containers SET StorageReserved = " + \
                        "StorageReserved - ? WHERE ContainerId = ?", \
                        (reserved, cid))
            print "[itf_database] Specification " + spec_id + \
                " is removed from the database, and reserved space is released."
        return True
    return False

# Given a spec_id, return all related container_ids
def get_container_ids_for_spec(spec_id):
    container_ids = []
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM Relationships WHERE SpecId = ?", (spec_id,))
        container_ids = [str(x[1]) for x in cur.fetchall()]
    return container_ids

# Given a container_id, return all related spec_ids
def get_spec_ids_on_container(container_id):
    spec_ids = []
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM Relationships WHERE ContainerId = ?", \
                (container_id,))
        spec_ids = [str(x[0]) for x in cur.fetchall()]
    return spec_ids

# Get the complete container id list
def get_container_id_list():
    container_ids = []
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        cur.execute("SELECT ContainerId FROM Containers")
        container_ids = [str(x[0]) for x in cur.fetchall()]
    return container_ids

# Get the complete spec id list
def get_spec_id_list():
    spec_ids = []
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        cur.execute("SELECT SpecId FROM Specifications")
        spec_ids = [str(x[0]) for x in cur.fetchall()]
    return spec_ids

# Given a container_id, get a ContainerStatus object
def get_status(container_id):
    status = None
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM Containers WHERE ContainerId = ?", \
                (container_id,))
        data = cur.fetchone()
        if data is not None:
            status = itf_status.ContainerStatus(data)
    return status

# Given a spec_id, get a QosSpec object
def get_spec(spec_id):
    spec = None
    con = lite.connect(QOS_DB)
    with con:
        cur = con.cursor()
        cur.execute("SELECT * FROM Specifications WHERE SpecId = ?", \
                (spec_id,))
        data = cur.fetchone()
        if data is not None:
            spec = itf_spec.QosSpec(data)
    return spec

# Only for testing
def test():
    init()
    summary(False)
    summary(True)

    status = itf_status.ContainerStatus()
    status.ContainerId = "container1"
    status.NetworkAddress = "//aaa"
    update_container(status, init=True)
    summary(True)

    status = itf_status.ContainerStatus()
    status.ContainerId = "container2"
    status.NetworkAddress = "//bbb"
    update_container(status, init=True)
    summary(True)

    status = itf_status.ContainerStatus()
    status.ContainerId = "container1"
    status.NetworkAddress = "//ccc"
    status.StorageTotal = 1000
    update_container(status)
    summary(True)

    spec = itf_spec.QosSpec()
    spec.SpecId = "client1-spec1"
    container_list = ["container1"]
    add_scheduled_spec(spec, container_list, init=True)
    summary(True)

    spec = itf_spec.QosSpec()
    spec.SpecId = "client1-spec1"
    container_list = ["container1", "container2"]
    add_scheduled_spec(spec, container_list)
    summary(True)

    spec = itf_spec.QosSpec()
    spec.SpecId = "client1-spec1"
    container_list = ["container2"]
    add_scheduled_spec(spec, container_list)
    summary(True)

    remove_spec("client1-spec1")
    summary(True)

    spec = itf_spec.QosSpec()
    spec.SpecId = "client1-spec1"
    container_list = ["container1", "container2"]
    add_scheduled_spec(spec, container_list, init=True)
    summary(True)
    print get_container_ids_for_spec("client1-spec1")
    print get_spec_ids_on_container("container1")
    print get_container_id_list()
    print get_spec_id_list()
    status = get_status("container1x")
    print status
    status = get_status("container1")
    print status
    if status is not None:
        print status.to_string()
    spec = get_spec("xxx")
    print spec
    spec = get_spec("client1-spec1")
    print spec
    if spec is not None:
        print spec.to_string()


# Only for testing
if __name__ == '__main__':
    print '[itf_database] QoS Database Accessing Interface.'
    test()

