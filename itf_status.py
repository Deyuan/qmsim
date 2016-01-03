# Container Status File Interface
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 9, 2015

class ContainerStatus:
    def __init__(self, status_tuple=None):
        if status_tuple is None:
            self.ContainerId = ''           # (str) a unique string
            # static information
            self.StorageTotal = 0           # (int) MB
            self.PathToSwitch = ''          # (str) path, may be multiple switches
            self.CoresAvailable = 0         # (int) for concurrency available
            self.StorageRBW = 0.0           # (flt) max MB/s
            self.StorageWBW = 0.0           # (flt) max MB/s
            self.StorageRLatency = 0        # (int) min uSec
            self.StorageWLatency = 0        # (int) min uSec
            self.StorageRAIDLevel = 0       # (int) range 0..6
            self.CostPerGBMonth = 0.0       # (flt) allocation units
            self.DataIntegrity = 0          # (int) range 0 to 10**6, 0 is worst
            # dynamic information
            self.StorageReserved = 0        # (int) MB - containers don't know this
            self.StorageUsed = 0            # (int) MB
            self.StorageReliability = 0     # (int) an integer with a presumed leading 0
            self.ContainerAvailability = 0  # (int) an integer with a presumed leading 0
            self.StorageRBW_dyn = 0.0       # (flt) MB/s. e.g. average bw of past 10 minutes
            self.StorageWBW_dyn = 0.0       # (flt) MB/s. e.g. average bw of past 10 minutes
            # extra information
            self.PhysicalLocation = ''      # (str) hierarchical phisical location
            self.NetworkAddress = ''        # (str) grid path of the container
            self.StatusPath = ''            # (str) grid path of the status file
        else:
            self.ContainerId, \
            self.StorageTotal, \
            self.PathToSwitch, \
            self.CoresAvailable, \
            self.StorageRBW, \
            self.StorageWBW, \
            self.StorageRLatency, \
            self.StorageWLatency, \
            self.StorageRAIDLevel, \
            self.CostPerGBMonth, \
            self.DataIntegrity, \
            self.StorageReserved, \
            self.StorageUsed, \
            self.StorageReliability, \
            self.ContainerAvailability, \
            self.StorageRBW_dyn, \
            self.StorageWBW_dyn, \
            self.PhysicalLocation, \
            self.NetworkAddress, \
            self.StatusPath = status_tuple
            # convert unicode string into regular string
            self.ContainerId = str(self.ContainerId)
            self.PathToSwitch = str(self.PathToSwitch)
            self.PhysicalLocation = str(self.PhysicalLocation)
            self.NetworkAddress = str(self.NetworkAddress)
            self.StatusPath = str(self.StatusPath)

    # Parse status from a string
    def parse_string(self, status_string):
        if status_string == '':
            return -1;
        status = status_string.splitlines()
        for line in status:
            info = line.split('#')[0].split(',')
            if len(info) < 2:
                continue
            key = info[0].strip()
            val = info[1].strip()
            if key == '' or val == '':
                continue
            if key == 'ContainerId':
                self.ContainerId = val
            elif key == 'StorageTotal':
                self.StorageTotal = int(float(val))
            elif key == 'PathToSwitch':
                self.PathToSwitch = val
            elif key == 'CoresAvailable':
                self.CoresAvailable = int(float(val))
            elif key == 'StorageRBW':
                self.StorageRBW = float(val)
            elif key == 'StorageWBW':
                self.StorageWBW = float(val)
            elif key == 'StorageRLatency':
                self.StorageRLatency = int(float(val))
            elif key == 'StorageWLatency':
                self.StorageWLatency = int(float(val))
            elif key == 'StorageRAIDLevel':
                self.StorageRAIDLevel = int(float(val))
            elif key == 'CostPerGBMonth':
                self.CostPerGBMonth = float(val)
            elif key == 'DataIntegrity':
                self.DataIntegrity = int(float(val))
            elif key == 'StorageReserved':
                self.StorageReserved = int(float(val))
            elif key == 'StorageUsed':
                self.StorageUsed = int(float(val))
            elif key == 'StorageReliability':
                self.StorageReliability = int(float(val))
            elif key == 'ContainerAvailability':
                self.ContainerAvailability = int(float(val))
            elif key == 'StorageRBW_dyn':
                self.StorageRBW_dyn = float(val)
            elif key == 'StorageWBW_dyn':
                self.StorageWBW_dyn = float(val)
            elif key == 'PhysicalLocation':
                self.PhysicalLocation = val
            elif key == 'NetworkAddress':
                self.NetworkAddress = val
            elif key == 'StatusPath':
                self.StatusPath = val
            else:
                print '[itf_status] Warning: Cannot parse: ' + line
                return -1;
        return 0

    # Read status from a file
    def read_from_file(self, path):
        try:
            f = open(path, 'r')
        except:
            print '[itf_status] Error: Cannot open: ' + path
            return False
        status = f.read()
        self.parse_string(status)
        f.close()
        return True

    # Generate a multi-line string
    def to_string(self):
        status = '# Container status file. Generated by qmsim.\n' \
            + 'ContainerId'          + ', ' + str(self.ContainerId          ) + '\n' \
            + 'StorageTotal'         + ', ' + str(self.StorageTotal         ) + '\t# MB\n' \
            + 'PathToSwitch'         + ', ' + str(self.PathToSwitch         ) + '\n' \
            + 'CoresAvailable'       + ', ' + str(self.CoresAvailable       ) + '\n' \
            + 'StorageRBW'           + ', ' + str(self.StorageRBW           ) + '\t# MB/s\n' \
            + 'StorageWBW'           + ', ' + str(self.StorageWBW           ) + '\t# MB/s\n' \
            + 'StorageRLatency'      + ', ' + str(self.StorageRLatency      ) + '\t# uSec\n' \
            + 'StorageWLatency'      + ', ' + str(self.StorageWLatency      ) + '\t# uSec\n' \
            + 'StorageRAIDLevel'     + ', ' + str(self.StorageRAIDLevel     ) + '\n' \
            + 'CostPerGBMonth'       + ', ' + str(self.CostPerGBMonth       ) + '\t# $\n' \
            + 'DataIntegrity'        + ', ' + str(self.DataIntegrity        ) + '\t# 0:worst\n' \
            + 'StorageReserved'      + ', ' + str(self.StorageReserved      ) + '\t# MB\n' \
            + 'StorageUsed'          + ', ' + str(self.StorageUsed          ) + '\t# MB\n' \
            + 'StorageReliability'   + ', ' + str(self.StorageReliability   ) + '\n' \
            + 'ContainerAvailability'+ ', ' + str(self.ContainerAvailability) + '\n' \
            + 'StorageRBW_dyn'       + ', ' + str(self.StorageRBW_dyn       ) + '\t# MB/s\n' \
            + 'StorageWBW_dyn'       + ', ' + str(self.StorageWBW_dyn       ) + '\t# MB/s\n' \
            + 'PhysicalLocation'     + ', ' + str(self.PhysicalLocation     ) + '\n' \
            + 'NetworkAddress'       + ', ' + str(self.NetworkAddress       ) + '\n' \
            + 'StatusPath'           + ', ' + str(self.StatusPath           ) + '\n'
        return status

    # Write status to a file
    def write_to_file(self, path):
        # TODO: atomic and file lock
        try:
            with open(path, 'w') as f:
                status = self.to_string()
                f.write(status)
            return True
        except:
            print '[itf_status] Error: Cannot write to ' + path
            return False

    # Generate a tuple for inserting to SQLite database
    def to_tuple(self):
        return (self.ContainerId          ,
                self.StorageTotal         ,
                self.PathToSwitch         ,
                self.CoresAvailable       ,
                self.StorageRBW           ,
                self.StorageWBW           ,
                self.StorageRLatency      ,
                self.StorageWLatency      ,
                self.StorageRAIDLevel     ,
                self.CostPerGBMonth       ,
                self.DataIntegrity        ,
                self.StorageReserved      ,
                self.StorageUsed          ,
                self.StorageReliability   ,
                self.ContainerAvailability,
                self.StorageRBW_dyn       ,
                self.StorageWBW_dyn       ,
                self.PhysicalLocation     ,
                self.NetworkAddress       ,
                self.StatusPath           )


# A string for creating the container status table in SQLite database
def get_sql_header():
    header = " Containers(" + \
             "ContainerId"           + " TEXT PRIMARY KEY UNIQUE," + \
             "StorageTotal"          + " INT ," + \
             "PathToSwitch"          + " TEXT," + \
             "CoresAvailable"        + " INT ," + \
             "StorageRBW"            + " REAL," + \
             "StorageWBW"            + " REAL," + \
             "StorageRLatency"       + " INT ," + \
             "StorageWLatency"       + " INT ," + \
             "StorageRAIDLevel"      + " INT ," + \
             "CostPerGBMonth"        + " REAL," + \
             "DataIntegrity"         + " INT ," + \
             "StorageReserved"       + " INT ," + \
             "StorageUsed"           + " INT ," + \
             "StorageReliability"    + " INT ," + \
             "ContainerAvailability" + " INT ," + \
             "StorageRBW_dyn"        + " REAL," + \
             "StorageWBW_dyn"        + " REAL," + \
             "PhysicalLocation"      + " TEXT," + \
             "NetworkAddress"        + " TEXT," + \
             "StatusPath"            + " TEXT" + \
             ");"
    return header

# Testing
if __name__ == '__main__':
    status = ContainerStatus()
    status_str = status.to_string()
    print status_str
    status.parse_string(status_str)
    status.write_to_file('status.txt')
    status.read_from_file('status.txt')

