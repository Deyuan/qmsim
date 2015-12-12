# Client QoS Spec File Interface
# Author: Deyuan Guo, Chunkun Bo
# Date: Dec 9, 2015

class QosSpec:
    def __init__(self):
        self.SpecId = ''           # (str) a unique string
        # hard requirements
        self.Availability = 99     # (int) an integer with a presumed leading 0
        self.Reliability = 90      # (int) an integer with a presumed leading 0
        self.ReservedSize = 0      # (int) MB
        self.UsedSize = 0          # (int) MB
        self.DataIntegrity = 0     # (int) range 0 to 10**6, 0 is worst
        # flexible requirements
        self.Bandwidth = 'Low'     # (str) 'High' or 'Low'
        self.Latency = 'High'      # (str) 'High' or 'Low'
        # extra information
        self.PhysicalLocation = '' # (str) hierarchical phisical location

    # Parse spec from a string
    def parse_string(self, spec_string):
        spec = spec_string.splitlines()
        for line in spec:
            info = line.split('#')[0].split(',')
            if len(info) < 2:
                continue
            key = info[0].strip()
            val = info[1].strip()
            if key == '' or val == '':
                continue
            if key == 'SpecId':
                self.SpecId = val
            elif key == 'Availability':
                self.Availability = int(float(val))
            elif key == 'Reliability':
                self.Reliability = int(float(val))
            elif key == 'ReservedSize':
                self.ReservedSize = int(float(val))
            elif key == 'UsedSize':
                self.UsedSize = int(float(val))
            elif key == 'DataIntegrity':
                self.DataIntegrity = int(float(val))
            elif key == 'Bandwidth':
                if val == 'high' or val == 'High' or val == 'HIGH':
                    self.Bandwidth = 'High'
                else:
                    self.Bandwidth = 'Low'
            elif key == 'Latency':
                if val == 'high' or val == 'High' or val == 'HIGH':
                    self.Latency = 'High'
                else:
                    self.Latency = 'Low'
            elif key == 'PhysicalLocation':
                # a list of locaiton separated by ','
                self.PhysicalLocation = []
                for i in range(1, len(info)):
                    self.PhysicalLocation.append(info[i].strip())
            else:
                print '[itf_spec] Warning: Cannot parse: ' + line

    # Read spec from a file
    def read_from_file(self, path):
        try:
            f = open(path, 'r')
        except:
            print '[itf_spec] Error: Cannot parse ' + path
            return False
        spec = f.read()
        self.parse_string(spec)
        f.close()
        return True

    # Generate a multi-line string
    def to_string(self):
        spec = '# Client QoS spec file. Generated by qmsim.\n' \
             + 'SpecId'        + ', ' + str(self.SpecId       ) + '\n' \
             + 'Availability'  + ', ' + str(self.Availability ) + '\n' \
             + 'Reliability'   + ', ' + str(self.Reliability  ) + '\n' \
             + 'ReservedSize'  + ', ' + str(self.ReservedSize ) + '\t# MB\n' \
             + 'UsedSize'      + ', ' + str(self.UsedSize     ) + '\t# MB\n' \
             + 'DataIntegrity' + ', ' + str(self.DataIntegrity) + '\t# 0:worst\n' \
             + 'Bandwidth'     + ', ' + str(self.Bandwidth    ) + '\t# High or Low\n' \
             + 'Latency'       + ', ' + str(self.Latency      ) + '\t# High or Low\n' \
             + 'PhysicalLocation'
        if len(self.PhysicalLocation) == 0:
            spec += ', '
        else:
            for s in self.PhysicalLocation:
                spec += ', ' + s
        spec += '\n'
        return spec

    # Write spec to a file
    def write_to_file(self, path):
        # TODO: atomic and file lock
        try:
            with open(path, 'w') as f:
                spec = self.to_string()
                f.write(spec)
            return True
        except:
            print '[QoS Spec] Error: Cannot write to ' + path
            return False

# Testing
if __name__ == '__main__':
    spec = QosSpec()
    spec_str = spec.to_string()
    print spec_str
    spec.parse_string(spec_str)
    spec.write_to_file('spec.txt')
    spec.read_from_file('spec.txt')

