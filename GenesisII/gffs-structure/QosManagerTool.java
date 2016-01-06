package edu.virginia.vcgr.genii.client.cmd.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.morgan.util.io.StreamUtils;

import edu.virginia.vcgr.genii.client.byteio.ByteIOConstants;
import edu.virginia.vcgr.genii.client.cmd.ReloadShellException;
import edu.virginia.vcgr.genii.client.cmd.ToolException;
import edu.virginia.vcgr.genii.client.dialog.UserCancelException;
import edu.virginia.vcgr.genii.client.gpath.GeniiPath;
import edu.virginia.vcgr.genii.client.io.LoadFileResource;
import edu.virginia.vcgr.genii.client.rns.RNSException;
import edu.virginia.vcgr.genii.client.rp.ResourcePropertyException;
import edu.virginia.vcgr.genii.client.security.axis.AuthZSecurityException;

public class QosManagerTool extends BaseGridTool
{
	static final private String _DESCRIPTION = "config/tooldocs/description/dqos-manager";
	static final private String _USAGE = "config/tooldocs/usage/uqos-manager";
	static final private String _MANPAGE = "config/tooldocs/man/qos-manager";

	private boolean _test_db = false;
	
	private class QosSpec
	{
        public String SpecId = "";     			// (str) a unique string
        public int Availability = 99;     		// (int) an integer with a presumed leading 0
        public int Reliability = 99;      		// (int) an integer with a presumed leading 0
        public int ReservedSize = 100;    		// (int) MB
        public int UsedSize = 0;          		// (int) MB
        public int DataIntegrity = 0;     		// (int) range 0 to 10**6, 0 is worst
        public String Bandwidth = "Low";     	// (str) 'High' or 'Low'
        public String Latency = "High";      	// (str) 'High' or 'Low'
        public String PhysicalLocations = "";	// (str) locations separated by ;
        public String SpecPath = "";          	// (str) grid path to the spec file
        		
		public QosSpec() {
			
		}
		
		public boolean parse_string(String spec_string) {
			System.out.println("(qos_manager) NYI.");
			return false;
		}

		public String to_string() {
			System.out.println("(qos_manager) NYI.");
			return "";
		}

		public boolean read_from_file(String file_path) {
			System.out.println("(qos_manager) NYI.");
			return false;
		}

		public boolean write_to_file(String file_path) {
			System.out.println("(qos_manager) NYI.");
			return false;
		}
		
		public String get_sql_header() {
			String header = " Specifications(" + 
					"SpecId            TEXT PRIMARY KEY UNIQUE," + 
			        "Availability      INT," + 
			        "Reliability       INT," + 
			        "ReservedSize      INT," + 
			        "UsedSize          INT," + 
			        "DataIntegrity     INT," + 
			        "Bandwidth         TEXT," + 
			        "Latency           TEXT," + 
			        "PhysicalLocations TEXT," + 
			        "SpecPath          TEXT" + 
			        ");";
			return header;
		}
	}
	
	private class ContainerStatus
	{
        public String ContainerId = "";         // (str) a unique string
        // static information
        public int StorageTotal = 0;            // (int) MB
        public String PathToSwitch = "";        // (str) path, may be multiple switches
        public int CoresAvailable = 0;          // (int) for concurrency available
        public double StorageRBW = 0.0;         // (flt) max MB/s
        public double StorageWBW = 0.0;         // (flt) max MB/s
        public int StorageRLatency = 0;         // (int) min uSec
        public int StorageWLatency = 0;         // (int) min uSec
        public int StorageRAIDLevel = 0;        // (int) range 0..6
        public double CostPerGBMonth = 0.0;     // (flt) allocation units
        public int DataIntegrity = 0;           // (int) range 0 to 10**6, 0 is worst
        // dynamic information
        public int StorageReserved = 0;         // (int) MB - containers don't know this
        public int StorageUsed = 0;             // (int) MB
        public int StorageReliability = 0;      // (int) an integer with a presumed leading 0
        public int ContainerAvailability = 0;   // (int) an integer with a presumed leading 0
        public double StorageRBW_dyn = 0.0;  	// (flt) MB/s. e.g. average bw of past 10 minutes
        public double StorageWBW_dyn = 0.0;     // (flt) MB/s. e.g. average bw of past 10 minutes
        // extra information
        public String PhysicalLocation = "";    // (str) hierarchical phisical location
        public String NetworkAddress = "";      // (str) grid path of the container
        public String StatusPath = "";          // (str) grid path of the status file
        		
		public ContainerStatus() {
			
		}
		
		public boolean parse_string(String spec_string) {
			System.out.println("(qos_manager) NYI.");
			return false;
		}

		public String to_string() {
			System.out.println("(qos_manager) NYI.");
			return "";
		}

		public boolean read_from_file(String file_path) {
			System.out.println("(qos_manager) NYI.");
			return false;
		}

		public boolean write_to_file(String file_path) {
			System.out.println("(qos_manager) NYI.");
			return false;
		}
		
		public String get_sql_header() {
		    String header = " Containers(" + 
		             "ContainerId"           + " TEXT PRIMARY KEY UNIQUE," + 
		             "StorageTotal"          + " INT ," + 
		             "PathToSwitch"          + " TEXT," + 
		             "CoresAvailable"        + " INT ," + 
		             "StorageRBW"            + " REAL," + 
		             "StorageWBW"            + " REAL," + 
		             "StorageRLatency"       + " INT ," + 
		             "StorageWLatency"       + " INT ," + 
		             "StorageRAIDLevel"      + " INT ," + 
		             "CostPerGBMonth"        + " REAL," + 
		             "DataIntegrity"         + " INT ," + 
		             "StorageReserved"       + " INT ," + 
		             "StorageUsed"           + " INT ," + 
		             "StorageReliability"    + " INT ," + 
		             "ContainerAvailability" + " INT ," + 
		             "StorageRBW_dyn"        + " REAL," + 
		             "StorageWBW_dyn"        + " REAL," + 
		             "PhysicalLocation"      + " TEXT," + 
		             "NetworkAddress"        + " TEXT," + 
		             "StatusPath"            + " TEXT" + 
		             ");";
			return header;
		}
	}

	public QosManagerTool()
	{
		super(new LoadFileResource(_DESCRIPTION), new LoadFileResource(_USAGE), false, ToolCategory.ADMINISTRATION);
		addManPage(new LoadFileResource(_MANPAGE));
	}

	@Option({ "test-db" })
	public void set_test_db()
	{
		_test_db = true;
	}

	@Override
	protected int runCommand() throws ReloadShellException, ToolException, UserCancelException, RNSException, AuthZSecurityException,
		IOException, ResourcePropertyException
	{
		System.out.println("(qos-manager) Main entry.");
		qos_manager(_test_db, getArgument(0));

		return 0;
	}

	@Override
	protected void verify() throws ToolException
	{
	}

	public void qos_manager(boolean test_db, String path) throws IOException
	{
		if (test_db) {
			System.out.println("(qos_manager) Test the QoS database.");
			test_db();
		} else {
			System.out.println("(qos_manager) Not yet implemented.");
		}
	}
	
	/* QoS database interfaces */
	private boolean db_destroy() {
		System.out.println("(qos_manager) NYI.");
		return false;
	}

	private boolean db_init() {
		System.out.println("(qos_manager) NYI.");
		return false;
	}

	private void db_summary(boolean verbose) {
		System.out.println("(qos_manager) NYI.");
	}

	private boolean db_update_container(ContainerStatus status, boolean update) {
		System.out.println("(qos_manager) NYI.");
		return false;
	}
	
	private boolean db_add_scheduled_spec(QosSpec spec,
			List<String> scheduled_container_ids, boolean init) {
		System.out.println("(qos_manager) NYI.");
		return false;
	}

	private boolean db_remove_spec(String spec_id) {
		System.out.println("(qos_manager) NYI.");
		return false;
	}

	private boolean db_remove_container(String container_id) {
		System.out.println("(qos_manager) NYI.");
		return false;
	}

	private List<String> db_get_container_ids_for_spec(String spec_id) {
		List<String> container_ids = new ArrayList<String>();
		System.out.println("(qos_manager) NYI.");
		return container_ids;
	}

	private List<String> db_get_spec_ids_on_container(String container_id) {
		List<String> spec_ids = new ArrayList<String>();
		System.out.println("(qos_manager) NYI.");
		return spec_ids;
	}

	private List<String> db_get_container_id_list() {
		List<String> container_ids = new ArrayList<String>();
		System.out.println("(qos_manager) NYI.");
		return container_ids;
	}

	private List<String> db_get_spec_id_list() {
		List<String> spec_ids = new ArrayList<String>();
		System.out.println("(qos_manager) NYI.");
		return spec_ids;
	}

	private ContainerStatus db_get_status(String container_id) {
		System.out.println("(qos_manager) NYI.");
		return null;		
	}

	private QosSpec db_get_spec(String spec_id) {
		System.out.println("(qos_manager) NYI.");
		return null;		
	}

	private void test_db() {
	    db_init();
	    db_summary(false);
	    db_summary(true);

	    ContainerStatus status = new ContainerStatus();
	    status.ContainerId = "container1";
	    status.NetworkAddress = "//aaa";
	    db_update_container(status, true);
	    db_summary(true);

	    //status = new ContainerStatus()
	    status.ContainerId = "container2";
	    status.NetworkAddress = "//bbb";
	    db_update_container(status, true);
	    db_summary(true);

	    //status = new ContainerStatus()
	    status.ContainerId = "container1";
	    status.NetworkAddress = "//ccc";
	    status.StorageTotal = 1000;
	    db_update_container(status, false);
	    db_summary(true);

	    QosSpec spec = new QosSpec();
	    spec.SpecId = "client1-spec1";
	    List<String> container_list = new ArrayList<String>();
	    container_list.add("container1");
	    db_add_scheduled_spec(spec, container_list, true);
	    db_summary(true);

	    //spec = new QosSpec();
	    spec.SpecId = "client1-spec1";
	    container_list.clear();
	    container_list.add("container1");
	    container_list.add("container2");
	    db_add_scheduled_spec(spec, container_list, false);
	    db_summary(true);

	    //spec = new QosSpec();
	    spec.SpecId = "client1-spec1";
	    container_list.clear();
	    container_list.add("container2");
	    db_add_scheduled_spec(spec, container_list, false);
	    db_summary(true);

	    db_remove_spec("client1-spec1");
	    db_summary(true);

	    //spec = new QosSpec();
	    spec.SpecId = "client1-spec1";
	    container_list.clear();
	    container_list.add("container1");
	    container_list.add("container2");
	    db_add_scheduled_spec(spec, container_list, true);
	    db_summary(true);
	    
	    System.out.println(db_get_container_ids_for_spec("client1-spec1").toString());
	    System.out.println(db_get_spec_ids_on_container("container1").toString());
	    System.out.println(db_get_container_id_list().toString());
	    System.out.println(db_get_spec_id_list().toString());
	    status = db_get_status("container1x");
	    if (status != null) System.out.println("Error");
	    status = db_get_status("container1");
	    if (status != null) System.out.println(status.to_string());
	    else System.out.println("Error");
	    
	    spec = db_get_spec("xxx");
	    if (spec != null) System.out.println("Error");
	    spec = db_get_spec("client1-spec1");
	    if (spec != null) System.out.println(spec.to_string());
	    else System.out.println("Error");
	}
}
