package edu.virginia.vcgr.genii.client.cmd.tools;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import org.sqlite.*;


import org.morgan.util.io.StreamUtils;

import edu.virginia.vcgr.genii.client.byteio.ByteIOConstants;
import edu.virginia.vcgr.genii.client.cmd.ReloadShellException;
import edu.virginia.vcgr.genii.client.cmd.ToolException;
import edu.virginia.vcgr.genii.client.context.ContextManager;
import edu.virginia.vcgr.genii.client.context.ICallingContext;
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

	private String _spec_path_to_schedule = null;
	private String _spec_id_to_schedule = null;
	private String _spec_id_to_remove = null;
	private String _status_path_to_add = null;
	private String _container_id_to_remove = null;
	private boolean _show_db = false;
	private boolean _show_db_verbose = false;
	private boolean _clean_db = false;
	private boolean _monitor = false;
	private boolean _test_db = false;
	
	private String QOSDBPath = "/home/dg/database/";
	private String QOSDBName = QOSDBPath + "qos.db"; 
	
	public QosManagerTool()
	{
		super(new LoadFileResource(_DESCRIPTION), new LoadFileResource(_USAGE), false, ToolCategory.ADMINISTRATION);
		addManPage(new LoadFileResource(_MANPAGE));
	}

	@Option({ "schedule" })
	public void set_schedule(String spec_path)
	{
		_spec_path_to_schedule = spec_path;
	}

	@Option({ "schedule-id" })
	public void set_schedule_id(String spec_id)
	{
		_spec_id_to_schedule = spec_id;
	}

	@Option({ "rm-spec" })
	public void set_rm_spec(String spec_id)
	{
		_spec_id_to_remove = spec_id;
	}

	@Option({ "add-container" })
	public void set_add_container(String status_path)
	{
		_status_path_to_add = status_path;
	}

	@Option({ "rm-container" })
	public void set_rm_container(String container_id)
	{
		_container_id_to_remove = container_id;
	}

	@Option({ "show-db" })
	public void set_show_db()
	{
		_show_db = true;
	}

	@Option({ "show-db-verbose" })
	public void set_show_db_verbose()
	{
		_show_db_verbose = true;
	}

	@Option({ "clean-db" })
	public void set_clean_db()
	{
		_clean_db = true;
	}

	@Option({ "monitor" })
	public void set_monitor()
	{
		_monitor = true;
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
		qos_manager(getArgument(0));
		return 0;
	}

	@Override
	protected void verify() throws ToolException
	{
	}

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

	public void qos_manager(String arg) throws IOException
	{
		System.out.println("(qos-manager) Main entry.");
		if (_spec_path_to_schedule != null) {
			System.out.println("(qos_manager) Schedule a QoS specs file at " + _spec_path_to_schedule);
			// TODO: call scheduler
		} else if (_spec_id_to_schedule != null) {
			System.out.println("(qos_manager) Schedule a QoS specs id " + _spec_id_to_schedule);
			// TODO: call scheduler
		} else if (_spec_id_to_remove != null) {
			System.out.println("(qos_manager) Remove a QoS specs id " + _spec_id_to_remove);
			db_remove_spec(_spec_id_to_remove);
		} else if (_status_path_to_add != null) {
			System.out.println("(qos_manager) Add a container with status file at " + _status_path_to_add);
			ContainerStatus status = new ContainerStatus();
			status.read_from_file(_status_path_to_add);
			db_update_container(status, true); // init
		} else if (_container_id_to_remove != null) {
			System.out.println("(qos_manager) Remove container id " + _container_id_to_remove);
			db_remove_container(_container_id_to_remove);
		} else if (_show_db) {
			System.out.println("(qos_manager) Show information of the QoS database.");
			db_summary(false);
		} else if (_show_db_verbose) {
			System.out.println("(qos_manager) Show details of the QoS database.");
			db_summary(true);
		} else if (_clean_db) {
			System.out.println("(qos_manager) Clean QoS database.");
			db_init();
		} else if (_monitor) {
			System.out.println("(qos_manager) Monitor container status and specs.");
			// TODO: add monitors code here
		} else if (_test_db) {
			System.out.println("(qos_manager) Test the QoS database.");
			test_db();
		} else {
			System.out.println("(qos_manager) Not yet implemented...");			
		}
	}
	
	/* QoS database interfaces */	
	private void db_destroy() {
		File dbFile = new File(this.QOSDBName);
	    dbFile.delete();
	}

	private boolean db_init() {
		ContainerStatus status = new ContainerStatus();
		QosSpec spec = new QosSpec();
		// if exists
		File dbFile = new File(this.QOSDBName);
		
		if(dbFile.exists()){
			db_destroy();
		}
		Connection conn = null;
		Statement stmt = null;
		try{
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:"+this.QOSDBName);
			System.out.println("Connect to DB successfully...");
			stmt = conn.createStatement();
			// create table1 and table2
			String create_spec_table = "CREATE TABLE" + spec.get_sql_header();
			String create_status_table = "CREATE TABLE" + status.get_sql_header();
			
			stmt.executeUpdate(create_spec_table);
			stmt.executeUpdate(create_status_table);
				
			String sql = "CREATE TABLE Relationships(SpecId TEXT, ContainerId TEXT, UNIQUE(SpecId, ContainerId) ON CONFLICT REPLACE);";
			//stmt.executeQuery(sql);
			stmt.executeUpdate(sql);
			stmt.close();
			conn.close();
			return true;
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return false;
	}

	private void db_summary(boolean verbose) {
		System.out.println("(qos_manager) NYI.");
		
		Connection conn = null;
		Statement stmt = null;
		try{
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:"+this.QOSDBName);
			
			String[] anArray;
			anArray = new String[3];
			anArray[0] = "Specifications";
			anArray[1] = "Containers";
			anArray[2] = "Relationships";
			System.out.println("--------------------");
			System.out.println("[itf_database] QoS Database Summary");
			
			stmt = conn.createStatement();
			// create table1 and table2
			String get_spec_info = "SELECT * FROM Specifications;";
			
			stmt.executeUpdate(get_spec_info);
			
			System.out.println(get_spec_info);
			
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}		
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
