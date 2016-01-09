/* QoS Manager
 * Authors: Deyuan Guo, Chunkun Bo
 * January 2016.
 */
package edu.virginia.vcgr.genii.client.cmd.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.morgan.util.io.StreamUtils;

import edu.virginia.vcgr.genii.client.InstallationProperties;
import edu.virginia.vcgr.genii.client.byteio.ByteIOConstants;
import edu.virginia.vcgr.genii.client.cmd.ReloadShellException;
import edu.virginia.vcgr.genii.client.cmd.ToolException;
import edu.virginia.vcgr.genii.client.context.GridUserEnvironment;
import edu.virginia.vcgr.genii.client.dialog.UserCancelException;
import edu.virginia.vcgr.genii.client.gpath.GeniiPath;
import edu.virginia.vcgr.genii.client.io.LoadFileResource;
import edu.virginia.vcgr.genii.client.rns.PathOutcome;
import edu.virginia.vcgr.genii.client.rns.RNSException;
import edu.virginia.vcgr.genii.client.rp.ResourcePropertyException;
import edu.virginia.vcgr.genii.client.security.axis.AuthZSecurityException;

public class QosManagerTool extends BaseGridTool
{
	static final private String _DESCRIPTION = "config/tooldocs/description/dqos-manager";
	static final private String _USAGE = "config/tooldocs/usage/uqos-manager";
	static final private String _MANPAGE = "config/tooldocs/man/qos-manager";

	static QosManagerTool QosManager = null;
	static QosManagerTool factory() {
		if (QosManager == null) {
			QosManager = new QosManagerTool();
		}
		return QosManager;
	}

	private String _spec_id_to_schedule = null;
	private String _spec_id_to_remove = null;
	private String _status_path_to_add = null;
	private String _container_id_to_remove = null;
	private boolean _show_db = false;
	private boolean _show_db_verbose = false;
	private boolean _init_db = false;
	private boolean _monitor = false;
	private boolean _spec_template = false;
	private boolean _status_template = false;
	private boolean _test = false;

	private String _gridHomeDir = null;
	private String _localUserDir = null;
	private String _qosDbName = "qos.db";

	public QosManagerTool()
	{
		super(new LoadFileResource(_DESCRIPTION), new LoadFileResource(_USAGE),
				false, ToolCategory.ADMINISTRATION);
		addManPage(new LoadFileResource(_MANPAGE));
	}

	@Option({ "reschedule" })
	public void set_reschedule(String spec_id)
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

	@Option({ "init-db" })
	public void set_clean_db()
	{
		_init_db = true;
	}

	@Option({ "monitor" })
	public void set_monitor()
	{
		_monitor = true;
	}

	@Option({ "spec-template" })
	public void set_spec_template()
	{
		_spec_template = true;
	}

	@Option({ "status-template" })
	public void set_status_template()
	{
		_status_template = true;
	}

	@Option({ "test" })
	public void set_test_db()
	{
		_test = true;
	}

	@Override
	protected int runCommand() throws ReloadShellException, ToolException,
		UserCancelException, RNSException, AuthZSecurityException,
		IOException, ResourcePropertyException
	{
		qos_manager(getArgument(0));
		return 0;
	}

	@Override
	protected void verify() throws ToolException
	{
	}

	/**************************************************************************
	 *  QoS Specifications
	 **************************************************************************/
	private class QosSpec
	{
		public String SpecId = "";            // (str) a unique string
		public int Availability = 99;         // (int) with presumed leading 0
		public int Reliability = 99;          // (int) with presumed leading 0
		public int ReservedSize = 100;        // (int) MB
		public int UsedSize = 0;              // (int) MB
		public int DataIntegrity = 0;         // (int) 0 to 10**6, 0 is worst
		public String Bandwidth = "Low";      // (str) 'High' or 'Low'
		public String Latency = "High";       // (str) 'High' or 'Low'
		public String PhysicalLocations = ""; // (str) locations separated by ;
		public String SpecPath = "";          // (str) grid path to a spec file

		public QosSpec() {
		}

		// Constructor from a SQlite ResultSet
		public QosSpec(ResultSet rs) {
			try {
				this.SpecId = rs.getString(1);
				this.Availability = rs.getInt(2);
				this.Reliability = rs.getInt(3);
				this.ReservedSize = rs.getInt(4);
				this.UsedSize = rs.getInt(5);
				this.DataIntegrity = rs.getInt(6);
				this.Bandwidth = rs.getString(7);
				this.Latency = rs.getString(8);
				this.PhysicalLocations = rs.getString(9);
				this.SpecPath = rs.getString(10);
			} catch (Exception e) {
				System.out.println(e.getClass().getName() + ": " + e.getMessage());
			}
		}

		public boolean parse_string(String spec_string) {
			String[] lines = spec_string.split("\n");
			for (int i = 0; i < lines.length; i++) {
				String[] contents = lines[i].split("#");
				if (contents.length > 0) {
					String[] key_val = contents[0].split(",");
					String key = "", val = "";
					if (key_val.length > 0) key = key_val[0].trim();
					if (key_val.length > 1) val = key_val[1].trim();
					if (key.equals("")) { continue; }
					else if (key.equals("SpecId")) { this.SpecId = val; }
					else if (key.equals("Availability")) { this.Availability = Integer.parseInt(val); }
					else if (key.equals("Reliability")) { this.Reliability = Integer.parseInt(val); }
					else if (key.equals("ReservedSize")) { this.ReservedSize = Integer.parseInt(val); }
					else if (key.equals("UsedSize")) { this.UsedSize = Integer.parseInt(val); }
					else if (key.equals("DataIntegrity")) { this.DataIntegrity = Integer.parseInt(val); }
					else if (key.equals("Bandwidth")) { this.Bandwidth = val; }
					else if (key.equals("Latency")) { this.Latency = val; }
					else if (key.equals("PhysicalLocations")) { this.PhysicalLocations = val; }
					else if (key.equals("SpecPath")) { this.SpecPath = val; }
					else {
						System.out.println("(qm) Warning: unrecognized specs key: " + key);
						return false;
					}
				}
			}
			if (this.SpecId.equals("")) return false;
			else return true;
		}

		public String to_string() {
			String spec_str = "# QoS specification file. Generated by qos-manager.\n"
					+ "SpecId"        + ", " + this.SpecId        + "\n"
					+ "Availability"  + ", " + this.Availability  + "\n"
					+ "Reliability"   + ", " + this.Reliability   + "\n"
					+ "ReservedSize"  + ", " + this.ReservedSize  + "\t# MB\n"
					+ "UsedSize"      + ", " + this.UsedSize      + "\t# MB\n"
					+ "DataIntegrity" + ", " + this.DataIntegrity + "\t# 0:worst\n"
					+ "Bandwidth"     + ", " + this.Bandwidth     + "\t# High or Low\n"
					+ "Latency"       + ", " + this.Latency       + "\t# High or Low\n"
					+ "PhysicalLocations" + "," + this.PhysicalLocations + "\n"
					+ "SpecPath"      + "," + this.SpecPath + "\n";
			return spec_str;
		}

		public boolean read_from_file(String spec_path) {
			char[] data = new char[ByteIOConstants.PREFERRED_SIMPLE_XFER_BLOCK_SIZE];
			int read;
			InputStream in = null;
			InputStreamReader reader = null;
			String spec_str = "";

			try {
				GeniiPath path = new GeniiPath(spec_path);
				if (!path.exists())
					throw new FileNotFoundException(String.format("Unable to find spec file %s!", path));
				if (!path.isFile())
					throw new IOException(String.format("Spec path %s is not a file!", path));

				in = path.openInputStream();
				reader = new InputStreamReader(in);

				while ((read = reader.read(data, 0, data.length)) > 0) {
					String s = new String(data, 0, read);
					spec_str += s;
				}

				if (spec_str != "") {
					boolean succ = this.parse_string(spec_str);
					if (succ) {
						System.out.println("(qm) Get QoS specs from " + spec_path);
						this.SpecPath = "grid:" + path.lookupRNS();
						return true;
					}
				}
			} catch (Exception e) {
				System.out.println(e.toString());
				spec_str = "";
			} finally {
				StreamUtils.close(reader);
				StreamUtils.close(in);
			}

			System.out.println("(qm) Fail to get QoS specs from " + spec_path);
			return false;
		}

		@SuppressWarnings("unused")
		public boolean write_to_file(String file_path) {
			System.out.println("(qm) NYI.");
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

		public String to_sql_string() {
			String spec_sql_str = "'" + this.SpecId + "',"
					+ this.Availability + "," + this.Reliability + ","
					+ this.ReservedSize + "," + this.UsedSize + ","
					+ this.DataIntegrity + ",'" + this.Bandwidth + "','"
					+ this.Latency  + "','" + this.PhysicalLocations  + "','"
					+ this.SpecPath + "'";
			return spec_sql_str;
		}
	}

	/**************************************************************************
	 *  Container Status
	 **************************************************************************/
	private class ContainerStatus
	{
		public String ContainerId = "";       // (str) a unique string
		// static information
		public int StorageTotal = 0;          // (int) MB
		public String PathToSwitch = "";      // (str) may be multiple switches
		public int CoresAvailable = 0;        // (int) for concurrency
		public double StorageRBW = 0.0;       // (flt) max MB/s
		public double StorageWBW = 0.0;       // (flt) max MB/s
		public int StorageRLatency = 0;       // (int) min uSec
		public int StorageWLatency = 0;       // (int) min uSec
		public int StorageRAIDLevel = 0;      // (int) range 0..6
		public double CostPerGBMonth = 0.0;   // (flt) allocation units
		public int DataIntegrity = 0;         // (int) 0 to 10**6, 0 is worst
		// dynamic information
		public int StorageReserved = 0;       // (int) MB, containers dont know
		public int StorageUsed = 0;           // (int) MB
		public int StorageReliability = 0;    // (int) with presumed leading 0
		public int ContainerAvailability = 0; // (int) with presumed leading 0
		public double StorageRBW_dyn = 0.0;   // (flt) MB/s. Avg of past 10 min
		public double StorageWBW_dyn = 0.0;   // (flt) MB/s. Avg of past 10 min
		// extra information
		public String PhysicalLocation = "";  // (str) physical location
		public String NetworkAddress = "";    // (str) container rns service
		public String StatusPath = "";        // (str) status file genii path

		public ContainerStatus() {
		}

		// Constructor from a SQLite ResultSet
		public ContainerStatus(ResultSet rs) {
			try {
				this.ContainerId = rs.getString(1);
				this.StorageTotal = rs.getInt(2);
				this.PathToSwitch = rs.getString(3);
				this.CoresAvailable = rs.getInt(4);
				this.StorageRBW = rs.getDouble(5);
				this.StorageWBW = rs.getDouble(6);
				this.StorageRLatency = rs.getInt(7);
				this.StorageWLatency = rs.getInt(8);
				this.StorageRAIDLevel = rs.getInt(9);
				this.CostPerGBMonth = rs.getDouble(10);
				this.DataIntegrity = rs.getInt(11);
				this.StorageReserved = rs.getInt(12);
				this.StorageUsed = rs.getInt(13);
				this.StorageReliability = rs.getInt(14);
				this.ContainerAvailability = rs.getInt(15);
				this.StorageRBW_dyn = rs.getDouble(16);
				this.StorageWBW_dyn = rs.getDouble(17);
				this.PhysicalLocation = rs.getString(18);
				this.NetworkAddress = rs.getString(19);
				this.StatusPath = rs.getString(20);
			} catch (Exception e) {
				System.out.println(e.getClass().getName() + ": " + e.getMessage());
			}
		}

		public boolean parse_string(String status_string) {
			String[] lines = status_string.split("\n");
			for (int i = 0; i < lines.length; i++) {
				String[] contents = lines[i].split("#");
				if (contents.length > 0) {
					String[] key_val = contents[0].split(",");
					String key = "", val = "";
					if (key_val.length > 0) key = key_val[0].trim();
					if (key_val.length > 1) val = key_val[1].trim();
					if (key.equals("")) { continue; }
					else if (key.equals("ContainerId")) { this.ContainerId = val; }
					else if (key.equals("StorageTotal")) { this.StorageTotal = Integer.parseInt(val); }
					else if (key.equals("PathToSwitch")) { this.PathToSwitch = val; }
					else if (key.equals("CoresAvailable")) { this.CoresAvailable = Integer.parseInt(val); }
					else if (key.equals("StorageRBW")) { this.StorageRBW = Double.parseDouble(val); }
					else if (key.equals("StorageWBW")) { this.StorageWBW = Double.parseDouble(val); }
					else if (key.equals("StorageRLatency")) { this.StorageRLatency = Integer.parseInt(val); }
					else if (key.equals("StorageWLatency")) { this.StorageWLatency = Integer.parseInt(val); }
					else if (key.equals("StorageRAIDLevel")) { this.StorageRAIDLevel = Integer.parseInt(val); }
					else if (key.equals("CostPerGBMonth")) { this.CostPerGBMonth = Double.parseDouble(val); }
					else if (key.equals("DataIntegrity")) { this.DataIntegrity = Integer.parseInt(val); }
					else if (key.equals("StorageReserved")) { this.StorageReserved = Integer.parseInt(val); }
					else if (key.equals("StorageUsed")) { this.StorageUsed = Integer.parseInt(val); }
					else if (key.equals("StorageReliability")) { this.StorageReliability = Integer.parseInt(val); }
					else if (key.equals("ContainerAvailability")) { this.ContainerAvailability = Integer.parseInt(val); }
					else if (key.equals("StorageRBW_dyn")) { this.StorageRBW_dyn = Double.parseDouble(val); }
					else if (key.equals("StorageWBW_dyn")) { this.StorageWBW_dyn = Double.parseDouble(val); }
					else if (key.equals("PhysicalLocation")) { this.PhysicalLocation = val; }
					else if (key.equals("NetworkAddress")) { this.NetworkAddress = val; }
					else if (key.equals("StatusPath")) { this.StatusPath = val; }
					else {
						System.out.println("(qm) Warning: unrecognized status key: " + key);
						return false;
					}
				}
			}
			if (this.ContainerId.equals("")) return false;
			else return true;
		}

		public String to_string() {
			String status_str = "# Container status file. Generated by qos-manager.\n"
					+ "ContainerId"          + ", " + this.ContainerId           + "\n"
					+ "StorageTotal"         + ", " + this.StorageTotal          + "\t# MB\n"
					+ "PathToSwitch"         + ", " + this.PathToSwitch          + "\n"
					+ "CoresAvailable"       + ", " + this.CoresAvailable        + "\n"
					+ "StorageRBW"           + ", " + this.StorageRBW            + "\t# MB/s\n"
					+ "StorageWBW"           + ", " + this.StorageWBW            + "\t# MB/s\n"
					+ "StorageRLatency"      + ", " + this.StorageRLatency       + "\t# uSec\n"
					+ "StorageWLatency"      + ", " + this.StorageWLatency       + "\t# uSec\n"
					+ "StorageRAIDLevel"     + ", " + this.StorageRAIDLevel      + "\n"
					+ "CostPerGBMonth"       + ", " + this.CostPerGBMonth        + "\t# $\n"
					+ "DataIntegrity"        + ", " + this.DataIntegrity         + "\t# 0:worst\n"
					+ "StorageReserved"      + ", " + this.StorageReserved       + "\t# MB\n"
					+ "StorageUsed"          + ", " + this.StorageUsed           + "\t# MB\n"
					+ "StorageReliability"   + ", " + this.StorageReliability    + "\n"
					+ "ContainerAvailability"+ ", " + this.ContainerAvailability + "\n"
					+ "StorageRBW_dyn"       + ", " + this.StorageRBW_dyn        + "\t# MB/s\n"
					+ "StorageWBW_dyn"       + ", " + this.StorageWBW_dyn        + "\t# MB/s\n"
					+ "PhysicalLocation"     + ", " + this.PhysicalLocation      + "\n"
					+ "NetworkAddress"       + ", " + this.NetworkAddress        + "\n"
					+ "StatusPath"           + ", " + this.StatusPath            + "\n";
			return status_str;
		}

		public boolean read_from_file(String status_path) {
			char[] data = new char[ByteIOConstants.PREFERRED_SIMPLE_XFER_BLOCK_SIZE];
			int read;
			InputStream in = null;
			InputStreamReader reader = null;
			String status_str = "";

			try {
				GeniiPath path = new GeniiPath(status_path);
				if (!path.exists())
					throw new FileNotFoundException(String.format("Unable to find status file %s!", path));
				if (!path.isFile())
					throw new IOException(String.format("Status path %s is not a file!", path));

				in = path.openInputStream();
				reader = new InputStreamReader(in);

				while ((read = reader.read(data, 0, data.length)) > 0) {
					String s = new String(data, 0, read);
					status_str += s;
				}

				if (status_str != "") {
					boolean succ = this.parse_string(status_str);
					if (succ) {
						System.out.println("(qm) Get container status from " + status_path);
						this.StatusPath = "grid:" + path.lookupRNS();
						return true;
					}
				}
			} catch (Exception e) {
				System.out.println(e.toString());
				status_str = "";
			} finally {
				StreamUtils.close(reader);
				StreamUtils.close(in);
			}

			System.out.println("(qm) Fail to get container status from " + status_path);
			return false;
		}

		@SuppressWarnings("unused")
		public boolean write_to_file(String file_path) {
			System.out.println("(qm) NYI.");
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

		public String to_sql_string() {
			String status_sql_str = "'" + this.ContainerId + "',"
					+ this.StorageTotal + ",'" + this.PathToSwitch + "',"
					+ this.CoresAvailable + "," + this.StorageRBW + ","
					+ this.StorageWBW + "," + this.StorageRLatency + ","
					+ this.StorageWLatency + "," + this.StorageRAIDLevel + ","
					+ this.CostPerGBMonth + "," + this.DataIntegrity + ","
					+ this.StorageReserved + "," + this.StorageUsed + ","
					+ this.StorageReliability + "," + this.ContainerAvailability + ","
					+ this.StorageRBW_dyn + "," + this.StorageWBW_dyn + ","
					+ "'" + this.PhysicalLocation + "',"
					+ "'" + this.NetworkAddress + "','" + this.StatusPath + "'";
			return status_sql_str;
		}
	}

	/**************************************************************************
	 *  QoS Manager Entry
	 **************************************************************************/
	public void qos_manager(String arg) throws IOException
	{
		boolean succ;
		if (_spec_id_to_schedule != null) {
			System.out.println("(qm) main: Schedule a QoS specs id "
					+ _spec_id_to_schedule);
			succ = db_sync_down();
			if (succ) {
				schedule(null, _spec_id_to_schedule);
				db_sync_up();
			}
		} else if (_spec_id_to_remove != null) {
			System.out.println("(qm) main: Remove a QoS specs id "
					+ _spec_id_to_remove);
			succ = db_sync_down();
			if (succ) {
				db_remove_spec(_spec_id_to_remove);
				db_sync_up();
			}
		} else if (_status_path_to_add != null) {
			System.out.println("(qm) main: Add a container with status file at "
					+ _status_path_to_add);
			ContainerStatus status = new ContainerStatus();
			succ = status.read_from_file(_status_path_to_add);
			if (succ) {
				succ = db_sync_down();
				if (succ) {
					db_update_container(status, true); // init
					db_sync_up();
				}
			}
		} else if (_container_id_to_remove != null) {
			System.out.println("(qm) main: Remove container id "
					+ _container_id_to_remove);
			succ = db_sync_down();
			if (succ) {
				db_remove_container(_container_id_to_remove);
				db_sync_up();
			}
		} else if (_show_db) {
			System.out.println("(qm) main: Show information of the QoS database.");
			succ = db_sync_down();
			if (succ) {
				db_summary(false);
			}
		} else if (_show_db_verbose) {
			System.out.println("(qm) main: Show details of the QoS database.");
			succ = db_sync_down();
			if (succ) {
				db_summary(true);
			}
		} else if (_init_db) {
			System.out.println("(qm) main: Initialize the QoS database.");
			GeniiPath dbFile = new GeniiPath(db_grid_path());
			if (dbFile.exists()) {
				System.out.println("(qm) Warning: QoS database already exists. To rebuild an empty");
				System.out.println("     QoS database, please remove grid:" + db_grid_path());
			} else {
				db_init();
				db_sync_up();
			}
		} else if (_monitor) {
			System.out.println("(qm) main: Monitor container status and specs.");
			succ = db_sync_down();
			if (succ) {
				monitor_all();
				db_sync_up();
			}
		} else if (_spec_template) {
			QosSpec spec = new QosSpec();
			System.out.println(spec.to_string());
		} else if (_status_template) {
			ContainerStatus status = new ContainerStatus();
			System.out.println(status.to_string());
		} else if (_test) {
			System.out.println("(qm) internal: Test the QoS manager.");
			succ = db_sync_down();
			if (succ) {
				test_db();
				db_sync_up();
			}
		} else {
			System.out.println("(qm) main: Please run 'man qos-manager' for usable options.");
		}
	}

	/**************************************************************************
	 *  QoS Database Interfaces
	 **************************************************************************/
	private String db_grid_path() {
		if (this._gridHomeDir == null) {
			Map<String, String> env = GridUserEnvironment.getGridUserEnvironment();
			this._gridHomeDir = env.get("HOME");
		}
		return this._gridHomeDir + "/" + this._qosDbName;
	}

	private String db_local_path() {
		if (this._localUserDir == null) {
			this._localUserDir = InstallationProperties.getUserDir();
		}
		return this._localUserDir + "/" + this._qosDbName;
	}

	private boolean db_sync_down() {
		GeniiPath dbFile = new GeniiPath(db_grid_path());
		if (!dbFile.exists()) {
			System.out.println("(qm) db: Please run 'qos-manager --init-db' to initialize the QoS database.");
			return false;
		}
		System.out.println("(qm) db: Sync from grid to local.");
		PathOutcome po = CopyTool.copy("grid:" + db_grid_path(),
				"local:" + db_local_path(), false, true, null, stderr);
		if (PathOutcome.OUTCOME_SUCCESS.differs(po)) {
			System.out.println(po.toString());
			return false;
		} else {
			return true;
		}
	}

	private boolean db_sync_up() {
		File dbFile = new File(db_local_path());
		if (!dbFile.exists()) {
			System.out.println("(qm) db: Error: Cannot find local:" + db_local_path());
			return false;
		}
		System.out.println("(qm) db: Sync from local to grid.");
		PathOutcome po = CopyTool.copy("local:" + db_local_path(),
				"grid:" + db_grid_path(), false, true, null, stderr);
		if (PathOutcome.OUTCOME_SUCCESS.differs(po)) {
			System.out.println(po.toString());
			return false;
		} else {
			return true;
		}
	}

	private void db_destroy() {
		// The qos.db file in grid home directory should be removed by hand
		GeniiPath dbFileGrid = new GeniiPath(db_grid_path());
		assert(!dbFileGrid.exists());

		File dbFileLocal = new File(db_local_path());
		if (dbFileLocal.exists()) {
			dbFileLocal.delete();
		}
	}

	private boolean db_init() {
		db_destroy();

		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			System.out.println("(qm) db: Connect to QoS DB successfully.");
			stmt = conn.createStatement();

			QosSpec spec = new QosSpec();
			String create_spec_table = "CREATE TABLE" + spec.get_sql_header();
			ContainerStatus status = new ContainerStatus();
			String create_status_table = "CREATE TABLE" + status.get_sql_header();

			stmt.executeUpdate(create_spec_table);
			stmt.executeUpdate(create_status_table);

			String sql = "CREATE TABLE Relationships(SpecId TEXT, ContainerId TEXT, UNIQUE(SpecId, ContainerId) ON CONFLICT REPLACE);";
			stmt.executeUpdate(sql);
			stmt.close();
			conn.close();
			return true;
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
		}
		return false;
	}

	private void db_summary(boolean verbose) {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());

			System.out.println("----------------------------------------");
			if (verbose == true) {
				System.out.println("(qm) db: QoS Database Details:");

				System.out.println("  ** QOS DB PATHS:");
				System.out.println("   - grid:" + db_grid_path());
				System.out.println("   - local:" + db_local_path());

				stmt = conn.createStatement();

				System.out.print("  ** SPECIFICATIONS: ");
				String sql = "SELECT * FROM Specifications;";
				ResultSet rs_spec = stmt.executeQuery(sql);
				ResultSetMetaData rsmd_spec = rs_spec.getMetaData();

				int numberOfColumns_spec = rsmd_spec.getColumnCount();

				for (int i = 1; i <= numberOfColumns_spec; i++) {
					if (i > 1) System.out.print(", ");
					String columnName_spec = rsmd_spec.getColumnName(i);
					System.out.print(columnName_spec);
				}
				System.out.println("");

				String[] spec_prefix = {"","(A 0.)","(R 0.)","","","DI ","","","",""};
				String[] spec_suffix = {"","",""," MB"," MB","","","","",""};
				while (rs_spec.next()) {
					System.out.print("   - ");
					for (int i = 1; i <= numberOfColumns_spec; i++) {
						if (i > 1) System.out.print(", ");
						System.out.print(spec_prefix[i - 1]);
						String columnValue_spec = rs_spec.getString(i);
						System.out.print(columnValue_spec);
						System.out.print(spec_suffix[i - 1]);
					}
					System.out.println("");
				}

				System.out.print("  ** CONTAINERS: ");
				sql = "SELECT * FROM Containers;";
				ResultSet rs_container = stmt.executeQuery(sql);
				ResultSetMetaData rsmd_container = rs_container.getMetaData();

				int numberOfColumns_container = rsmd_container.getColumnCount();

				for (int i = 1; i <= numberOfColumns_container; i++) {
					if (i > 1) System.out.print(", ");
					String columnName_container = rsmd_container.getColumnName(i);
					System.out.print(columnName_container);
				}
				System.out.println("");

				String[] status_prefix = {"","","","","","","","","RAID ","$","DI ","","","(R 0.)","(A 0.)","","","","",""};
				String[] status_suffix = {""," MB",""," cores"," MB/s"," MB/s"," us"," us","","/GB/Month",""," MB"," MB","",""," MB/s"," MB/s","","",""};
				while (rs_container.next()) {
					System.out.print("   - ");
					for (int i = 1; i <= numberOfColumns_container; i++) {
						if (i > 1) System.out.print(", ");
						System.out.print(status_prefix[i - 1]);
						String columnValue_container = rs_container.getString(i);
						System.out.print(columnValue_container);
						System.out.print(status_suffix[i - 1]);
					}
					System.out.println("");
				}

				System.out.print("  ** RELATIONSHIPS: ");
				sql = "SELECT * FROM Relationships;";
				ResultSet rs_relationship = stmt.executeQuery(sql);
				ResultSetMetaData rsmd_relationship = rs_container.getMetaData();

				int numberOfColumns_relationship = rsmd_relationship.getColumnCount();

				for (int i = 1; i <= numberOfColumns_relationship; i++) {
					if (i > 1) System.out.print(", ");
					String columnName_relationship = rsmd_relationship.getColumnName(i);
					System.out.print(columnName_relationship);
				}
				System.out.println("");

				while (rs_relationship.next()) {
					System.out.print("   - ");
					for (int i = 1; i <= numberOfColumns_relationship; i++) {
						if (i > 1) System.out.print(" -> ");
						String columnValue_relationship = rs_relationship.getString(i);
						System.out.print(columnValue_relationship);
					}
					System.out.println("");
				}

			} else { // verbose == false
				System.out.println("(qm) db: QoS Database Summary:");

				System.out.println("  ** QOS DB PATHS:");
				System.out.println("   - grid:" + db_grid_path());
				System.out.println("   - local:" + db_local_path());

				stmt = conn.createStatement();

				String sql = "SELECT SpecId FROM Specifications;";
				ResultSet rs_spec = stmt.executeQuery(sql);

				System.out.print("  ** SPECIFICATIONS:\t");
				while (rs_spec.next()) {
					String columnValue_spec = rs_spec.getString(1);
					System.out.print(" " + columnValue_spec + ";");
				}
				System.out.println("");

				sql = "SELECT ContainerId FROM Containers;";
				ResultSet rs_container = stmt.executeQuery(sql);

				System.out.print("  ** CONTAINERS:\t");
				while (rs_container.next()) {
					String columnValue_container = rs_container.getString(1);
					System.out.print(" " + columnValue_container + ";");
				}
				System.out.println("");

				sql = "SELECT * FROM Relationships;";
				ResultSet rs_relationship = stmt.executeQuery(sql);

				System.out.print("  ** RELATIONSHIPS:\t");
				while (rs_relationship.next()) {
					String spec_id = rs_relationship.getString(1);
					String container_id = rs_relationship.getString(2);
					System.out.print(" " + spec_id + " -> " + container_id + ";");
				}
				System.out.println("");
			}

			System.out.println("----------------------------------------");
			stmt.close();
			conn.close();

		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	private boolean db_update_container(ContainerStatus status, boolean init) {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			assert (status != null);

			// TODO: check satisfiability of specs related to this container.
			if (init) {
				String sql = "SELECT * FROM Containers WHERE ContainerId = '" + status.ContainerId + "';";
				ResultSet rs = stmt.executeQuery(sql);

				if (rs.next()) {
					//container already exists
					System.out.println("(qm) db: Container: " + status.ContainerId + " already exists.");
					System.out.println("(qm) db: Update status for " + status.ContainerId);
					String sql_storagereserved = "SELECT StorageReserved FROM Containers WHERE ContainerId = '" + status.ContainerId + "';";
					ResultSet rs_reserved = stmt.executeQuery(sql_storagereserved);
					status.StorageReserved = rs_reserved.getInt(1);

					String sql_delete = "DELETE FROM Containers WHERE ContainerId = '" + status.ContainerId + "';";
					stmt.executeUpdate(sql_delete);
					String status_sql_str = status.to_sql_string();
					sql = "INSERT INTO Containers VALUES (" + status_sql_str + ");";
					stmt.executeUpdate(sql);

				}
				else{
					// Insert new container
					System.out.println("(qm) db: Insert status of new container: " + status.ContainerId);
					String status_sql_str = status.to_sql_string();
					String sql_insert = "INSERT INTO Containers VALUES (" + status_sql_str + ");";
					stmt.executeUpdate(sql_insert);
				}
			} else {
				// Update existing container
				System.out.println("(qm) db: Update status of container: " + status.ContainerId);

				String sql_storagereserved = "SELECT StorageReserved FROM Containers WHERE ContainerId = '" + status.ContainerId + "';";
				ResultSet rs_reserved = stmt.executeQuery(sql_storagereserved);
				status.StorageReserved = rs_reserved.getInt(1);

				String sql = "DELETE FROM Containers WHERE ContainerId = '" + status.ContainerId + "';";
				stmt.executeUpdate(sql);
				String status_sql_str = status.to_sql_string();
				sql = "INSERT INTO Containers VALUES (" + status_sql_str + ");";
				stmt.executeUpdate(sql);
			}
			stmt.close();
			conn.close();
			return true;
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return false;
	}

	private boolean db_add_scheduled_spec(QosSpec spec,
			List<String> scheduled_container_ids, boolean init)
	{
		Connection conn = null;
		Statement stmt = null;

		assert(spec != null);
		String spec_sql_str = spec.to_sql_string();

		// TODO: file copy
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			if (init) {
				// Insert new spec
				System.out.println("(qm) db: Insert scheduled spec: " + spec.SpecId);

				String sql = "INSERT INTO Specifications VALUES (" + spec_sql_str + ");";
				stmt.executeUpdate(sql);

				for (int i = 0; i < scheduled_container_ids.size(); i++) {
					sql = "INSERT INTO Relationships VALUES ('" + spec.SpecId + "','"
							+ scheduled_container_ids.get(i) + "');";
					String con_storagereserved = "SELECT StorageReserved FROM Containers WHERE ContainerId = '"
							+ scheduled_container_ids.get(i) + "';";
					ResultSet con_reserved = stmt.executeQuery(con_storagereserved);
					int container_storagereserved = con_reserved.getInt(1) + spec.ReservedSize;

					String sql_update_rstorage = "UPDATE Containers SET StorageReserved = " + container_storagereserved
							+ " where ContainerID = '" + scheduled_container_ids.get(i) + "';";
					stmt.executeUpdate(sql);
					stmt.executeUpdate(sql_update_rstorage);
				}
			} else {
				// Update existing spec
				List<String> container_ids_old = new ArrayList<String>();
				container_ids_old = db_get_container_ids_for_spec(spec.SpecId);

				System.out.println("(qm) db: Update scheduled spec: " + spec.SpecId);

				String old_spec = "SELECT ReservedSize FROM Specifications WHERE SpecId = '"
						+ spec.SpecId + "';";
				ResultSet old_reserved = stmt.executeQuery(old_spec);
				int old_spec_reserved = old_reserved.getInt(1);

				String sql = "DELETE FROM Relationships WHERE SpecId = '" + spec.SpecId + "';";
				stmt.executeUpdate(sql);

				sql = "DELETE FROM Specifications WHERE SpecId = '" + spec.SpecId + "';";
				stmt.executeUpdate(sql);

				sql = "INSERT INTO Specifications VALUES (" + spec_sql_str + ");";
				stmt.executeUpdate(sql);

				for (int i = 0; i < scheduled_container_ids.size(); i++) {

					if (container_ids_old.contains(scheduled_container_ids.get(i))) {
						// a container both in old and new
						String con_storagereserved = "SELECT StorageReserved FROM Containers WHERE ContainerId = '"
								+ scheduled_container_ids.get(i) + "';";
						ResultSet con_reserved = stmt.executeQuery(con_storagereserved);
						int container_storagereserved = con_reserved.getInt(1) + spec.ReservedSize - old_spec_reserved;

						String sql_update_rstorage = "UPDATE Containers SET StorageReserved = " + container_storagereserved
								+ " where ContainerID = '" + scheduled_container_ids.get(i) + "';";
						stmt.executeUpdate(sql_update_rstorage);

						sql = "INSERT INTO Relationships VALUES ('" + spec.SpecId + "','"
								+ scheduled_container_ids.get(i) + "');";
						stmt.executeUpdate(sql);
					} else {
						// a container only in new: create and file copy
						String con_storagereserved = "SELECT StorageReserved FROM Containers WHERE ContainerId = '"
								+ scheduled_container_ids.get(i) + "';";
						ResultSet con_reserved = stmt.executeQuery(con_storagereserved);
						int container_storagereserved = con_reserved.getInt(1) + spec.ReservedSize;

						String sql_update_rstorage = "UPDATE Containers SET StorageReserved = " + container_storagereserved
								+ " where ContainerID = '" + scheduled_container_ids.get(i) + "';";
						stmt.executeUpdate(sql_update_rstorage);

						sql = "INSERT INTO Relationships VALUES ('" + spec.SpecId + "','"
								+ scheduled_container_ids.get(i) + "');";
						stmt.executeUpdate(sql);
						System.out.println("(qm) db : File copy for specification " + spec.SpecId + " to " + scheduled_container_ids.get(i));
					}
				}
				for (int i = 0; i <container_ids_old.size(); i++) {
					if (!scheduled_container_ids.contains(container_ids_old.get(i))) {
						String con_storagereserved = "SELECT StorageReserved FROM Containers WHERE ContainerId = '"
								+ container_ids_old.get(i) + "';";
						ResultSet con_reserved = stmt.executeQuery(con_storagereserved);
						int container_storagereserved = con_reserved.getInt(1) - old_spec_reserved;
						String sql_update_rstorage = "UPDATE Containers SET StorageReserved = " + container_storagereserved
								+ " where ContainerID = '" + container_ids_old.get(i) + "';";
						stmt.executeUpdate(sql_update_rstorage);
						System.out.println("(qm) db : Delete strorage of specification " + spec.SpecId + " on " + container_ids_old.get(i));
					}
				}
			}
			stmt.close();
			conn.close();
			return true;
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return false;
	}

	private boolean db_remove_spec(String spec_id) {
		System.out.println("(qm) db: Remove specification: " + spec_id);

		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT ReservedSize FROM Specifications WHERE SpecId = '" + spec_id + "';";
			ResultSet rs = stmt.executeQuery(sql);

			if (rs.next()) {
				int spec_reserved = rs.getInt(1);
				sql = "SELECT ContainerId FROM Relationships WHERE SpecId = '" + spec_id + "';";
				ResultSet rs_container = stmt.executeQuery(sql);

				List<String> container_list = new ArrayList<String>();
				while (rs_container.next()) {
					container_list.add(rs_container.getString(1));
				}

				for (int i = 0; i < container_list.size(); i++) {
					sql = "SELECT StorageReserved FROM Containers WHERE ContainerId = '" + container_list.get(i) + "';";
					ResultSet rs_reserved = stmt.executeQuery(sql);
					int storage_reserved = rs_reserved.getInt(1);
					storage_reserved -= spec_reserved;
					sql = "UPDATE Containers SET StorageReserved = " + storage_reserved
							+ " WHERE ContainerId =" + "'" + container_list.get(i) +"';";
					stmt.executeUpdate(sql);
				}

				sql = "DELETE FROM Specifications WHERE SpecId = '" + spec_id + "';";
				stmt.executeUpdate(sql);
				sql = "DELETE FROM Relationships WHERE SpecId = '" + spec_id + "';";
				stmt.executeUpdate(sql);
			}
			stmt.close();
			conn.close();
			return true;
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return false;
	}

	private boolean db_remove_container(String container_id) {
		System.out.println("(qm) db: Remove container: " + container_id);

		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT * FROM Containers WHERE ContainerId = '" + container_id + "';";
			ResultSet rs = stmt.executeQuery(sql);

			if (rs.next()) {
				sql = "SELECT * FROM Relationships WHERE ContainerId = '" + container_id + "';";
				ResultSet rs_specs = stmt.executeQuery(sql);

				if (rs_specs.next()) {
					System.out.println("(qm) db: ERROR: specifications on container are not rescheduled.");
					return false;
				}

				sql = "DELETE FROM Containers WHERE ContainerId = '" + container_id + "';";
				stmt.executeUpdate(sql);
			}
			else {
				System.out.println("(qm) db: " + container_id + " does not exist in db.");
			}
			stmt.close();
			conn.close();
			return true;
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return false;
	}

	private List<String> db_get_container_ids_for_spec(String spec_id) {
		System.out.println("(qm) db: Get container ids for specification: " + spec_id);
		List<String> container_ids = new ArrayList<String>();

		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT ContainerId FROM Relationships WHERE SpecId = '" + spec_id + "';";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				container_ids.add(rs.getString(1));
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return container_ids;
	}

	private List<String> db_get_spec_ids_on_container(String container_id) {
		System.out.println("(qm) db: Get spec ids on container: " + container_id);
		List<String> spec_ids = new ArrayList<String>();

		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT SpecId FROM Relationships WHERE ContainerId = '" + container_id + "';";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				spec_ids.add(rs.getString(1));
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return spec_ids;
	}

	private List<String> db_get_container_id_list() {
		System.out.println("(qm) db: Get container id list. ");
		List<String> container_ids = new ArrayList<String>();

		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT ContainerId From Containers;";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				container_ids.add(rs.getString(1));
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return container_ids;
	}

	private List<String> db_get_spec_id_list() {
		System.out.println("(qm) db: Get specification id list. ");
		List<String> spec_ids = new ArrayList<String>();

		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT SpecId From Specifications;";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				spec_ids.add(rs.getString(1));
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return spec_ids;
	}

	private ContainerStatus db_get_status(String container_id) {
		System.out.println("(qm) db: Get container stautus for: " + container_id);
		ContainerStatus status = null;

		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT * FROM Containers WHERE ContainerId = '" + container_id + "';";
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				status = new ContainerStatus(rs);
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return status;
	}

	private QosSpec db_get_spec(String spec_id) {
		System.out.println("(qm) db: Get specification contents for: " + spec_id);
		QosSpec spec = null;

		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT * FROM Specifications WHERE SpecId = '" + spec_id + "';";
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				spec = new QosSpec(rs);
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(-1);
		}
		return spec;
	}

	private void test_db() {
		System.out.println("#### DB Test 0: Show empty QoS database");
		db_init();
		db_summary(false);
		db_summary(true);

		System.out.println("#### DB Test 1: Insert container1 into db");
		ContainerStatus status = new ContainerStatus();
		status.ContainerId = "container1";
		status.NetworkAddress = "//aaa";
		db_update_container(status, true);
		db_summary(true);

		System.out.println("#### DB Test 2: Insert container2 into db");
		status.ContainerId = "container2";
		status.NetworkAddress = "//bbb";
		db_update_container(status, true);
		db_summary(true);

		System.out.println("#### DB Test 3: Update container1 into db");
		status.ContainerId = "container1";
		status.NetworkAddress = "//ccc";
		status.StorageTotal = 1000;
		db_update_container(status, true);
		db_summary(true);

		System.out.println("#### DB Test 4: Add a scheduled spec");
		QosSpec spec = new QosSpec();
		spec.SpecId = "client1-spec1";
		spec.ReservedSize = 10;
		List<String> container_list = new ArrayList<String>();
		container_list.add("container1");
		db_add_scheduled_spec(spec, container_list, true);
		db_summary(true);

		System.out.println("#### DB Test 5: Update a scheduled spec");
		spec.SpecId = "client1-spec1";
		spec.ReservedSize = 20;
		container_list.clear();
		container_list.add("container1");
		container_list.add("container2");
		db_add_scheduled_spec(spec, container_list, false);
		db_summary(true);

		System.out.println("#### DB Test 6: Update a scheduled spec");
		spec.SpecId = "client1-spec1";
		spec.ReservedSize = 30;
		container_list.clear();
		container_list.add("container2");
		db_add_scheduled_spec(spec, container_list, false);
		db_summary(true);

		System.out.println("#### DB Test 7: Remove a spec");
		db_remove_spec("client1-spec1");
		db_summary(true);

		System.out.println("#### DB Test 8: Add a scheduled spec");
		spec.SpecId = "client1-spec1";
		container_list.clear();
		container_list.add("container1");
		container_list.add("container2");
		db_add_scheduled_spec(spec, container_list, true);
		db_summary(false);
		db_summary(true);

		System.out.println("#### DB Test 9: Get information from db");
		System.out.println(db_get_container_ids_for_spec("client1-spec1").toString());
		System.out.println(db_get_spec_ids_on_container("container1").toString());
		System.out.println(db_get_container_id_list().toString());
		System.out.println(db_get_spec_id_list().toString());
		status = db_get_status("xxx");
		if (status != null) System.out.println("Error");
		status = db_get_status("container1");
		if (status != null) System.out.println(status.to_sql_string());
		else System.out.println("Error");

		spec = db_get_spec("xxx");
		if (spec != null) System.out.println("Error");
		spec = db_get_spec("client1-spec1");
		if (spec != null) System.out.println(spec.to_sql_string());
		else System.out.println("Error");
	}

	/**************************************************************************
	 *  QoS Checker
	 **************************************************************************/
	// Check if disk space is satisfied
	// Rule: for every container, free space >= (client reserved - client used)
	private boolean check_space(QosSpec spec, List<ContainerStatus> status_list) {
		for (int i = 0; i < status_list.size(); i++) {
			ContainerStatus status = status_list.get(i);
			if (status.StorageTotal - status.StorageUsed < spec.ReservedSize - spec.UsedSize) {
				return false;
			}
		}
		return status_list.size() > 0;
	}

	// Reliability rule: All container together should satisfy the spec
	private boolean check_reliability(QosSpec spec, List<ContainerStatus> status_list) {
		double spec_reliability = Double.parseDouble("0." + Integer.toString(spec.Reliability));
		double container_failure = 1.0;
		for (int i = 0; i < status_list.size(); i++) {
			ContainerStatus status = status_list.get(i);
			container_failure = container_failure *
					(1 - Double.parseDouble("0." + Integer.toString(status.StorageReliability)));
		}
		if (1 - container_failure >= spec_reliability) {
			return true;
		} else {
			return false;
		}
	}

	// Availability rule: All container together should satisfy the spec
	private boolean check_availability(QosSpec spec, List<ContainerStatus> status_list) {
		double spec_availability = Double.parseDouble("0." + Integer.toString(spec.Availability));
		double container_unavailable = 1.0;
		for (int i = 0; i < status_list.size(); i++) {
			ContainerStatus status = status_list.get(i);
			container_unavailable = container_unavailable *
					(1 - Double.parseDouble("0." + Integer.toString(status.ContainerAvailability)));
		}
		if (1 - container_unavailable >= spec_availability) {
			return true;
		} else {
			return false;
		}
	}

	// Bandwidth rule: Even if there are multiple replications, client only use
	// one container for the spec. Only check the primary container.
	boolean check_bandwidth(QosSpec spec, List<ContainerStatus> status_list) {
		if (spec.Bandwidth == "Low") {
			return true;
		} else {
			double BW_threshold = 5.0; // assume 5 MB/s is the threshold
			// the first container is the primary container to use
			ContainerStatus primary = status_list.get(0);
			double free_RBW = primary.StorageRBW - primary.StorageRBW_dyn;
			double free_WBW = primary.StorageWBW - primary.StorageWBW_dyn;
			if (free_RBW >= BW_threshold && free_WBW >= BW_threshold) {
				return true;
			} else {
				return false;
			}
		}
	}

	// Rule: Every container should satisfy the data integrity requirement.
	boolean check_dataintegrity(QosSpec spec, List<ContainerStatus> status_list) {
		for (int i = 0; i < status_list.size(); i++) {
			ContainerStatus status = status_list.get(i);
			if (status.DataIntegrity < spec.DataIntegrity) {
				return false;
			}
		}
		return true;
	}

	// check latency using physical location, assuming if first two levels
	// are the same, the latency can be satisfied; only check the primary
	boolean check_latency(QosSpec spec, List<ContainerStatus> status_list) {
		if (spec.Latency == "High") {
			return true;
		} else {
			String[] spec_location = spec.PhysicalLocations.split("/");
			String[] container_location = status_list.get(0).PhysicalLocation.split("/");
			if (spec_location[1].toLowerCase().equals(container_location[1].toLowerCase()) &&
					spec_location[2].toLowerCase().equals(container_location[2].toLowerCase())) {
				return true;
			} else {
				return false;
			}
		}
	}

	// QoS Checker main entry: Check if a list of container can satisfy a spec
	boolean check_all(QosSpec spec, List<ContainerStatus> status_list) {
		System.out.println("(qm) checker: Check satisfiability of spec " + spec.SpecId +
				" on containers " + status_list.toString());
		boolean satisfied = true;
		if (status_list.size() == 0) { // not scheduled
			return false;
		}
		// Check disk space
		satisfied = satisfied && check_space(spec, status_list);
		if (!satisfied) {
			System.out.println("(qm) checker:  Disk space not satisfied for spec: " + spec.SpecId);
		}
		// Check data integrity
		satisfied = satisfied && check_dataintegrity(spec, status_list);
		if (!satisfied) {
			System.out.println("(qm) checker:  Dataintegrity not satisfied for spec: " + spec.SpecId);
		}
		// Check reliability
		satisfied = satisfied && check_reliability(spec, status_list);
		if (!satisfied) {
			System.out.println("(qm) checker:  Reliability not satisfied for spec: " + spec.SpecId);
		}
		// Check availability
		satisfied = satisfied && check_availability(spec, status_list);
		if (!satisfied) {
			System.out.println("(qm) checker:  Availability not satisfied for spec: " + spec.SpecId);
		}
		// Check bandwidth
		satisfied = satisfied && check_bandwidth(spec, status_list);
		if (!satisfied) {
			System.out.println("(qm) checker:  Bandwidth not satisfied for spec: " + spec.SpecId);
		}
		// Check latency
		satisfied = satisfied && check_latency(spec, status_list);
		if (!satisfied) {
			System.out.println("(qm) checker:  Latency not satisfied for spec: " + spec.SpecId);
		}
		if (satisfied) {
			System.out.println("(qm) checker:  Spec " + spec.SpecId + " is satisfied on " + status_list.toString());
		}
		return satisfied;
	}

	/**************************************************************************
	 *  QoS Scheduler
	 **************************************************************************/
	// Schedule filter: filter out some single containers
	boolean schedule_filter(QosSpec spec, ContainerStatus status) {
		if (status.ContainerAvailability <= 0) return false;
		if (status.StorageReliability <= 0) return false;
		List<ContainerStatus> tmp = new ArrayList<ContainerStatus>();
		tmp.add(status);
		if (!check_space(spec, tmp)) return false;
		if (!check_dataintegrity(spec, tmp)) return false;
		return true;
	}

	// return a list of scheduled container ids
	public List<String> schedule(String spec_path, String spec_id) {
		assert(spec_path == null && spec_id != null || spec_path != null && spec_id == null);
		List<String> scheduled_containers = new ArrayList<String>();
		QosSpec spec = null;
		if (spec_path != null) {
			spec = new QosSpec();
			boolean succ = spec.read_from_file(spec_path);
			if (!succ) spec = null;
		} else {
			spec = db_get_spec(spec_id);
		}
		if (spec == null) {
			System.out.println("(qm) Error: spec not available.");
			return scheduled_containers;
		}
		List<String> container_ids = db_get_container_id_list();
		List<ContainerStatus> status_list = new ArrayList<ContainerStatus>();
		// filter out some containers
		List<ContainerStatus> tmp = new ArrayList<ContainerStatus>();
		for (int i = 0; i < container_ids.size(); i++) {
			ContainerStatus status = db_get_status(container_ids.get(i));
			if (schedule_filter(spec, status)) {
				status_list.add(status);
			}
		}
		boolean scheduled = false;
		// try a single container
		for (int i = 0; i < status_list.size(); i++) {
			tmp.clear();
			tmp.add(status_list.get(i));
			if (check_all(spec, tmp)) {
				scheduled = true;
				break;
			}
		}
		// try 2 containers
		if (!scheduled) {
			for (int i = 0; i < status_list.size(); i++) {
				for (int j = i; j < status_list.size(); j++) {
					tmp.clear();
					tmp.add(status_list.get(i));
					tmp.add(status_list.get(j));
					if (check_all(spec, tmp)) {
						scheduled = true;
						break;
					}
				}
			}
		}
		// try 3 containers
		if (!scheduled) {
			for (int i = 0; i < status_list.size(); i++) {
				for (int j = i; j < status_list.size(); j++) {
					for (int k = j; k < status_list.size(); k++) {
						tmp.clear();
						tmp.add(status_list.get(i));
						tmp.add(status_list.get(j));
						tmp.add(status_list.get(k));
						if (check_all(spec, tmp)) {
							scheduled = true;
							break;
						}
					}
				}
			}
		}
		// try 4 containers
		if (!scheduled) {
			for (int i = 0; i < status_list.size(); i++) {
				for (int j = i; j < status_list.size(); j++) {
					for (int k = j; k < status_list.size(); k++) {
						for (int l = k; l < status_list.size(); l++) {
							tmp.clear();
							tmp.add(status_list.get(i));
							tmp.add(status_list.get(j));
							tmp.add(status_list.get(k));
							tmp.add(status_list.get(l));
							if (check_all(spec, tmp)) {
								scheduled = true;
								break;
							}
						}
					}
				}
			}
		}
		if (scheduled) {
			double costs = 0;
			for (ContainerStatus status: tmp) {
				scheduled_containers.add(status.ContainerId);
				costs += status.CostPerGBMonth;
			}
			double cost = costs / 1024.0 * spec.ReservedSize;
			System.out.println("(qm) Schedule results: " + scheduled_containers.toString());
			System.out.printf("(qm) Cost: $%.2f/month", cost);
		}
		return scheduled_containers;
	}

	/**************************************************************************
	 *  QoS Monitors
	 **************************************************************************/
	// Monitor a qos spec
	private boolean monitor_spec(String spec_id) {
		List<String> container_ids = db_get_container_ids_for_spec(spec_id);
		QosSpec spec = db_get_spec(spec_id);
		List<ContainerStatus> status_list = new ArrayList<ContainerStatus>();
		for (int i = 0; i < container_ids.size(); i++) {
			status_list.add(db_get_status(container_ids.get(i)));
		}
		boolean satisfied = check_all(spec, status_list);
		if (!satisfied) {
			// reschedule
			List<String> rescheduled = schedule(null, spec_id);
			db_add_scheduled_spec(spec, rescheduled, false); //update
		}
		return true;
	}

	// Monitor qos specs related to a container
	private boolean monitor_specs_on_container(String container_id) {
		List<String> spec_ids = db_get_spec_ids_on_container(container_id);
		for (int i = 0; i < spec_ids.size(); i++) {
			monitor_spec(spec_ids.get(i));
		}
		return true;
	}

	// Update status of a container to qos database
	private boolean monitor_container(String container_id) {
		ContainerStatus status_in_db = db_get_status(container_id);
		assert(status_in_db != null);
		ContainerStatus status_remote = new ContainerStatus();
		boolean succ = status_remote.read_from_file(status_in_db.StatusPath);
		if (!succ) {
			System.out.println("(qm) monitor: Container " + container_id +
					" is not available.");
			// TODO: update database and reschedule
			return false;
		} else {
			// Avoid overwriting the reserved size (container doesn't know this)
			status_remote.StorageReserved = status_in_db.StorageReserved;
			db_update_container(status_remote, false);
			// Call QoS Monitor
			System.out.println("(qm) monitor: Call QoS Monitor for " + container_id);
			monitor_specs_on_container(container_id);
		}
		return true;
	}

	// Monitor all containers in the qos database.
	private boolean monitor_all() {
		List<String> container_ids = db_get_container_id_list();
		for (int i = 0; i < container_ids.size(); i++) {
			monitor_container(container_ids.get(i));
		}
		return true;
	}
}
