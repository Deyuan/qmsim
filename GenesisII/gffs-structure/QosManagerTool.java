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
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.axis.message.MessageElement;
import org.apache.axis.types.URI;
import org.ggf.rns.LookupResponseType;
import org.morgan.util.io.StreamUtils;
import org.oasis_open.docs.wsrf.rl_2.Destroy;
import org.oasis_open.docs.wsrf.rp_2.GetResourcePropertyDocument;
import org.oasis_open.docs.wsrf.rp_2.InsertResourceProperties;
import org.oasis_open.docs.wsrf.rp_2.InsertType;
import org.oasis_open.docs.wsrf.rp_2.UpdateResourceProperties;
import org.oasis_open.docs.wsrf.rp_2.UpdateType;
import org.ws.addressing.EndpointReferenceType;

import edu.virginia.vcgr.genii.client.InstallationProperties;
import edu.virginia.vcgr.genii.client.byteio.ByteIOConstants;
import edu.virginia.vcgr.genii.client.cache.unified.CacheManager;
import edu.virginia.vcgr.genii.client.cmd.ReloadShellException;
import edu.virginia.vcgr.genii.client.cmd.ToolException;
import edu.virginia.vcgr.genii.client.comm.ClientUtils;
import edu.virginia.vcgr.genii.client.context.ContextManager;
import edu.virginia.vcgr.genii.client.context.GridUserEnvironment;
import edu.virginia.vcgr.genii.client.context.ICallingContext;
import edu.virginia.vcgr.genii.client.dialog.UserCancelException;
import edu.virginia.vcgr.genii.client.gpath.GeniiPath;
import edu.virginia.vcgr.genii.client.gpath.GeniiPathType;
import edu.virginia.vcgr.genii.client.io.LoadFileResource;
import edu.virginia.vcgr.genii.client.naming.ResolverDescription;
import edu.virginia.vcgr.genii.client.naming.ResolverUtils;
import edu.virginia.vcgr.genii.client.naming.WSName;
import edu.virginia.vcgr.genii.client.resource.IResource;
import edu.virginia.vcgr.genii.client.resource.ResourceException;
import edu.virginia.vcgr.genii.client.resource.TypeInformation;
import edu.virginia.vcgr.genii.client.rns.GeniiDirPolicy;
import edu.virginia.vcgr.genii.client.rns.PathOutcome;
import edu.virginia.vcgr.genii.client.rns.RNSConstants;
import edu.virginia.vcgr.genii.client.rns.RNSException;
import edu.virginia.vcgr.genii.client.rns.RNSPath;
import edu.virginia.vcgr.genii.client.rns.RNSPathQueryFlags;
import edu.virginia.vcgr.genii.client.rp.ResourcePropertyException;
import edu.virginia.vcgr.genii.client.security.axis.AuthZSecurityException;
import edu.virginia.vcgr.genii.client.security.axis.ResourceSecurityPolicy;
import edu.virginia.vcgr.genii.client.sync.SyncProperty;
import edu.virginia.vcgr.genii.common.GeniiCommon;
import edu.virginia.vcgr.genii.common.rfactory.VcgrCreate;
import edu.virginia.vcgr.genii.common.rfactory.VcgrCreateResponse;
import edu.virginia.vcgr.genii.resolver.GeniiResolverPortType;
import edu.virginia.vcgr.genii.resolver.UpdateResponseType;

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

	private String _spec_id_to_remove = null;
	private String _directory_to_remove = null;
	private String _status_path_to_add = null;
	private String _container_id_to_remove = null;
	private boolean _show_db = false;
	private boolean _show_db_verbose = false;
	private boolean _init_db = false;
	private boolean _monitor = false;
	private boolean _clean_replicas = false;
	private boolean _spec_template = false;
	private String _status_template = null;
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

	@Option({ "rm-spec" })
	public void set_rm_spec(String spec_id)
	{
		_spec_id_to_remove = spec_id;
	}

	@Option({ "rm-directory" })
	public void set_rm_directory(String directory)
	{
		_directory_to_remove = directory;
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

	@Option({ "clean-replicas" })
	public void set_clean_replicas()
	{
		_clean_replicas = true;
	}

	@Option({ "spec-template" })
	public void set_spec_template()
	{
		_spec_template = true;
	}

	@Option({ "status-template" })
	public void set_status_template(String rns_path)
	{
		_status_template = rns_path;
	}

	@Option({ "test" })
	public void set_test()
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

	/**
	 * A class for representing the QoS specifications, including some file
	 * I/O and SQL utilities.
	 */
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
						System.out.println("(qm) Read QoS specs from " + spec_path);
						this.SpecPath = "grid:" + path.lookupRNS();
						return true;
					}
				}
			} catch (Exception e) {
				System.out.println(e.getClass().getName() + ": " + e.getMessage());
				spec_str = "";
			} finally {
				StreamUtils.close(reader);
				StreamUtils.close(in);
			}

			System.out.println("(qm) Fail to read QoS specs from " + spec_path);
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

	/**
	 * A class for representing the status of a container, including some file
	 * I/O and SQL utilities.
	 */
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
		public String RnsPath = "";           // (str) container rns service
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
				this.RnsPath = rs.getString(19);
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
					else if (key.equals("RnsPath")) { this.RnsPath = val; }
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
					+ "RnsPath"              + ", " + this.RnsPath               + "\n"
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
						System.out.println("(qm) Read container status from " + status_path);
						this.StatusPath = "grid:" + path.lookupRNS();
						// For relative RNS path, the base path should be the
						// StatusPath.
						if (this.RnsPath.charAt(0) == '/') {
							GeniiPath rns = new GeniiPath(this.RnsPath);
							this.RnsPath = "grid:" + rns.lookupRNS();
						} else {
							int i;
							for (i = this.StatusPath.length() - 1; i >= 0; i--) {
								if (this.StatusPath.charAt(i) == '/') break;
							}
							String base = this.StatusPath.substring(0, i + 1);
							GeniiPath rns = new GeniiPath(base + this.RnsPath);
							this.RnsPath = "grid:" + rns.lookupRNS();
						}
						return true;
					}
				}
			} catch (Exception e) {
				System.out.println(e.getClass().getName() + ": " + e.getMessage());
				status_str = "";
			} finally {
				StreamUtils.close(reader);
				StreamUtils.close(in);
			}

			System.out.println("(qm) Fail to read container status from " + status_path);
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
					"RnsPath"               + " TEXT," +
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
					+ "'" + this.RnsPath + "','" + this.StatusPath + "'";
			return status_sql_str;
		}
	}

	/**************************************************************************
	 *  QoS Manager Entry
	 **************************************************************************/

	/**
	 * The main entry of the qos-manager. Options are handled here.
	 * @param arg
	 * @throws IOException
	 */
	public void qos_manager(String arg) throws IOException
	{
		boolean succ;
		if (_spec_id_to_remove != null) {
			System.out.println("(qm) main: Remove a QoS specs id "
					+ _spec_id_to_remove);
			succ = db_sync_down();
			succ = succ && db_remove_spec(_spec_id_to_remove);
			succ = succ && db_sync_up();
		} else if (_directory_to_remove != null) {
			System.out.println("(qm) main: Remove a directory "
					+ _directory_to_remove);
			succ = db_sync_down();
			succ = succ && db_remove_directory(_directory_to_remove);
			succ = succ && db_sync_up();
		} else if (_status_path_to_add != null) {
			System.out.println("(qm) main: Add a container with status file at "
					+ _status_path_to_add);
			ContainerStatus status = new ContainerStatus();
			succ = status.read_from_file(_status_path_to_add);
			if (succ) {
				if (is_rns_valid(status.RnsPath) && is_rns_available(status.RnsPath)) {
					succ = db_sync_down();
					if (succ) {
						ContainerStatus status_db = db_get_status(status.ContainerId);
						if (status_db == null) {
							succ = db_update_container(status, true); // init
						} else {
							succ = db_update_container(status, false); // update
						}
						succ = succ && db_sync_up();
					}
				} else {
					System.out.println("(qm) main: " + _status_path_to_add
							+ " is not added to the QoS database.");
				}
			}
		} else if (_container_id_to_remove != null) {
			System.out.println("(qm) main: Remove container id "
					+ _container_id_to_remove);
			succ = db_sync_down();
			if (succ) {
				ContainerStatus status = db_get_status(_container_id_to_remove);
				if (status != null) {
					// Set availability to 0
					status.ContainerAvailability = 0;
					succ = succ && db_update_container(status, false);
					// Reschedule
					succ = succ && monitor_container(_container_id_to_remove);
					// Remove container
					succ = succ && db_remove_container(_container_id_to_remove);
					succ = succ && db_sync_up();
				}
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
			String db_grid_path = db_get_grid_path();
			if (db_grid_path == null) return;
			GeniiPath dbFile = new GeniiPath(db_grid_path);
			if (dbFile.exists()) {
				System.out.println("(qm) Warning: QoS database already exists. To rebuild an empty");
				System.out.println("     QoS database, please remove grid:" + db_grid_path);
			} else {
				succ = db_destroy();
				succ = succ && db_init();
				succ = succ && db_sync_up();
			}
		} else if (_monitor) {
			System.out.println("(qm) main: Monitor container status and specs.");
			succ = db_sync_down();
			succ = succ && monitor_all();
			succ = succ && db_sync_up(); // Sync up partial results when failure?
		} else if (_clean_replicas) {
			System.out.println("(qm) main: Cleaning all unused replicas.");
			succ = db_sync_down();
			succ = succ && clean_replicas();
			succ = succ && db_sync_up();
		} else if (_spec_template) {
			QosSpec spec = new QosSpec();
			System.out.println(spec.to_string());
		} else if (_status_template != null) {
			ContainerStatus status = gen_status_template(_status_template);
			if (status != null) {
				System.out.println(status.to_string());
			}
		} else if (_test) { // internal
			System.out.println("(qm) internal: Test the QoS manager.");
			// Should not sync up or down.
			test_db();
		} else {
			System.out.println("(qm) main: Please run 'man qos-manager' for usable options.");
		}
	}

	/**************************************************************************
	 *  QoS Database Interfaces
	 **************************************************************************/

	/**
	 * QoS DB: Get the path of the qos.db file in grid home directory.
	 * @return
	 */
	private String db_get_grid_path() {
		if (this._gridHomeDir == null) {
			Map<String, String> env = GridUserEnvironment.getGridUserEnvironment();
			this._gridHomeDir = env.get("HOME");
			if (this._gridHomeDir == null) {
				System.out.println("(qm) Error: HOME variable undefined.");
				return null;
			}
		}
		return this._gridHomeDir + "/" + this._qosDbName;
	}

	/**
	 * QoS DB: Get the path of the qos.db file in user local directory.
	 * @return
	 */
	private String db_get_local_path() {
		if (this._localUserDir == null) {
			this._localUserDir = InstallationProperties.getUserDir();
			if (this._localUserDir == null) {
				System.out.println("(qm) Error: Cannot get user local directory.");
				return null;
			}
		}
		return this._localUserDir + "/" + this._qosDbName;
	}

	/**
	 * QoS DB: Synchronize the qos.db in grid home directory to local.
	 * @return
	 */
	private boolean db_sync_down() {
		String db_grid_path = db_get_grid_path();
		String db_local_path = db_get_local_path();
		if (db_grid_path == null || db_local_path == null) {
			return false;
		}
		GeniiPath dbFile = new GeniiPath(db_grid_path);
		if (!dbFile.exists()) {
			System.out.println("(qm) db: Please run 'qos-manager --init-db' to initialize the QoS database.");
			return false;
		}
		System.out.println("(qm) db: Sync from grid to local.");
		PathOutcome po = CopyTool.copy("grid:" + db_grid_path,
				"local:" + db_local_path, false, true, null, stderr);
		if (PathOutcome.OUTCOME_SUCCESS.differs(po)) {
			System.out.println(po.toString());
			return false;
		} else {
			return true;
		}
	}

	/**
	 * QoS DB: Synchronize the local qos.db to grid home directory.
	 * @return
	 */
	private boolean db_sync_up() {
		String db_grid_path = db_get_grid_path();
		String db_local_path = db_get_local_path();
		if (db_grid_path == null || db_local_path == null) {
			return false;
		}
		File dbFile = new File(db_local_path);
		if (!dbFile.exists()) {
			System.out.println("(qm) db: Error: Cannot find local:" + db_local_path);
			return false;
		}
		System.out.println("(qm) db: Sync from local to grid.");
		PathOutcome po = CopyTool.copy("local:" + db_local_path,
				"grid:" + db_grid_path, false, true, null, stderr);
		if (PathOutcome.OUTCOME_SUCCESS.differs(po)) {
			System.out.println(po.toString());
			return false;
		} else {
			return true;
		}
	}

	/**
	 * QoS DB: Destroy the local qos.db file.
	 * @return
	 */
	private boolean db_destroy() {
		String db_grid_path = db_get_grid_path();
		String db_local_path = db_get_local_path();
		if (db_grid_path == null || db_local_path == null) {
			return false;
		}
		GeniiPath dbFileGrid = new GeniiPath(db_grid_path);
		// The qos.db file in grid home directory should be removed by hand
		assert(dbFileGrid.exists() == false);

		File dbFileLocal = new File(db_local_path);
		if (dbFileLocal.exists()) {
			dbFileLocal.delete();
		}
		return true;
	}

	/**
	 * QoS DB: Initialize the DB. If the qos.db file already exists in user
	 * home directory, this file should be removed manually.
	 * @return
	 */
	private boolean db_init() {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			System.out.println("(qm) db: Connect to QoS DB successfully.");
			stmt = conn.createStatement();

			QosSpec spec = new QosSpec();
			String create_spec_table = "CREATE TABLE" + spec.get_sql_header();
			ContainerStatus status = new ContainerStatus();
			String create_status_table = "CREATE TABLE" + status.get_sql_header();

			stmt.executeUpdate(create_spec_table);
			stmt.executeUpdate(create_status_table);

			// ReplicaFlag: 1 primary, 0 replica, -1 none
			// ResolverFlag: 1 resolver, 0 not
			// ReplicaId: non-negative: actual id, negative: (abs - 1) id to remove
			// ON CONFLICT REPLACE?
			String sql = "CREATE TABLE Relationships(Directory TEXT, SpecId TEXT, ContainerId TEXT, ReplicaFlag INT, ResolverFlag INT, ReplicaId INT, UNIQUE(Directory, SpecId, ContainerId), UNIQUE(Directory, ReplicaId));";
			stmt.executeUpdate(sql);

			stmt.close();
			conn.close();
			return true;
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
		}
		return false;
	}

	/**
	 * QoS DB: Print out summary of the DB.
	 * @param verbose
	 */
	private void db_summary(boolean verbose) {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());

			System.out.println("----------------------------------------");
			if (verbose == true) {
				System.out.println("(qm) db: QoS Database Details:");

				System.out.println("  ** QOS DB PATHS:");
				System.out.println("   - grid:" + db_get_grid_path());
				System.out.println("   - local:" + db_get_local_path());

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

				String h = rsmd_relationship.getColumnName(1) + ", ";
				h += rsmd_relationship.getColumnName(4) + " ([P]Primary, [R]Replica, [-]None), ";
				h += rsmd_relationship.getColumnName(6) + " (x-unused), ";
				h += rsmd_relationship.getColumnName(5) + " ([R]Resolver, [-]None), ";
				h += rsmd_relationship.getColumnName(2) + ", ";
				h += rsmd_relationship.getColumnName(3);
				System.out.println(h);

				while (rs_relationship.next()) {
					String r = "   - " + rs_relationship.getString(1) + " ";
					int rep = rs_relationship.getInt(4);
					if (rep == 1) r += "[P]";
					else if (rep == 0) r += "[R]";
					else r += "[-]";
					r += "[";
					int replica_id = rs_relationship.getInt(6);
					if (replica_id < 0) {
						r += "x-";
						replica_id = - replica_id - 1;
					}
					r += "" + replica_id + "] ";
					int res = rs_relationship.getInt(5);
					if (res == 1) r += "[R]";
					else r += "[-]";
					r += " (" + rs_relationship.getString(2) + ", "
							+ rs_relationship.getString(3) + ")";
					System.out.println(r);
				}

			} else { // verbose == false
				System.out.println("(qm) db: QoS Database Summary:");

				System.out.println("  ** QOS DB PATHS:");
				System.out.println("   - grid:" + db_get_grid_path());
				System.out.println("   - local:" + db_get_local_path());

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

				System.out.println("  ** RELATIONSHIPS:");
				while (rs_relationship.next()) {
					String r = "   - " + rs_relationship.getString(1) + " ";
					int rep = rs_relationship.getInt(4);
					if (rep == 1) r += "[P]";
					else if (rep == 0) r += "[R]";
					else r += "[-]";
					int res = rs_relationship.getInt(5);
					if (res == 1) r += "[R]";
					else r += "[-]";
					r += " (" + rs_relationship.getString(2) + ", "
							+ rs_relationship.getString(3) + ")";
					System.out.println(r);
				}
			}

			System.out.println("----------------------------------------");
			stmt.close();
			conn.close();

		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	/**
	 * QoS DB: Update the status of a container in DB.
	 * @param status
	 * @param init
	 * @return
	 */
	private boolean db_update_container(ContainerStatus status, boolean init) {
		assert (status != null);
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT * FROM Containers WHERE ContainerId = '" + status.ContainerId + "';";
			ResultSet rs = stmt.executeQuery(sql);

			if (rs.next()) { // exist
				if (init) { // error
					System.out.println("(qm) db: Error: Container: " + status.ContainerId + " already exists.");
					return false;
				} else { // update
					System.out.println("(qm) db: Update status of container: " + status.ContainerId);

					// Maintain the reserved size
					sql = "SELECT StorageReserved FROM Containers WHERE ContainerId = '" + status.ContainerId + "';";
					ResultSet rs_reserved = stmt.executeQuery(sql);
					status.StorageReserved = rs_reserved.getInt(1);

					sql = "DELETE FROM Containers WHERE ContainerId = '" + status.ContainerId + "';";
					stmt.executeUpdate(sql);
					sql = "INSERT INTO Containers VALUES (" + status.to_sql_string() + ");";
					stmt.executeUpdate(sql);
				}
			} else { // not exist
				if (init) { // insert
					System.out.println("(qm) db: Insert status of new container: " + status.ContainerId);
					sql = "INSERT INTO Containers VALUES (" + status.to_sql_string() + ");";
					stmt.executeUpdate(sql);
				} else { // error
					System.out.println("(qm) db: Error: Cannot update a not exist container.");
					return false;
				}
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			return false;
		}
		return true;
	}


	/**
	 * QoS DB: Update a spec in DB.
	 * Caller should be responsible for updating the reserved size for all
	 * related containers.
	 * @param status
	 * @param init
	 * @return
	 */
	private boolean db_update_spec(QosSpec spec, boolean init) {
		assert (spec != null);
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT * FROM Specifications WHERE SpecId = '" + spec.SpecId + "';";
			ResultSet rs = stmt.executeQuery(sql);

			if (rs.next()) { // exist
				if (init) { // error
					System.out.println("(qm) db: Error: Spec: " + spec.SpecId + " already exists.");
					return false;
				} else { // update
					System.out.println("(qm) db: Update spec: " + spec.SpecId);
					sql = "DELETE FROM Specifications WHERE SpecId = '" + spec.SpecId + "';";
					stmt.executeUpdate(sql);
					sql = "INSERT INTO Specifications VALUES (" + spec.to_sql_string() + ");";
					stmt.executeUpdate(sql);
				}
			} else { // not exist
				if (init) { // insert
					System.out.println("(qm) db: Insert a new spec: " + spec.SpecId);
					sql = "INSERT INTO Specifications VALUES (" + spec.to_sql_string() + ");";
					stmt.executeUpdate(sql);
				} else { // error
					System.out.println("(qm) db: Error: Cannot update a not exist spec.");
					return false;
				}
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * QoS DB: Add a scheduled directory into the DB.
	 * @param mkdir_path
	 * @param spec
	 * @param scheduled_container_ids
	 * @param init
	 * @return
	 */
	private boolean db_add_scheduled_directory(String mkdir_path, QosSpec spec,
			List<String> scheduled_container_ids, boolean init)
	{
		assert(mkdir_path != null && spec != null);
		assert(scheduled_container_ids != null && scheduled_container_ids.size() > 0);
		Connection conn = null;
		Statement stmt = null;

		GeniiPath dir = new GeniiPath(mkdir_path);
		mkdir_path = "grid:" + dir.lookupRNS();
		System.out.println("(qm) db: Add scheduled directory: " + mkdir_path +
				" (" + spec.SpecId + ", " + scheduled_container_ids.toString() + ")");

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			List<String> container_ids_old = db_rel_query(RelQuery.CONTAINERS_RELATED_TO_DIR, mkdir_path);
			if (container_ids_old.size() > 0) { // if directory exists in db
				if (init == true) { // init
					System.out.println("(qm) db: Error: directory should not exist if it is created for the first time.");
					return false;
				} else { // update
					//check if spec exists
					String sql = "SELECT * FROM Specifications WHERE SpecId = '" + spec.SpecId + "';";
					ResultSet rs = stmt.executeQuery(sql);
					if (rs.next()) { // if spec exists in db
						String old_spec = "SELECT ReservedSize FROM Specifications WHERE SpecId = '"
								+ spec.SpecId + "';";
						ResultSet old_reserved = stmt.executeQuery(old_spec);
						int old_spec_reserved = old_reserved.getInt(1);

						sql = "SELECT ContainerId FROM Relationships WHERE Directory = '" + mkdir_path + "' AND ResolverFlag = 1;";
						rs = stmt.executeQuery(sql);
						String resolver_id = null;
						if (rs.next()) {
							resolver_id = rs.getString(1);
						}

						for (int i = 0; i < scheduled_container_ids.size(); i++) {
							if (container_ids_old.contains(scheduled_container_ids.get(i))) {
								// a container both in old and new
								//update reserved storage
								String con_storagereserved = "SELECT StorageReserved FROM Containers WHERE ContainerId = '"
										+ scheduled_container_ids.get(i) + "';";
								ResultSet con_reserved = stmt.executeQuery(con_storagereserved);
								int container_storagereserved = con_reserved.getInt(1) + spec.ReservedSize - old_spec_reserved;
								String sql_update_rstorage = "UPDATE Containers SET StorageReserved = " + container_storagereserved
										+ " where ContainerID = '" + scheduled_container_ids.get(i) + "';";
								stmt.executeUpdate(sql_update_rstorage);

								String sql_replicaid = "SELECT ReplicaId FROM Relationships WHERE ContainerId = '"
										+ scheduled_container_ids.get(i) + "' AND " + "Directory = '" + mkdir_path + "';";
								ResultSet rs_old_replicaid = stmt.executeQuery(sql_replicaid);
								int replicaid = rs_old_replicaid.getInt(1);

								// Update Relationships and set ReplicaId, ReplicaFlag and ResolverFlag
								int ReplicaFlag = (i == 0 ? 1 : 0);
								int ResolverFlag = 0; // determine which container is the resolver
								if (resolver_id == null) {
									if (scheduled_container_ids.size() > 1 && i == 1) {
										ResolverFlag = 1;
									}
								} else {
									if (scheduled_container_ids.get(i).equals(resolver_id)) {
										ResolverFlag = 1;
									}
								}
								int ReplicaId = replicaid;
								sql = "UPDATE Relationships SET ReplicaFlag = " + ReplicaFlag
										+ ", ResolverFlag = " + ResolverFlag
										+ ", ReplicaId = " + ReplicaId
										+ " Where Directory = '" + mkdir_path
										+ "' AND ContainerId = '" + scheduled_container_ids.get(i) + "';";
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

								// get existing max replica id
								int max_replica_id = -1;
								String sql_replicaid = "SELECT ReplicaId FROM Relationships WHERE Directory = '" + mkdir_path + "';";
								ResultSet rs_replicaid = stmt.executeQuery(sql_replicaid);
								while (rs_replicaid.next()) {
									int id = rs_replicaid.getInt(1);
									if (id < 0) id = -id - 1;
									if (max_replica_id < id) max_replica_id = id;
								}

								// Insert to Relationships and set ReplicaId, ReplicaFlag and ResolverFlag
								int ReplicaFlag = (i == 0 ? 1 : 0);
								int ResolverFlag = 0; // determine which container is the resolver
								if (resolver_id == null) {
									if (scheduled_container_ids.size() > 1 && i == 1) {
										ResolverFlag = 1;
									}
								} else {
									if (scheduled_container_ids.get(i).equals(resolver_id)) {
										ResolverFlag = 1;
									}
								}
								int ReplicaId = max_replica_id + 1;

								sql = "INSERT INTO Relationships VALUES ('"
										+ mkdir_path + "', '" + spec.SpecId + "', '"
										+ scheduled_container_ids.get(i)+ "' ,"
										+ ReplicaFlag + "," + ResolverFlag + "," + ReplicaId + ");";
								stmt.executeUpdate(sql);
							}
						}
						//for old containers which do not exist in new scheduled containers
						for (int i = 0; i < container_ids_old.size(); i++) {
							if (!scheduled_container_ids.contains(container_ids_old.get(i))) {
								// Set replica ID to negative.
								// Users should call --clean-replica after file copy is done.
								sql = "SELECT ReplicaId FROM Relationships WHERE ContainerId = '"
										+ container_ids_old.get(i) + "' AND Directory = '" + mkdir_path + "';";
								rs = stmt.executeQuery(sql);
								int old_replica_id = rs.getInt(1);
								sql = "UPDATE Relationships SET ReplicaId = " + (-old_replica_id - 1)
										+ " where ContainerID = '" + container_ids_old.get(i)
										+ "' AND Directory = '" + mkdir_path + "';";
								stmt.executeUpdate(sql);
								// clean the primary flag
								sql = "UPDATE Relationships SET ReplicaFlag = " + 0
										+ " where ContainerID = '" + container_ids_old.get(i)
										+ "' AND Directory = '" + mkdir_path + "';";
								stmt.executeUpdate(sql);
							}
						}
					} else { // if spec does not exist in db
						System.out.println("(qm) db: Error: specification should exist for existing directory.");
						return false;
					}
				}
			} else { // if directory does not exist in db
				if (init == false) { // update
					System.out.println("(qm) db: Error: directory should exist in db.");
					return false;
				} else { // init
					//check if spec exists
					String sql = "SELECT * FROM Specifications WHERE SpecId = '" + spec.SpecId + "';";
					ResultSet rs = stmt.executeQuery(sql);
					if (rs.next()) {
						//update specification
						db_update_spec(spec, false);
					} else {
						//insert new specification
						sql = "INSERT INTO Specifications VALUES (" + spec.to_sql_string() + ");";
						stmt.executeUpdate(sql);
					}
					for (int i = 0; i < scheduled_container_ids.size(); i++) {
						//update container reserved size
						String con_storagereserved = "SELECT StorageReserved FROM Containers WHERE ContainerId = '"
								+ scheduled_container_ids.get(i) + "';";
						ResultSet con_reserved = stmt.executeQuery(con_storagereserved);
						int container_storagereserved = con_reserved.getInt(1) + spec.ReservedSize;
						String sql_update_rstorage = "UPDATE Containers SET StorageReserved = " + container_storagereserved
								+ " where ContainerID = '" + scheduled_container_ids.get(i) + "';";
						stmt.executeUpdate(sql_update_rstorage);
						//insert into relationships
						if (i == 0) {
							sql = "INSERT INTO Relationships VALUES ('" + mkdir_path + "','" + spec.SpecId + "', '" +scheduled_container_ids.get(0)+ "' ,"
									+ 1 + "," + 0 + "," + i + ");";
						} else if (i == 1) {
							sql = "INSERT INTO Relationships VALUES ('" + mkdir_path + "','" + spec.SpecId + "', '" +scheduled_container_ids.get(i)+ "' ,"
									+ 0 + "," + 1 + "," + i + ");";
						} else {
							sql = "INSERT INTO Relationships VALUES ('" + mkdir_path + "','" + spec.SpecId + "', '" +scheduled_container_ids.get(i)+ "' ,"
									+ 0 + "," + 0 + "," + i + ");";
						}
						stmt.executeUpdate(sql);
					}
				}
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * QoS DB: Remove a specification from the DB.
	 * All relationships related to this spec will be deleted. But the actual
	 * directories will not be deleted.
	 * @param spec_id
	 * @return
	 */
	private boolean db_remove_spec(String spec_id) {
		assert(spec_id != null);
		System.out.println("(qm) db: Remove specification: " + spec_id);
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT ReservedSize FROM Specifications WHERE SpecId = '" + spec_id + "';";
			ResultSet rs = stmt.executeQuery(sql);

			if (rs.next()) {
				int spec_reserved = rs.getInt(1);
				sql = "SELECT ContainerId FROM Relationships WHERE SpecId = '" + spec_id + "';";
				ResultSet rs_rel = stmt.executeQuery(sql);

				while (rs_rel.next()) {
					String container_id = rs_rel.getString(1);
					sql = "SELECT StorageReserved FROM Containers WHERE ContainerId = '" + container_id + "';";
					ResultSet rs_reserved = stmt.executeQuery(sql);
					int storage_reserved = rs_reserved.getInt(1);
					storage_reserved -= spec_reserved;
					sql = "UPDATE Containers SET StorageReserved = " + storage_reserved
							+ " WHERE ContainerId =" + "'" + container_id +"';";
					stmt.executeUpdate(sql);
				}

				sql = "DELETE FROM Specifications WHERE SpecId = '" + spec_id + "';";
				stmt.executeUpdate(sql);
				sql = "DELETE FROM Relationships WHERE SpecId = '" + spec_id + "';";
				stmt.executeUpdate(sql);
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			return false;
		}
		return true;
	}
	/**
	 * QoS DB: Remove a directory from the DB.
	 * All relationships related to this directory will be deleted. But the actual
	 * directories will not be deleted.
	 * @param dir
	 * @return
	 */
	private boolean db_remove_directory(String dir) {
		assert(dir != null);
		System.out.println("(qm) db: Remove directory: " + dir);
		Connection conn = null;
		Statement stmt = null;
		GeniiPath path = new GeniiPath(dir);
		dir = "grid:" + path.lookupRNS();

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			List<String> spec_ids = db_rel_query(RelQuery.SPECS_RELATED_TO_DIR, dir);
			if (spec_ids.size() == 0) {
				System.out.println("(qm) db: No record for directory: " + dir);
				stmt.close();
				conn.close();
				return true;
			}
			assert(spec_ids.size() == 1); // a directory should be only related to one spec
			String sql = "SELECT ReservedSize FROM Specifications WHERE SpecId = '" + spec_ids.get(0) + "';";
			ResultSet rs = stmt.executeQuery(sql);
			int spec_reserved = rs.getInt(1);
			List<String> container_ids = db_rel_query(RelQuery.CONTAINERS_RELATED_TO_DIR, dir);
			for (int i = 0; i < container_ids.size(); i++) {
				sql = "SELECT StorageReserved FROM Containers WHERE ContainerId = '" + container_ids.get(i) + "';";
				ResultSet rs_reserved = stmt.executeQuery(sql);
				int storage_reserved = rs_reserved.getInt(1);
				storage_reserved -= spec_reserved;
				sql = "UPDATE Containers SET StorageReserved = " + storage_reserved
							+ " WHERE ContainerId =" + "'" + container_ids.get(i) +"';";
				stmt.executeUpdate(sql);
			}

			sql = "DELETE FROM Relationships WHERE Directory = '" + dir + "';";
			stmt.executeUpdate(sql);

			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * QoS DB: Remove a container from the DB. There should be no directories
	 * scheduled on this container.
	 * @param container_id
	 * @return
	 */
	private boolean db_remove_container(String container_id) {
		assert(container_id != null);
		System.out.println("(qm) db: Remove container: " + container_id);
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT * FROM Containers WHERE ContainerId = '" + container_id + "';";
			ResultSet rs = stmt.executeQuery(sql);

			if (rs.next()) {
				sql = "SELECT ReplicaId FROM Relationships WHERE ContainerId = '" + container_id + "';";
				ResultSet rs_rel = stmt.executeQuery(sql);

				boolean has_replica = false;
				boolean has_valid_replica = false;
				while (rs_rel.next()) {
					has_replica = true;
					if (rs_rel.getInt(1) >= 0) has_valid_replica = true;
				}

				if (has_replica) {
					if (has_valid_replica) {
						System.out.println("(qm) db: ERROR: Specifications on container are not rescheduled.");
						return false;
					} else {
						System.out.println("(qm) db: Warning: Please do --clean-replicas before removing" + container_id);
						return true;
					}
				} else {
					sql = "DELETE FROM Containers WHERE ContainerId = '" + container_id + "';";
					stmt.executeUpdate(sql);
				}

			} else {
				System.out.println("(qm) db: " + container_id + " does not exist in db.");
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * QoS DB: Relationship query types
	 * Relationships:
	 * A Specification (can be used for creating) multiple Directories
	 * A Directory (can be replicated on) multiple Containers
	 */
	public enum RelQuery {
		SPECS_RELATED_TO_DIR,
		SPECS_RELATED_TO_CONTAINER,
		DIRS_RELATED_TO_SPEC,
		DIRS_RELATED_TO_CONTAINER,
		CONTAINERS_RELATED_TO_SPEC,
		CONTAINERS_RELATED_TO_DIR
	}

	/**
	 * QoS DB: Query a relationship.
	 * @param q
	 * @param id
	 * @return a list of strings
	 */
	private List<String> db_rel_query(RelQuery q, String id) {
		assert(q != null && id != null);
		Set<String> results = new HashSet<String>();
		Connection conn = null;
		Statement stmt = null;
		if (q == RelQuery.SPECS_RELATED_TO_DIR || q == RelQuery.CONTAINERS_RELATED_TO_DIR) {
			GeniiPath path = new GeniiPath(id);
			id = "grid:" + path.lookupRNS();
		}

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			String sql = null;
			if (q == RelQuery.SPECS_RELATED_TO_DIR) {
				sql = "SELECT SpecId FROM Relationships WHERE Directory =";
			} else if (q == RelQuery.SPECS_RELATED_TO_CONTAINER) {
				sql = "SELECT SpecId FROM Relationships WHERE ContainerId =";
			} else if (q == RelQuery.DIRS_RELATED_TO_SPEC) {
				sql = "SELECT Directory FROM Relationships WHERE SpecId =";
			} else if (q == RelQuery.DIRS_RELATED_TO_CONTAINER) {
				sql = "SELECT Directory FROM Relationships WHERE ContainerId =";
			} else if (q == RelQuery.CONTAINERS_RELATED_TO_SPEC) {
				sql = "SELECT ContainerId FROM Relationships WHERE SpecId =";
			} else if (q == RelQuery.CONTAINERS_RELATED_TO_DIR) {
				sql = "SELECT ContainerId FROM Relationships WHERE Directory =";
			}
			sql += " '" + id + "';";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				results.add(rs.getString(1));
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			results.clear();
		}

		return new ArrayList<String>(results);
	}

	/**
	 * QoS DB: Get the list of IDs of all containers.
	 * @return
	 */
	private List<String> db_get_container_id_list() {
		System.out.println("(qm) db: Get container id list. ");
		List<String> container_ids = new ArrayList<String>();
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
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
			container_ids.clear();
		}
		return container_ids;
	}

	/**
	 * QoS DB: Get the list of IDs of all specs.
	 * @return
	 */
	private List<String> db_get_spec_id_list() {
		System.out.println("(qm) db: Get specification id list. ");
		List<String> spec_ids = new ArrayList<String>();
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
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
			spec_ids.clear();
		}
		return spec_ids;
	}

	/**
	 * QoS DB: Get the list of directories.
	 * @return
	 */
	private List<String> db_get_dir_list() {
		System.out.println("(qm) db: Get directory list. ");
		Set<String> dirs = new HashSet<String>();
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			String sql = "SELECT Directory From Relationships;";
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				dirs.add(rs.getString(1));
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			dirs.clear();
		}
		return new ArrayList<String>(dirs);
	}

	/**
	 * QoS DB: Given a container ID, get its status in DB.
	 * @param container_id
	 * @return
	 */
	private ContainerStatus db_get_status(String container_id) {
		assert(container_id != null);
		ContainerStatus status = null;
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
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
			status = null;
		}
		System.out.println("(qm) db: Get container stautus of " + container_id
				+ " [" + (status == null ? "null" : "successful") + "]");
		return status;
	}

	/**
	 * QoS DB: Given a spec ID, get its spec in DB.
	 * @param spec_id
	 * @return
	 */
	private QosSpec db_get_spec(String spec_id) {
		assert(spec_id != null);
		QosSpec spec = null;
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
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
			spec = null;
		}
		System.out.println("(qm) db: Get specification contents of " + spec_id
				+ " [" + (spec == null ? "null" : "successful") + "]");
		return spec;
	}

	/**
	 * QoS DB: Given a container RNS path, get its container ID.
	 * @param container_rns
	 * @return
	 */
	private String db_get_container_id_from_rns(String container_rns) {
		assert(container_rns != null);
		String container_id = null;
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			GeniiPath path = new GeniiPath(container_rns);
			String sql = "SELECT ContainerId FROM Containers WHERE RnsPath = 'grid:" + path.lookupRNS() + "';";
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				container_id = rs.getString(1);
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			container_id = null;
		}
		System.out.println("(qm) db: Get container id for RNS: " + container_rns
				+ " [" + container_id + "]");
		return container_id;
	}

	/**
	 * QoS DB: Given a directory, get all replica ids.
	 * @param dir
	 * @return
	 */
	private List<Integer> db_get_replica_ids_for_dir(String dir) {
		assert(dir != null);
		List<Integer> replica_ids = new ArrayList<Integer>();
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			GeniiPath path = new GeniiPath(dir);
			String sql = "SELECT ReplicaId FROM Relationships WHERE Directory = 'grid:" + path.lookupRNS() + "';";
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				replica_ids.add(rs.getInt(1));
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			replica_ids.clear();
		}

		return replica_ids;
	}

	/**
	 * QoS DB: Given a directory and a replica ID, remove the record in DB.
	 * @param dir
	 * @return
	 */
	private boolean db_remove_replica_for_dir(String dir, int replica_id) {
		assert(dir != null);
		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + db_get_local_path());
			stmt = conn.createStatement();

			GeniiPath path = new GeniiPath(dir);
			String sql = "DELETE FROM Relationships WHERE Directory = 'grid:" + path.lookupRNS() + "' AND ReplicaId = " + replica_id + ";";
			stmt.executeQuery(sql);
			stmt.close();
			conn.close();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			return false;
		}

		return true;
	}

	/**
	 * QoS DB: Internal function for testing the QoS DB.
	 * Will not affect the qos.db in grid home directory.
	 */
	private void test_db() {
		// Starts from an empty local db
		String db_local_path = db_get_local_path();
		if (db_local_path == null) return;
		File dbFileLocal = new File(db_local_path);
		if (dbFileLocal.exists()) {
			dbFileLocal.delete();
		}

		System.out.println("#### DB Test 0: Show empty QoS database");
		db_init();
		db_summary(false);
		db_summary(true);

		System.out.println("#### DB Test 1: Insert container1 into db");
		ContainerStatus status = new ContainerStatus();
		status.ContainerId = "container1";
		status.RnsPath = "//aaa";
		db_update_container(status, true);
		db_summary(true);

		System.out.println("#### DB Test 2: Insert container2 into db");
		status.ContainerId = "container2";
		status.RnsPath = "//bbb";
		db_update_container(status, true);
		db_summary(true);

		System.out.println("#### DB Test 3: Update container1 into db");
		status.ContainerId = "container1";
		status.RnsPath = "//ccc";
		status.StorageTotal = 1000;
		db_update_container(status, false);
		db_summary(true);

		System.out.println("#### DB Test 4: Add a scheduled spec");
		QosSpec spec = new QosSpec();
		spec.SpecId = "client1-spec1";
		spec.ReservedSize = 10;
		List<String> container_list = new ArrayList<String>();
		container_list.add("container1");
		db_add_scheduled_directory("bck", spec, container_list, true);
		db_summary(true);

		System.out.println("#### DB Test 5: Update a scheduled spec");
		spec.SpecId = "client1-spec1";
		spec.ReservedSize = 20;
		container_list.clear();
		container_list.add("container1");
		container_list.add("container2");
		db_add_scheduled_directory("bck", spec, container_list, false);
		db_summary(true);

		System.out.println("#### DB Test 5+: Remove Directory");
		db_remove_directory("bck");
		db_summary(true);

		System.out.println("#### DB Test 6: Update a scheduled spec");
		spec.SpecId = "client1-spec1";
		spec.ReservedSize = 30;
		container_list.clear();
		container_list.add("container2");
		db_add_scheduled_directory("bck", spec, container_list, false);
		db_summary(true);

		System.out.println("#### DB Test 7: Remove a spec");
		db_remove_spec("client1-spec1");
		db_summary(true);

		System.out.println("#### DB Test 8: Add a scheduled spec");
		spec.SpecId = "client1-spec1";
		container_list.clear();
		container_list.add("container1");
		container_list.add("container2");
		db_add_scheduled_directory("bck", spec, container_list, true);
		db_summary(false);
		db_summary(true);

		System.out.println("#### DB Test 9: Get information from db");
		System.out.println(db_rel_query(RelQuery.CONTAINERS_RELATED_TO_SPEC, "client1-spec1").toString());
		System.out.println(db_rel_query(RelQuery.SPECS_RELATED_TO_CONTAINER, "container1").toString());
		System.out.println(db_rel_query(RelQuery.CONTAINERS_RELATED_TO_DIR, "bck").toString());
		System.out.println(db_rel_query(RelQuery.SPECS_RELATED_TO_DIR, "bck").toString());
		System.out.println(db_rel_query(RelQuery.DIRS_RELATED_TO_SPEC, "client1-spec1").toString());
		System.out.println(db_rel_query(RelQuery.DIRS_RELATED_TO_CONTAINER, "container1").toString());
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

		List<String> dirs = null;
		dirs = db_get_dir_list();
		if (dirs == null) System.out.println("Error");
		else System.out.println("Directories in DB: " + dirs.toString());
	}

	/**************************************************************************
	 *  QoS Checker
	 **************************************************************************/

	/**
	 * QoS Checker: Check space.
	 * Rule: for every container, free space >= (client reserved - client used)
	 * @param spec
	 * @param status_list
	 * @return
	 */
	private boolean check_space(QosSpec spec, List<ContainerStatus> status_list) {
		for (int i = 0; i < status_list.size(); i++) {
			ContainerStatus status = status_list.get(i);
			if (status.StorageTotal - status.StorageUsed < spec.ReservedSize - spec.UsedSize) {
				return false;
			}
		}
		return status_list.size() > 0;
	}

	/**
	 * Parse the availability and reliability. Add a leading 0 to an integer
	 * to get a double value.
	 * @param n
	 * @return
	 */
	private double parse_leading_zero(int n) {
		String s;
		if (n >= 0) {
			s = "0." + Integer.toString(n);
		} else {
			s = "-0." + Integer.toString(-n);
		}
		return Double.parseDouble(s);
	}

	/**
	 * QoS Checker: Check reliability.
	 * Rule: All container together should satisfy the spec.
	 * @param spec
	 * @param status_list
	 * @return
	 */
	private boolean check_reliability(QosSpec spec, List<ContainerStatus> status_list) {
		double spec_reliability = parse_leading_zero(spec.Reliability);
		double container_failure = 1.0;
		for (int i = 0; i < status_list.size(); i++) {
			ContainerStatus status = status_list.get(i);
			container_failure = container_failure *
					(1 - parse_leading_zero(status.StorageReliability));
		}
		if (1 - container_failure >= spec_reliability) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * QoS Checker: Check availability.
	 * Rule: All container together should satisfy the spec.
	 * @param spec
	 * @param status_list
	 * @return
	 */
	private boolean check_availability(QosSpec spec, List<ContainerStatus> status_list) {
		double spec_availability = parse_leading_zero(spec.Availability);
		double container_unavailable = 1.0;
		for (int i = 0; i < status_list.size(); i++) {
			ContainerStatus status = status_list.get(i);
			container_unavailable = container_unavailable *
					(1 - parse_leading_zero(status.ContainerAvailability));
		}
		if (1 - container_unavailable >= spec_availability) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * QoS Checker: Check bandwidth.
	 * Rule: Even if there are multiple replications, client only use one
	 * container for the spec. Only check the primary container.
	 * @param spec
	 * @param status_list
	 * @return
	 */
	private boolean check_bandwidth(QosSpec spec, List<ContainerStatus> status_list) {
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

	/**
	 * QoS Checker: Check data integrity.
	 * Rule: Every container should satisfy the data integrity requirement.
	 * @param spec
	 * @param status_list
	 * @return
	 */
	private boolean check_dataintegrity(QosSpec spec, List<ContainerStatus> status_list) {
		for (int i = 0; i < status_list.size(); i++) {
			ContainerStatus status = status_list.get(i);
			if (status.DataIntegrity < spec.DataIntegrity) {
				return false;
			}
		}
		return true;
	}

	/**
	 * QoS Checker: Check latency using physical location, assuming if first two
	 * levels are the same, the latency can be satisfied; only check the primary
	 * @param spec
	 * @param status_list
	 * @return
	 */
	private boolean check_latency(QosSpec spec, List<ContainerStatus> status_list) {
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

	/**
	 * QoS Checker: Specifications that only need to check the primary.
	 * @param spec
	 * @param status_list
	 * @param verbose
	 * @return
	 */
	private boolean check_first(QosSpec spec, List<ContainerStatus> status_list, boolean verbose) {
		// Check bandwidth
		if (!check_bandwidth(spec, status_list)) {
			if (verbose) System.out.println("(qm) checker: Bandwidth not satisfied for spec: " + spec.SpecId);
			return false;
		}
		// Check latency
		if (!check_latency(spec, status_list)) {
			if (verbose) System.out.println("(qm) checker: Latency not satisfied for spec: " + spec.SpecId);
			return false;
		}
		return true;
	}

	/**
	 * QoS Checker: Specifications that need to check each single one.
	 * @param spec
	 * @param status_list
	 * @param verbose
	 * @return
	 */
	private boolean check_each(QosSpec spec, List<ContainerStatus> status_list, boolean verbose) {
		// Check disk space
		if (!check_space(spec, status_list)) {
			if (verbose) System.out.println("(qm) checker: Disk space not satisfied for spec: " + spec.SpecId);
			return false;
		}
		// Check data integrity
		if (!check_dataintegrity(spec, status_list)) {
			if (verbose) System.out.println("(qm) checker: Dataintegrity not satisfied for spec: " + spec.SpecId);
			return false;
		}
		return true;
	}

	/**
	 * QoS Checker: Specifications that need to check all together.
	 * @param spec
	 * @param status_list
	 * @param verbose
	 * @return
	 */
	private boolean check_all(QosSpec spec, List<ContainerStatus> status_list, boolean verbose) {
		// Check reliability
		if (!check_reliability(spec, status_list)) {
			if (verbose) System.out.println("(qm) checker: Reliability not satisfied for spec: " + spec.SpecId);
			return false;
		}
		// Check availability
		if (!check_availability(spec, status_list)) {
			if (verbose) System.out.println("(qm) checker: Availability not satisfied for spec: " + spec.SpecId);
			return false;
		}
		return true;
	}

	/**
	 * QoS Checker: Check if a list of container can satisfy a spec
	 * @param spec
	 * @param status_list
	 * @return
	 */
	private boolean check_qos(QosSpec spec, List<ContainerStatus> status_list, boolean verbose) {
		List<String> container_ids = new ArrayList<String>();
		if (verbose) {
			for (int i = 0; i < status_list.size(); i++) {
				container_ids.add(status_list.get(i).ContainerId);
			}
			System.out.println("(qm) checker: Check satisfiability of spec "
					+ spec.SpecId + " on containers " + container_ids.toString());
		}
		if (status_list.size() == 0) return false;

		// filter out containers with availability = 0
		for (int i = status_list.size() - 1; i >= 0; i--) {
			if (status_list.get(i).ContainerAvailability <= 0) {
				status_list.remove(i);
			}
		}

		if (!check_each(spec, status_list, verbose)) return false;
		if (!check_all(spec, status_list, verbose)) return false;
		if (!check_first(spec, status_list, verbose)) return false;

		if (verbose) {
			System.out.println("(qm) checker: Spec " + spec.SpecId + " is satisfied on " + container_ids.toString());
		}
		return true;
	}

	/**************************************************************************
	 *  QoS Scheduler
	 **************************************************************************/

	/**
	 * QoS Scheduler: Schedule filter: filter out some single containers
	 * @param spec
	 * @param status
	 * @return
	 */
	boolean schedule_filter(QosSpec spec, ContainerStatus status) {
		if (status.ContainerAvailability <= 0) return false;
		if (status.StorageReliability <= 0) return false;
		List<ContainerStatus> tmp = new ArrayList<ContainerStatus>();
		tmp.add(status);
		if (!check_each(spec, tmp, false)) return false;
		return true;
	}

	/**
	 * QoS Scheduler: Schedule for a specification file
	 * @param spec_path
	 * @param spec_id
	 * @return a list of scheduled container RNS paths
	 */
	private List<String> schedule_internal(String spec_path, String spec_id) {
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
			if (check_qos(spec, tmp, false)) {
				scheduled = true;
				break;
			}
		}
		// try 2 containers
		if (!scheduled) {
			for (int i = 0; i < status_list.size(); i++) {
				tmp.clear();
				tmp.add(status_list.get(i));
				if (!check_first(spec, tmp, false)) continue;
				for (int j = 0; j < status_list.size(); j++) {
					if (j == i) continue;
					tmp.clear();
					tmp.add(status_list.get(i));
					tmp.add(status_list.get(j));
					if (check_all(spec, tmp, false)) {
						scheduled = true;
						break;
					}
				}
			}
		}
		// try 3 containers
		if (!scheduled) {
			for (int i = 0; i < status_list.size(); i++) {
				tmp.clear();
				tmp.add(status_list.get(i));
				if (!check_first(spec, tmp, false)) continue;
				for (int j = 0; j < status_list.size(); j++) {
					if (j == i) continue;
					for (int k = j + 1; k < status_list.size(); k++) {
						if (k == i) continue;
						tmp.clear();
						tmp.add(status_list.get(i));
						tmp.add(status_list.get(j));
						tmp.add(status_list.get(k));
						if (check_all(spec, tmp, false)) {
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
				tmp.clear();
				tmp.add(status_list.get(i));
				if (!check_first(spec, tmp, false)) continue;
				for (int j = 0; j < status_list.size(); j++) {
					if (j == i) continue;
					for (int k = j + 1; k < status_list.size(); k++) {
						if (k == i) continue;
						for (int l = k + 1; l < status_list.size(); l++) {
							if (l == i) continue;
							tmp.clear();
							tmp.add(status_list.get(i));
							tmp.add(status_list.get(j));
							tmp.add(status_list.get(k));
							tmp.add(status_list.get(l));
							if (check_all(spec, tmp, false)) {
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
				scheduled_containers.add(status.RnsPath);
				costs += status.CostPerGBMonth;
			}
			double cost = costs / 1024.0 * spec.ReservedSize;
			System.out.println("(qm) Schedule results: " + scheduled_containers.toString());
			System.out.printf("(qm) Cost: $%.2f/month \n", cost);
		}
		return scheduled_containers;
	}

	/**
	 * QoS Scheduler: Wrapper for calling the QoS scheduler from other files.
	 * @param spec_path
	 * @param spec_id
	 * @param target_path
	 * @return a list of scheduled container RNS paths
	 */
	public List<String> schedule_wrapper(String spec_path, String spec_id, String target_path) {
		assert(spec_path == null && spec_id != null || spec_path != null && spec_id == null);
		List<String> scheduled_containers = new ArrayList<String>();
		if (spec_path != null) {
			System.out.println("(qm) scheduler: Schedule a QoS spec file " + spec_path);
		} else if (spec_id != null) {
			System.out.println("(qm) scheduler: Reschedule a spec id " + spec_id);
		}
		boolean succ = db_sync_down();
		if (succ) {
			scheduled_containers = schedule_internal(spec_path, spec_id);
		}
		return scheduled_containers;
	}

	/**
	 * QoS Scheduler: Wrapper for committing the scheduling results to the QoS database.
	 * @param spec_path
	 * @param mkdir_path
	 * @param scheduled_rns
	 * @return
	 */
	public boolean commit_scheduling_results(String spec_path, String mkdir_path,
			List<String> scheduled_rns)
	{
		System.out.println("(qm) scheduler: Commit scheduling results.");
		boolean succ = db_sync_down();
		if (succ) {
			List<String> container_ids = new ArrayList<String>();
			for (String rns: scheduled_rns) {
				String id = db_get_container_id_from_rns(rns);
				if (id != null) {
					container_ids.add(id);
				} else {
					System.out.println("(qm) Error: Cannnot lookup container ID for RNS " + rns);
					succ = false;
					break;
				}
			}
			if (succ) {
				QosSpec spec = new QosSpec();
				succ = spec.read_from_file(spec_path);
				succ = succ && db_add_scheduled_directory(mkdir_path, spec, container_ids, true);
				succ = succ && db_sync_up();
			}
		}

		return succ;
	}

	/**************************************************************************
	 *  QoS Monitors
	 **************************************************************************/

	/**
	 * QoS Monitor: Check if the RNS is valid
	 * @param rns_path
	 * @return
	 */
	private boolean is_rns_valid(String rns_path) {
		GeniiPath path = new GeniiPath(rns_path);
		// should be a grid path
		if (path.pathType() != GeniiPathType.Grid) {
			System.out.println("(qm) Error: " + rns_path + " is not a grid path.");
			return false;
		}
		// should exist
		if (!path.exists()) {
			System.out.println("(qm) Error: " + rns_path + " does not exist.");
			return false;
		}
		// should be a directory
		if (!path.isDirectory()) {
			System.out.println("(qm) Error: " + rns_path + " is not a directory.");
			return false;
		}
		return true;
	}

	/**
	 * QoS Monitor: Check if the Resource Naming Service of a grid folder is available.
	 * @param rns_path
	 * @return
	 */
	private boolean is_rns_available(String rns_path) {
		if (!is_rns_valid(rns_path)) {
			return false;
		}

		// test availability - false if not available or no access permission
		try {
			GeniiPath gPath = new GeniiPath(rns_path);
			RNSPath current = RNSPath.getCurrent();
			RNSPath rns = current.lookup(gPath.path(), RNSPathQueryFlags.MUST_EXIST);
			EndpointReferenceType service = rns.getEndpoint();
			GeniiCommon common = ClientUtils.createProxy(GeniiCommon.class, service);
			common.getResourcePropertyDocument(new GetResourcePropertyDocument());
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.out.println("(qm) Warning: " + rns_path + " is not available.");
			return false;
		}

		return true;
	}

	/**
	 * QoS Monitor: Monitor if a directory's spec is satisfied.
	 * @param dir
	 * @return
	 */
	private boolean monitor_directory(String dir) {
		assert(dir != null);
		GeniiPath path = new GeniiPath(dir);
		dir = path.lookupRNS().toString();
		boolean succ = true;
		System.out.println("(qm) monitor: Monitor a directory: " + dir);

		if (path.exists()) {
			List<String> spec_ids = db_rel_query(RelQuery.SPECS_RELATED_TO_DIR, dir);
			List<String> container_ids = db_rel_query(RelQuery.CONTAINERS_RELATED_TO_DIR, dir);
			assert(spec_ids.size() == 1 && container_ids.size() > 0);

			QosSpec spec = db_get_spec(spec_ids.get(0));
			List<ContainerStatus> status_list = new ArrayList<ContainerStatus>();
			for (int i = 0; i < container_ids.size(); i++) {
				status_list.add(db_get_status(container_ids.get(i)));
			}
			boolean satisfied = check_qos(spec, status_list, true);
			if (!satisfied) {
				// reschedule
				System.out.println("(qm) monitor: Reschedule directory: " + dir);
				List<String> rescheduled_rns = schedule_internal(null, spec_ids.get(0));
				// Convert RNS paths to container ids.
				List<String> container_ids_new = new ArrayList<String>();
				for (String rns: rescheduled_rns) {
					String id = db_get_container_id_from_rns(rns);
					if (id != null) {
						container_ids_new.add(id);
					} else {
						System.out.println("(qm) Error: Cannnot lookup container ID for RNS " + rns);
						return false;
					}
				}

				System.out.println("(qm) monitor: Reschedule results: " + container_ids_new.toString());
				if (container_ids_new.isEmpty()) {
					System.out.println("(qm) monitor: Cannot reschedule " + dir + ". Please add more available containers.");
					return false;
				} else {
					succ = db_add_scheduled_directory(dir, spec, container_ids_new, false); //update
				}
			}

		} else {
			// the directory may be deleted by the user, just clean the DB
			succ = db_remove_directory(dir);
		}
		return succ;
	}

	/**
	 * QoS Monitor: Monitor if everything related to a container is satisfied.
	 * Note: This function will not update the container status, because we
	 * may temporarily set the availability to 0 to trigger scheduling.
	 * @param dir
	 * @return
	 */
	private boolean monitor_container(String container_id) {
		assert(container_id != null);
		ContainerStatus status = db_get_status(container_id);
		assert(status != null);
		System.out.println("(qm) monitor: Monitor all directories that are related to " + container_id);

		List<String> dirs = db_rel_query(RelQuery.DIRS_RELATED_TO_CONTAINER, container_id);
		boolean succ = true;
		for (int i = 0; i < dirs.size(); i++) {
			succ = monitor_directory(dirs.get(i)) && succ;
		}
		return succ;
	}

	/**
	 * QoS Monitor: Update specs to qos database.
	 * If reserved size is changed, all related containers are updated.
	 * @param container_id
	 * @return
	 */
	private boolean update_spec(String spec_id) {
		assert(spec_id != null);
		QosSpec spec_in_db = db_get_spec(spec_id);
		assert(spec_in_db != null);
		QosSpec spec_remote = new QosSpec();
		boolean succ = spec_remote.read_from_file(spec_in_db.SpecPath);
		System.out.println("(qm) monitor: Read the spec file of " + spec_id + " from " + spec_in_db.SpecPath);
		if (!succ) {
			System.out.println("(qm) monitor: Warning: Cannot access the spec file of " + spec_id);
		} else {
			// NOTE: allow users to change the spec ID?
			assert(spec_remote.SpecId.equals(spec_in_db.SpecId));
			if (spec_remote.ReservedSize != spec_in_db.ReservedSize) {
				// Update reserved size of all related containers
				List<String> dirs = db_rel_query(RelQuery.DIRS_RELATED_TO_SPEC, spec_id);
				for (int i = 0; i < dirs.size(); i++) {
					List<String> container_ids = db_rel_query(RelQuery.CONTAINERS_RELATED_TO_DIR, dirs.get(i));
					ContainerStatus status = db_get_status(container_ids.get(i));
					status.StorageReserved -= spec_in_db.ReservedSize;
					status.StorageReserved += spec_remote.ReservedSize;
					db_update_container(status, false); // update
				}
			}
			db_update_spec(spec_remote, false); // update
		}
		return true;
	}

	/**
	 * QoS Monitor: Update status of a container to qos database.
	 * This function will set the availability for further scheduling.
	 * @param container_id
	 * @return
	 */
	private boolean update_container(String container_id) {
		assert(container_id != null);
		ContainerStatus status_in_db = db_get_status(container_id);
		assert(status_in_db != null);
		ContainerStatus status_remote = new ContainerStatus();
		System.out.println("(qm) monitor: Read the status file of " + container_id + " from " + status_in_db.StatusPath);
		boolean succ = status_remote.read_from_file(status_in_db.StatusPath);
		if (!succ) {
			System.out.println("(qm) monitor: Warning: Cannot access the status file of " + container_id);
			// Check the availability of RNS path anyway
			if (!is_rns_available(status_in_db.RnsPath)) {
				System.out.println("(qm) monitor: Warning: " + container_id + " is [not available].");
				status_in_db.ContainerAvailability = 0;
			} else {
				System.out.println("(qm) monitor: " + container_id + " is [available].");
			}
			db_update_container(status_in_db, false);
		} else {
			assert(status_remote.ContainerId.equals(status_in_db.ContainerId) &&
					status_remote.RnsPath.equals(status_in_db.RnsPath));
			if (!is_rns_available(status_remote.RnsPath)) {
				System.out.println("(qm) monitor: Warning: " + container_id + " is [not available].");
				status_remote.ContainerAvailability = 0;
			} else {
				System.out.println("(qm) monitor: " + container_id + " is [available].");
			}
			// Avoid overwriting the reserved size (container doesn't know this)
			status_remote.StorageReserved = status_in_db.StorageReserved;
			db_update_container(status_remote, false);
		}
		return true;
	}

	/**
	 * QoS Monitor: Monitor all containers in the qos database.
	 * @return
	 */
	private boolean monitor_all() {
		System.out.println("(qm) monitor: Monitor everything.");
		// Step 1: update all containers
		List<String> container_ids = db_get_container_id_list();
		for (int i = 0; i < container_ids.size(); i++) {
			update_container(container_ids.get(i));
		}
		// Step 2: update all specs
		List<String> spec_ids = db_get_spec_id_list();
		for (int i = 0; i < spec_ids.size(); i++) {
			update_spec(spec_ids.get(i));
		}
		// Step 3: monitor all directories
		List<String> dirs = db_get_dir_list();
		for (int i = 0; i < dirs.size(); i++) {
			monitor_directory(dirs.get(i));
		}
		return true;
	}

	/**************************************************************************
	 *  Utility Functions
	 **************************************************************************/

	/**
	 * Get the End Point Identifier of a path
	 * @param path
	 * @return
	 */
	private String get_epi(String path) {
		GeniiPath gPath = new GeniiPath(path);
		String epi = null;
		try {
			RNSPath current = RNSPath.getCurrent();
			RNSPath rns = current.lookup(gPath.path(), RNSPathQueryFlags.MUST_EXIST);

			EndpointReferenceType epr = rns.getEndpoint();
			WSName wsname = new WSName(epr);
			epi = wsname.getEndpointIdentifier().toString();
		} catch (Exception e) {
			System.out.println(e.getClass().getName() + ": " + e.getMessage());
			System.out.println("(qm) Error: Cannot get EPI of " + path);
			epi = null;
		}
		return epi;
	}

	/**
	 * A function to support the qos-manager --status-template=<rns-path> option
	 * @param rns_path
	 * @return
	 */
	private ContainerStatus gen_status_template(String rns_path) {
		if (!is_rns_valid(rns_path)) {
			return null;
		}

		// Set EPI as the container ID
		ContainerStatus status = new ContainerStatus();
		String epi = get_epi(rns_path);
		if (epi == null) {
			return null;
		}
		status.ContainerId = epi;

		// Availability
		if (is_rns_available(rns_path)) {
			status.ContainerAvailability = 90;
		} else {
			status.ContainerAvailability = 0;
		}

		// Get full grid path
		GeniiPath rns = new GeniiPath(rns_path);
		status.RnsPath = "grid:" + rns.lookupRNS();
		return status;
	}

	private boolean clean_replicas() {
		System.out.println("(qm) Warning: Cleaning all unused replicas.");
		List<String> dirs = db_get_dir_list();
		for (int i = 0; i < dirs.size(); i++) {
			String dir = dirs.get(i);
			List<Integer> replica_ids = db_get_replica_ids_for_dir(dir);
			for (int j = 0; j < replica_ids.size(); j++) {
				int id = replica_ids.get(j);
				int actual_id = Math.abs(id) - 1;
				if (id >= 0) continue; // replicas being used
				int err = 0;
				try {
					err = destroyReplica(dir, actual_id);
				} catch (Exception e) {
					System.out.println(e.getClass().getName() + ": " + e.getMessage());
					err = -1;
				}
				if (err != 0) {
					System.out.println("(qm) Error: Cannot remove replica ID " + actual_id + " for " + dir);
					return false;
				} else {
					boolean succ = db_remove_replica_for_dir(dir, id);
					assert(succ);
					System.out.println("(qm) Remove replica ID " + actual_id + " for " + dir);
				}
			}
		}
		return true;
	}

	/**************************************************************************
	 *  Miscellaneous Functions
	 **************************************************************************/

	/**
	 * A replicate function copied from ReplicateTool.java.
	 * The same as: replicate -p  <source-path> <container> [replicant number]
	 */
	public int replicate_policy(String sourcePath, String containerPath, String linkPath)
			throws ReloadShellException, ToolException, UserCancelException,
			RNSException, AuthZSecurityException, IOException, ResourcePropertyException
	{
		RNSPath current = RNSPath.getCurrent();
		RNSPath sourceRNS = current.lookup(sourcePath, RNSPathQueryFlags.MUST_EXIST);
		EndpointReferenceType sourceEPR = sourceRNS.getEndpoint();
		WSName sourceName = new WSName(sourceEPR);
		URI endpointIdentifier = sourceName.getEndpointIdentifier();
		if (endpointIdentifier == null) {
			System.out.println("(qm) replicate error: " + sourceRNS + ": EndpointIdentifier not found");
			return (-1);
		}
		List<ResolverDescription> resolverList = ResolverUtils.getResolvers(sourceName);
		if ((resolverList == null) || (resolverList.size() == 0)) {
			System.out.println("(qm) replication error: " + sourceRNS + ": Resource has no resolver element");
			return (-1);
		}
		TypeInformation type = new TypeInformation(sourceEPR);
		String serviceName = type.getBestMatchServiceName();
		if (serviceName == null) {
			System.out.println("(qm) replicate: " + sourceRNS + ": Type does not support replication");
			return (-1);
		}
		String servicePath = containerPath + '/' + "Services" + '/' + serviceName;
		RNSPath serviceRNS = current.lookup(servicePath, RNSPathQueryFlags.MUST_EXIST);
		EndpointReferenceType serviceEPR = serviceRNS.getEndpoint();
		RNSPath linkRNS = null;
		if (linkPath != null) {
			linkRNS = current.lookup(linkPath, RNSPathQueryFlags.MUST_NOT_EXIST);
		}
		// Setup existing resource tree before creating new resources.
		if (type.isRNS()) {
			Stack<RNSPath> stack = new Stack<RNSPath>();
			stack.push(sourceRNS);
			while (stack.size() > 0) {
				RNSPath currentRNS = stack.pop();
				addPolicy(currentRNS, stack);
			}
		}
		MessageElement[] elementArr = new MessageElement[2];
		elementArr[0] = new MessageElement(IResource.ENDPOINT_IDENTIFIER_CONSTRUCTION_PARAM, endpointIdentifier);
		elementArr[1] = new MessageElement(IResource.PRIMARY_EPR_CONSTRUCTION_PARAM, sourceEPR);
		GeniiCommon common = ClientUtils.createProxy(GeniiCommon.class, serviceEPR);
		VcgrCreate request = new VcgrCreate(elementArr);
		VcgrCreateResponse response = common.vcgrCreate(request);
		EndpointReferenceType newEPR = response.getEndpoint();
		if (linkRNS != null) {
			linkRNS.link(newEPR);
		}
		ResourceSecurityPolicy oldSP = new ResourceSecurityPolicy(sourceEPR);
		ResourceSecurityPolicy newSP = new ResourceSecurityPolicy(newEPR);
		newSP.copyFrom(oldSP);

		CacheManager.removeItemFromCache(sourceEPR, RNSConstants.ELEMENT_COUNT_QNAME, MessageElement.class);
		CacheManager.removeItemFromCache(newEPR, RNSConstants.ELEMENT_COUNT_QNAME, MessageElement.class);

		/*
		 * if (ADD_RESOURCES_TO_ACLS) { // Allow the new resource to modify the old resource, and vice versa, // even if the user did not
		 * delegate his identity to the resource. newSP.addResource(oldSP); oldSP.addResource(newSP); }
		 */
		return 0;
	}

	/**
	 * A replicate function copied from ReplicateTool.java
	 */
	private void addPolicy(RNSPath currentRNS, Stack<RNSPath> stack) throws RemoteException, RNSException
	{
		System.out.println("(qm) addPolicy " + currentRNS);
		GeniiCommon dirService = ClientUtils.createProxy(GeniiCommon.class, currentRNS.getEndpoint());
		MessageElement[] elementArr = new MessageElement[1];
		elementArr[0] = new MessageElement(GeniiDirPolicy.REPLICATION_POLICY_QNAME, "true");
		UpdateResourceProperties request = new UpdateResourceProperties(new UpdateType(elementArr));
		dirService.updateResourceProperties(request);

		Collection<RNSPath> contents = currentRNS.listContents();
		for (RNSPath child : contents) {
			TypeInformation type = new TypeInformation(child.getEndpoint());
			if (type.isRNS())
				stack.push(child);
		}
	}

	/**
	 * A resolver function copied from ResolverTool.java.
	 * The same as running: resolver -p <source-path> <resolver-path>
	 */
	public int resolver_policy(String sourcePath, String targetPath, boolean recursive)
			throws ReloadShellException, ToolException, UserCancelException,
			RNSException, AuthZSecurityException, IOException, ResourcePropertyException
	{
		RNSPath current = RNSPath.getCurrent();
		RNSPath sourceRNS = current.lookup(sourcePath, RNSPathQueryFlags.MUST_EXIST);

		RNSPath targetRNS = current.lookup(targetPath, RNSPathQueryFlags.MUST_EXIST);
		EndpointReferenceType targetEPR = targetRNS.getEndpoint();
		TypeInformation targetType = new TypeInformation(targetEPR);
		EndpointReferenceType resolverEPR = null;
		if (targetType.isEpiResolver()) {
			resolverEPR = targetEPR;
		} else if (targetType.isContainer()) {
			String servicePath = targetPath + "/Services/GeniiResolverPortType";
			RNSPath serviceRNS = current.lookup(servicePath, RNSPathQueryFlags.MUST_EXIST);
			EndpointReferenceType serviceEPR = serviceRNS.getEndpoint();
			GeniiResolverPortType resolverService = ClientUtils.createProxy(GeniiResolverPortType.class, serviceEPR);
			MessageElement[] params = new MessageElement[0];
			VcgrCreateResponse response = resolverService.vcgrCreate(new VcgrCreate(params));
			resolverEPR = response.getEndpoint();
			System.out.println("(qm) ResolverTool: Created resolver resource");
		} else {
			System.out.println("(qm) ResolverTool: Failed to find or create resolver at " + targetPath);
			return (-1);
		}
		Stack<RNSPath> stack = new Stack<RNSPath>();
		stack.push(sourceRNS);
		while (stack.size() > 0) {
			sourceRNS = stack.pop();
			addResolver(sourceRNS, resolverEPR, stack, recursive);
		}
		return 0;
	}

	/**
	 * A resolver function copied from ResolverTool.java
	 */
	private void addResolver(RNSPath sourceRNS, EndpointReferenceType resolverEPR,
			Stack<RNSPath> stack, boolean recursive)
			throws IOException, RNSException
	{
		EndpointReferenceType sourceEPR = sourceRNS.getEndpoint();
		WSName sourceName = new WSName(sourceEPR);
		if (!sourceName.isValidWSName()) {
			System.out.println(sourceRNS + ": no EPI");
			return;
		}
		if (sourceName.hasValidResolver()) {
			System.out.println(sourceRNS + ": already has resolver");
			return;
		}
		UpdateResponseType response = ResolverUtils.updateResolver(resolverEPR, sourceEPR);
		EndpointReferenceType finalEPR = response.getNew_EPR();
		TypeInformation type = new TypeInformation(sourceEPR);
		if (type.isRNS()) {
			GeniiCommon dirService = ClientUtils.createProxy(GeniiCommon.class, sourceEPR);
			MessageElement[] elementArr = new MessageElement[1];
			elementArr[0] = new MessageElement(GeniiDirPolicy.RESOLVER_POLICY_QNAME, resolverEPR);
			InsertResourceProperties insertReq = new InsertResourceProperties(new InsertType(elementArr));
			dirService.insertResourceProperties(insertReq);
		}
		CacheManager.removeItemFromCache(sourceRNS.pwd(), EndpointReferenceType.class);
		CacheManager.putItemInCache(sourceRNS.pwd(), finalEPR);
		if (sourceRNS.isRoot()) {
			System.out.println("Added resolver to root directory.");
			/*
			 * Store the new EPR in the client's calling context, so this client will see a root directory with a resolver element. Using the
			 * new EPR, the root directory can be replicated, and failover will work. Other existing clients will continue using the old root
			 * EPR, which still works as the root directory, but it does not support replication or failover.
			 */
			RNSPath rootPath = new RNSPath(finalEPR);
			String pwd = RNSPath.getCurrent().pwd();
			RNSPath currentPath = rootPath.lookup(pwd, RNSPathQueryFlags.MUST_EXIST);
			ICallingContext ctxt = ContextManager.getExistingContext();
			ctxt.setCurrentPath(currentPath);
			ContextManager.storeCurrentContext(ctxt);
		} else {
			sourceRNS.unlink();
			sourceRNS.link(finalEPR);
		}
		if (type.isRNS() && recursive) {
			Collection<RNSPath> contents = sourceRNS.listContents();
			for (RNSPath child : contents) {
				stack.push(child);
			}
		}
	}

	/**
	 * A resolver function copied from ResolverTool.java.
	 * The same as running: resolver -d <source-path> <replicant-number>
	 */
	private int destroyReplica(String replicaPath, int replicaNum)
			throws RNSException, AuthZSecurityException, ResourceException, ToolException
	{
		Boolean sameEPR = false;
		RNSPath current = RNSPath.getCurrent();
		RNSPath replicaRNS = current.lookup(replicaPath, RNSPathQueryFlags.MUST_EXIST);
		EndpointReferenceType replicaEPR = replicaRNS.getEndpoint();
		// Now get the replica vector of replica numbers
		int[] list = null;
		// First get the vector or replica numbers
		list = ResolverUtils.getEndpoints(replicaEPR);
		// If there is only one copy, don't allow replica remove
		if (list.length == 1) {
			stdout.println("Only one copy of " + replicaPath + ". Use rm to delete it.");
			return 1;
		}
		// Now look them all up and get their EPRs
		LookupResponseType dir = ResolverUtils.getEndpointEntries(replicaEPR);
		if (dir != null && list != null) {
			// Now find the replica
			int index = -1;
			for (int j = 0; j < list.length; j++) {
				if (list[j] == replicaNum) {
					index = j;
					break;
				}
			}
			if (index >= 0) {
				/*
				 * stdout.println(":replicaEPR data:\n" + replicaEPR.getAddress().toString()+"\n" +
				 * replicaEPR.getReferenceParameters().get_any()[0].toString()); stdout.println(":selected entry data:\n" +
				 * dir.getEntryResponse(index).getEndpoint().getAddress().toString()+"\n" + dir.getEntryResponse
				 * (index).getEndpoint().getReferenceParameters().get_any()[0].toString());
				 */
				// To determine if the two replica instances are the same we check if their
				// container address and resource key are the same.
				// The EPR equals operator does not do it correctly.
				sameEPR =
					replicaEPR.getAddress().toString().compareTo(dir.getEntryResponse(index).getEndpoint().getAddress().toString()) == 0
						&& replicaEPR.getReferenceParameters().get_any()[0].toString().compareTo(
							dir.getEntryResponse(index).getEndpoint().getReferenceParameters().get_any()[0].toString()) == 0;

				if (sameEPR) {
					// 2014-10-04 ASG. I had code to pick a different EPR, but the list of EPR's I
					// got back did not have resolvers embedded in them.
					// So instead i am going to call ResolverUtils.resolve(EPR) and let the server
					// pick one for me because it will properly embed
					// the resolver info in the EPR. I could alternatively, construct my own using
					// some notion of closeness, but i will not.
					// We must consider what might happen if the operation fails in the middle. If
					// we simply unlink the old and link in the new,
					// if a failure occurs after unlinking, and before linking, we could loose the
					// reference to the resource. Soooo, instead we
					// fist create a new link with the old, soon-to-be-removed-epr, then unlink,
					// link the new, unlink the old.
					RNSPath tempLink = current.lookup(replicaPath + "-warning-removal-replica-failed", RNSPathQueryFlags.MUST_NOT_EXIST);
					try {
						EndpointReferenceType replacementEPR = ResolverUtils.resolve(replicaEPR);
						tempLink.link(replicaEPR); // We will unlink this in just a moment, just
													// don't want to loose it.
						replicaRNS.unlink(); // unlink the old entry
						replicaRNS.link(replacementEPR);// link in the new
						// now when we destroy the replicaEPR we will not be removing the copy
						// pointed to by the directory entry
						// Next we unlink the temporary link
						tempLink.unlink();
					} catch (Throwable e) {
						stdout.println("Failed to get a new resolution EPR, this should NEVER happen.");
						throw new ToolException("Failure removing replicant: " + e.getLocalizedMessage(), e);
					}
				} else
					replicaEPR = dir.getEntryResponse(index).getEndpoint();
			} else {
				stdout.println(replicaNum + " is out of range");
				return 1;
			}
		} else {
			stdout.println("There are no replicas of " + replicaPath);
			return 1;
		}
		// Now destroy the replicant
		MessageElement[] elementArr = new MessageElement[1];
		elementArr[0] = new MessageElement(SyncProperty.UNLINKED_REPLICA_QNAME, "true");
		UpdateType update = new UpdateType(elementArr);
		UpdateResourceProperties request = new UpdateResourceProperties(update);

		try {
			GeniiCommon common = ClientUtils.createProxy(GeniiCommon.class, replicaEPR);
			common.updateResourceProperties(request);
			common.destroy(new Destroy());
		} catch (Throwable e) {
			throw new ToolException("Could no destroy the replicant: " + e.getLocalizedMessage(), e);
		}
		return 0;
	}
}
