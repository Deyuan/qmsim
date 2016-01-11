package edu.virginia.vcgr.genii.client.cmd.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.oasis_open.docs.wsrf.rl_2.Destroy;
import org.ws.addressing.EndpointReferenceType;

import edu.virginia.vcgr.genii.client.WellKnownPortTypes;
import edu.virginia.vcgr.genii.client.cmd.InvalidToolUsageException;
import edu.virginia.vcgr.genii.client.cmd.ReloadShellException;
import edu.virginia.vcgr.genii.client.cmd.ToolException;
import edu.virginia.vcgr.genii.client.comm.ClientUtils;
import edu.virginia.vcgr.genii.client.configuration.DeploymentName;
import edu.virginia.vcgr.genii.client.configuration.Installation;
import edu.virginia.vcgr.genii.client.configuration.NamespaceDefinitions;
import edu.virginia.vcgr.genii.client.context.ContextManager;
import edu.virginia.vcgr.genii.client.context.ICallingContext;
import edu.virginia.vcgr.genii.client.dialog.UserCancelException;
import edu.virginia.vcgr.genii.client.gpath.GeniiPath;
import edu.virginia.vcgr.genii.client.gpath.GeniiPathType;
import edu.virginia.vcgr.genii.client.io.LoadFileResource;
import edu.virginia.vcgr.genii.client.resource.PortType;
import edu.virginia.vcgr.genii.client.resource.TypeInformation;
import edu.virginia.vcgr.genii.client.rns.RNSException;
import edu.virginia.vcgr.genii.client.rns.RNSPath;
import edu.virginia.vcgr.genii.client.rns.RNSPathAlreadyExistsException;
import edu.virginia.vcgr.genii.client.rns.RNSPathDoesNotExistException;
import edu.virginia.vcgr.genii.client.rns.RNSPathQueryFlags;
import edu.virginia.vcgr.genii.client.rns.RNSUtilities;
import edu.virginia.vcgr.genii.client.rp.ResourcePropertyException;
import edu.virginia.vcgr.genii.client.security.axis.AuthZSecurityException;
import edu.virginia.vcgr.genii.common.GeniiCommon;
import edu.virginia.vcgr.genii.common.rfactory.VcgrCreate;
import edu.virginia.vcgr.genii.client.cmd.tools.CopyTool;
import edu.virginia.vcgr.genii.client.rns.CopyMachine;

public class MkdirTool extends BaseGridTool
{
	static private Log _logger = LogFactory.getLog(MkdirTool.class);

	static private final String _DESCRIPTION = "config/tooldocs/description/dmkdir";
	static private final String _USAGE_RESOURCE = "config/tooldocs/usage/umkdir";
	static private final String _MANPAGE = "config/tooldocs/man/mkdir";

	private boolean _parents = false;
	private String _rnsService = null;
	private String _specsPath = null;

	public MkdirTool()
	{
		super(new LoadFileResource(_DESCRIPTION), new LoadFileResource(_USAGE_RESOURCE), false, ToolCategory.DATA);
		addManPage(new LoadFileResource(_MANPAGE));
	}

	@Option({ "parents", "p" })
	public void setParents()
	{
		_parents = true;
	}

	@Option({ "rns-service" })
	public void setRns_service(String service)
	{
		_rnsService = service;
	}

	@Option({ "specs" })
	public void set_specs(String path)
	{
		_specsPath = path;
	}

	@Override
	protected int runCommand() throws ReloadShellException, ToolException, UserCancelException, RNSException, AuthZSecurityException,
		IOException, ResourcePropertyException
	{
		return makeDirectory(_parents, _rnsService, _specsPath, getArguments(), stderr);
	}

	@Override
	protected void verify() throws ToolException
	{
		if (numArguments() < 1)
			throw new InvalidToolUsageException();
	}

	public static EndpointReferenceType lookupPath(String path) throws RNSPathDoesNotExistException, RNSException, FileNotFoundException
	{
		NamespaceDefinitions nsd = Installation.getDeployment(new DeploymentName()).namespace();
		return RNSUtilities.findService(nsd.getRootContainer(), "EnhancedRNSPortType", new PortType[] { WellKnownPortTypes.RNS_PORT_TYPE() },
			new GeniiPath(path).path()).getEndpoint();
	}

	public static int makeDirectory(boolean parents, String rnsService, String specsPath, List<String> pathsToCreate, PrintWriter stderr) throws RNSException,
		InvalidToolUsageException, FileNotFoundException, IOException
	{
		boolean createParents = false;
		EndpointReferenceType service = null;
		List<String> scheduled_results = null;
		QosManagerTool qos_manager = QosManagerTool.factory();
		
		if (specsPath != null) {
			if (pathsToCreate.size() != 1) {
				System.out.println("(mkdir) qm: Please create one folder a time with --specs.");
				return 1;
			}
			System.out.println("(mkdir) qm: Dynamically scheduling with specifications: " + specsPath);
			scheduled_results = qos_manager.schedule_wrapper(specsPath, null, pathsToCreate.get(0));
			if (scheduled_results == null || scheduled_results.size() == 0) {
				System.out.println("(mkdir) qm: Unable to schedule, please modify the specs or add more available containers to the QoS database.");
				return 1;
			} else {
				System.out.println("(mkdir) qm: Scheduling results: " + scheduled_results.toString());
			}
			rnsService = scheduled_results.get(0);
		}

		if (rnsService != null) {
			GeniiPath gPath = new GeniiPath(rnsService);
			if (gPath.pathType() != GeniiPathType.Grid)
				throw new InvalidToolUsageException("RNSService must be a grid path. ");
			// October 1, 2015 by ASG, check if rnsServce has a Services/EnhancedRNSPortType sub-path
			// If it does, over-ride their path with the sub-dir
			if (new GeniiPath(rnsService + "/Services/EnhancedRNSPortType").exists()) {
				service = lookupPath(rnsService + "/Services/EnhancedRNSPortType");
			} else
				service = lookupPath(rnsService);
		}

		ICallingContext ctxt = ContextManager.getExistingContext();

		if (parents)
			createParents = true;

		RNSPath path = ctxt.getCurrentPath();
		for (String sPath : pathsToCreate) {
			GeniiPath gPath = new GeniiPath(sPath);
			if (gPath.exists())
				throw new RNSPathAlreadyExistsException(gPath.path());
			if (gPath.pathType() == GeniiPathType.Grid) {
				RNSPath newDir = lookup(gPath, RNSPathQueryFlags.MUST_NOT_EXIST);

				path = newDir;

				if (service == null) {
					if (createParents)
						path.mkdirs();
					else
						path.mkdir();
				} else {
					RNSPath parent = path.getParent();

					if (!parent.exists()) {
						String msg = "Can't create directory \"" + path.pwd() + "\".";
						_logger.error(msg);
						if (stderr != null)
							stderr.println(msg);
						return 1;
					}

					TypeInformation typeInfo = new TypeInformation(parent.getEndpoint());
					if (!typeInfo.isRNS()) {
						String msg = "\"" + parent.pwd() + "\" is not a directory.";
						_logger.error(msg);
						if (stderr != null)
							stderr.println(msg);
						return 1;
					}

					GeniiCommon common = ClientUtils.createProxy(GeniiCommon.class, service);
					EndpointReferenceType newEPR = common.vcgrCreate(new VcgrCreate(null)).getEndpoint();
					try {
						path.link(newEPR);
						newEPR = null;
					} finally {
						if (newEPR != null) {
							common = ClientUtils.createProxy(GeniiCommon.class, newEPR);
							common.destroy(new Destroy());
						}
					}
				}
			} else {
				File newFile = new File(gPath.path());
				if (createParents) {
					if (!newFile.mkdirs()) {
						String msg = "Could not create directory " + gPath.path();
						_logger.error(msg);
						if (stderr != null)
							stderr.println(msg);
						return 1;
					}
				} else if (!newFile.mkdir()) {
					String msg = "Could not create directory " + gPath.path();
					_logger.error(msg);
					if (stderr != null)
						stderr.println(msg);
					return 1;
				}
			}
		}

		if (specsPath != null) {
			System.out.println("(mkdir) qm: Post-processing NYI.");
			// TODO: when reaching here, the folder is created successfully.
			// TODO: Tell qos-manager that the folder is created. (maybe
			//       another table in the db?). This information is for rescheduling.
			// TODO: Set replicate and resolver.	

			rnsService = scheduled_results.get(0);
			
			if (scheduled_results.size() == 1) {
				//no need to replicate
			} 
			if (scheduled_results.size() > 1) {
				//set replicate
				//get primary RNSPath
				//I'm not sure if scheduled_results.get(0) is the RnsPath, if yes, we don't need to add db_get_container_RnsPath
				//QosManagerTool.java
				//Besides, I'm not sure if the sourcePath should be a RnsPath
				String sourcePath = qos_manager.db_get_container_RnsPath(scheduled_results.get(0));
				
				GeniiPath gPath = new GeniiPath(rnsService);
				RNSPath current = RNSPath.getCurrent();
				RNSPath rns = current.lookup(gPath.path(), RNSPathQueryFlags.MUST_EXIST);
				for (int i=1;i<scheduled_results.size();i++) {
					String targetPath = qos_manager.db_get_container_RnsPath(scheduled_results.get(i));
					CopyTool.copy(sourcePath, targetPath, true, true, rns, stderr);
				}
		}			
		return 0;
	}
}
}
