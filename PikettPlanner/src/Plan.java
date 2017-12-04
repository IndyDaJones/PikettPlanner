import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * 
 * @author j.nyffeler
 *
 */
public class Plan {
	private static final Logger log = Logger.getLogger( PikettPlanner.class.getName() );
	DBHandler db;
	String checksum;
	FileHandler file;
	ArrayList<String> sequenceNumbers;
	public Plan(){
		file = new FileHandler();
	}
	/**
	 * The method starts the calcualtion
	 */
	public void start() {
		log.log(Level.INFO,"create DBHandler");
		db = new DBHandler();
		log.log(Level.INFO,"DBHandler created");
		log.log(Level.INFO,"Startup Checksum calcualtion");
		try {
			performPlanning();
		} catch (SQLException e) {
			log.log(Level.SEVERE,e.getLocalizedMessage());
			e.printStackTrace();
		}
	}
	
	/**
	 * In this method the actual checksum for the source file is calculated
	 */
	private void startPlannung() {
		try {
			performMapping();
		} catch (SQLException e) {
			log.log(Level.SEVERE,e.getLocalizedMessage());
		
		}
	}

	private void performMapping() throws SQLException {
		ResultSet employees = db.GetMapping();
		int i = 0;
		while (employees != null && employees.next()) {
			i++;
		}
		ResultSet planning = db.GetPlanning(i);
		while (planning != null && planning.next()) {
			for (int j=2;j <= i; j++) {
				if (planning.getString(j) != null && planning.getString(j).equals("X")) {
					db.insertData(planning.getString(1), Integer.toString(j-1));
				}
			}
		}
	}
	private void performPlanning() throws SQLException {
		Hashtable<?, ?> customers = db.GetEmployeesPerCustomer();
		log.log(Level.INFO,"Customers fetched <"+customers.size()+">");
		// Clear the planning table
		db.clearPlanning();
		log.log(Level.INFO,"Pallning cleared");
		Hashtable weeks = db.GetWeeks();
		log.log(Level.INFO,"Weeks fetched <"+weeks.size()+">");
		for (int cust = 0; cust < customers.size(); cust++) {
			int empl = 0;
			for (int week = 1; week <= weeks.size(); week++) {
				Hashtable employees = db.GetEmployeeFromCustomer(customers.get(cust).toString(), week);
				empl = 0;
				if (employees.isEmpty()) {
					db.planWeek(customers.get(cust).toString(), Integer.toString(week));
				}else {
					log.log(Level.INFO,"Employee fetched: <"+employees.size()+">");
					String weekToPlan =  String.format("%02d", week);
					log.log(Level.INFO,"New week: Customer: <" + customers.get(cust).toString() + ">, Employee: <"+employees.get(empl).toString()+">, Week: <"+weekToPlan+">");
					db.planWeek(customers.get(cust).toString(), employees.get(empl).toString(), week);
					empl++;
				}
			}
		}
	}
	
	/**
	 * In this method the checksum is being calculated and stored in the database
	 * @param path Path to the file containing the sourcode from which a checksum is needed
	 * @param ID Identifier of the database record where the checksum is stored after calculation
	 */
	private void calculateChecksum(String path, String ID) {
		try {
			File Source = new File(path);
			log.log(Level.INFO,"Source loaded " + path);
			String file_checksum = FileHandler.getFileChecksum(checksum, Source);
			log.log(Level.INFO,"Checksum calculated " + file_checksum + "for sequence with ID "+ ID);
			db.updatePath(ID, file_checksum);
			// Let Thread sleep for a while so that the DB can do it's job properly! (Access DB)
			Thread.sleep(file.getDBTimeout());
		}catch (IOException e) {
			db.updatePath(ID, e.getLocalizedMessage());
			log.log(Level.SEVERE,e.getLocalizedMessage());
		}catch (Exception e) {
			db.updatePath(ID, e.getLocalizedMessage());
			log.log(Level.SEVERE,e.getLocalizedMessage());
		}
	}
	
	
	/**
	 * Prepares the path to the sequence for the checksum calcualtion
	 * @param System
	 * @param SequnceNumber
	 * @param SequenceName
	 * @return
	 */
	private String preparePathATK32(String System, String SequenceNumber, String SequenceName) {
		String path = "";
		//FileHandler file = new FileHandler();
		if(System != null && SequenceNumber != null && SequenceName != null) {
			//It is a sequence
			path = file.getFileLocation() +"/"+ System+"/"+SequenceName;
		}
		path = path.replace("\\","/");
		
		log.log(Level.INFO,"FilePath is " + path);
		path = path.replace("/", "\\");
		
		return path;
	}
	/**
	 * Prepares the path to the sequence for the checksum calcualtion
	 * @param System
	 * @param SequnceNumber
	 * @param SequenceName
	 * @return
	 */
	private String preparePathATK32(String System, String SequenceNumber) {
		String path = "";
		//FileHandler file = new FileHandler();
		if(System != null && SequenceNumber != null) {
			//It is a sequence
			path = file.getFileLocation() +"/"+ System+"/"+getLatestSequenceVersion(System, SequenceNumber);
		}
		path = path.replace("\\","/");
		
		log.log(Level.INFO,"FilePath is " + path);
		path = path.replace("/", "\\");
		
		return path;
	}
	/**
	 * Gives the latest version number from a set of sequences 
	 * @param System
	 * @param SequenceNumber
	 * @return
	 */
	private String getLatestSequenceVersion(String System, String SequenceNumber) {
		String path = file.getFileLocation() +"/"+ System+"/";
		File rep = new File(path);
		File[] list = rep.listFiles();
		rep = null;
		ArrayList<String> filenames = new ArrayList<String>();
		for ( int i = 0; i < list.length; i++) {
			if (list[i].getName().contains("SEQ"+SequenceNumber+"_X") && list[i].getName().contains(".SEQ"))
		    filenames.add(list[i].getName());
		}
		Collections.sort(filenames.subList(0, filenames.size()));
		String version = filenames.get(filenames.size()-1);
		return version;
	}
	
	/**
	 * Gives the latest version number from a set of sequences 
	 * @param System
	 * @param SequenceNumber
	 * @return
	 */
	private Hashtable getLatestSequencesFromFolder(String Folder) {
		String path = file.getFileLocation() +"/"+ Folder+"/";
		File rep = new File(path);
		File[] list = rep.listFiles();
		rep = null;
		ArrayList<String> filenames = new ArrayList<String>();
		for ( int i = 0; i < list.length; i++) {
			if (list[i].getName().contains("SEQ") && list[i].getName().contains(".SEQ"))
		    filenames.add(list[i].getName());
		}
		Collections.sort(filenames.subList(0, filenames.size()));
		String version = filenames.get(filenames.size()-1);
		
		Hashtable<String, String> result = new Hashtable<String, String>();
		for ( int i = 0; i < filenames.size(); i++) {
			String sequenceNumb = getSequenceNumberFromFileName(filenames.get(i));
			String latestVersion = getLatestSequenceVersion(Folder, sequenceNumb);
			if(!result.contains(latestVersion)) {
				result.put(getSequenceNumberFromFileName(latestVersion), latestVersion);
			}
		}
		return result;
	}
	/**
	 * Gives the latest version number from a set of sequences 
	 * @param System
	 * @param SequenceNumber
	 * @return
	 */
	private ArrayList<String> getLatestSequencesNumbersFromFolder(String Folder) {
		String path = file.getFileLocation() +"/"+ Folder+"/";
		File rep = new File(path);
		File[] list = rep.listFiles();
		rep = null;
		ArrayList<String> filenames = new ArrayList<String>();
		for ( int i = 0; i < list.length; i++) {
			if (list[i].getName().contains("SEQ") && list[i].getName().contains(".SEQ"))
		    filenames.add(list[i].getName());
		}
		Collections.sort(filenames.subList(0, filenames.size()));
		String version = filenames.get(filenames.size()-1);
		
		ArrayList<String> result = new ArrayList<String>();
		for ( int i = 0; i < filenames.size(); i++) {
			String sequenceNumb = getSequenceNumberFromFileName(filenames.get(i));
			String latestVersion = getLatestSequenceVersion(Folder, sequenceNumb);
			if(!result.contains(latestVersion)) {
				result.add(getSequenceNumberFromFileName(latestVersion));
			}
		}
		return result;
	}
	private ArrayList<String> getSequencesNumbersFromFolder(String Folder){
		String path = file.getFileLocation() +"/"+ Folder+"/";
		File rep = new File(path);
		File[] list = rep.listFiles();
		rep = null;
		for ( int i = 0; i < list.length; i++) {
			if (list[i].getName().contains("SEQ") && list[i].getName().contains(".SEQ") && !sequenceNumbers.contains(getSequenceNumberFromFileName(list[i].getName())))
				sequenceNumbers.add(getSequenceNumberFromFileName(list[i].getName()));
		}
		return sequenceNumbers;
	}
	/**
	 * Returns the sequence number from a sequence file name
	 * @param SequenceFileName
	 * @return
	 */
	private String getSequenceNumberFromFileName(String SequenceFileName) {
		return SequenceFileName.substring(SequenceFileName.indexOf("Q")+1, SequenceFileName.indexOf("X")-1);
	}
	/**
	 * Returns the sequence name from a sequence file name
	 * @param SequenceFileName
	 * @return
	 */
	private String getSequenceNameFromFileName(String SequenceFileName) {
		return SequenceFileName.substring(0, SequenceFileName.indexOf("."));
	}
	private boolean isValidFile(String path) {
		try {
			log.log(Level.INFO,"Try to load source  with path " + path);
			File Source = new File(path);
			log.log(Level.INFO,"Source successfully loaded " + path);
			//Get file input stream for reading the file content
			//FileInputStream fis = new FileInputStream(Source);
			//close the stream; We don't need it now.
			//fis.close();
			return Source.canRead();
		}catch (Exception e) {
			log.log(Level.SEVERE,e.getLocalizedMessage());
			return false;
		}
	}
}
