import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBHandler {
	private static final Logger log = Logger.getLogger( PikettPlanner.class.getName() );
	DBConnection conn;
	DBHandlerProperty props;
	/**
	 * Constructor
	 */
	public DBHandler(){
		props = new DBHandlerProperty();
		log.log(Level.INFO,"call initDB");
		initDB();
		log.log(Level.INFO,"Database initiated");
	}
	/**
	 * This method creates a new database instance
	 */
	private void initDB(){
		log.log(Level.INFO,"Try to create DBConnection");
		this.conn = new DBConnection(props);
	}
	/**
	 * Return the table name defined in the database.property files
	 * @return
	 */
	public String getMappingTableName() {
		return props.getDBProperty("MappingTable");
	}
	/**
	 * Return the table name defined in the database.property files
	 * @return
	 */
	public String getDefaultEmployee() {
		return props.getDBProperty("DefaultEmployee");
	}
	/**
	 * Return the table name defined in the database.property files
	 * @return
	 */
	public String getPlanningTableName() {
		return props.getDBProperty("PlanningTable");
	}
	/**
	 * Return the table name defined in the database.property files
	 * @return
	 */
	public String getCustomerTableName() {
		return props.getDBProperty("CustomerTable");
	}
	/**
	 * Return the table name defined in the database.property files
	 * @return
	 */
	public String getWeekTableName() {
		return props.getDBProperty("WeekTable");
	}
	/**
	 * Return the table name defined in the database.property files
	 * @return
	 */
	public String getHolidayTableName() {
		return props.getDBProperty("HolidayTable");
	}
	/**
	 * Return the table name defined in the database.property files
	 * @return
	 */
	public String getEmployeeTableName() {
		return props.getDBProperty("EmployeeTable");
	}
	/**
	 * Return the table name defined in the database.property files
	 * @return
	 */
	public String getEmployeePerCustomerTableName() {
		return props.getDBProperty("EmployeePerCustomerTable");
	}
	/**
	 * Returns the defined pathes to the source files
	 */
	public ResultSet clearPlanning() {
		//Statement s;
		int i = 0;
		ResultSet rs = null;
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String selection = "DELETE FROM "+ getPlanningTableName();
		    log.log(Level.INFO,"Query to execute: " + selection);
		    s.execute(selection);
		    rs = s.getResultSet();
		    s.close();
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return rs;
	}
	/**
	 * Returns the defined foldernames to the files
	 */
	public Hashtable GetEmployees() {
		//Statement s;
		int i = 0;
		Hashtable result = new Hashtable();
		ResultSet rs = null;
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String selection = "SELECT ID, Mitarbeiter FROM "+ getEmployeeTableName();
		    log.log(Level.INFO,"Query to execute: " + selection);
		    s.execute(selection);
		    rs = s.getResultSet();
		    s.close();
		    while((rs!=null) && (rs.next())){
		    	log.log(Level.INFO,"Data form Database fetched: <"+ rs.getString(1)+">");
		    	result.put(rs.getString(1), rs.getString(2));
		    	i++;
		    }
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return result;
	}
	
	/**
	 * Returns the defined foldernames to the files
	 */
	public Hashtable GetEmployeesPerCustomer() {
		//Statement s;
		int i = 0;
		ResultSet rs = null;
		Hashtable result = new Hashtable();
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String selection = "SELECT customer FROM "+ getEmployeePerCustomerTableName();
		    log.log(Level.INFO,"Query to execute: " + selection);
		    s.execute(selection);
		    rs = s.getResultSet();
		    while((rs!=null) && (rs.next())){
		    	result.put(i, rs.getString(1));
		    	i++;
		    }
		    s.close();
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    	
		    }
		return result;
	}
	
	/**
	 * Returns the defined foldernames to the files
	 */
	public Hashtable GetCustomers() {
		//Statement s;
		int i = 0;
		ResultSet rs = null;
		Hashtable result = new Hashtable();
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String selection = "SELECT ID, CUSTOMER FROM "+ getCustomerTableName();
		    log.log(Level.INFO,"Query to execute: " + selection);
		    s.execute(selection);
		    rs = s.getResultSet();
		    s.close();
		    while((rs!=null) && (rs.next())){
		    	log.log(Level.INFO,"Data form Database fetched: <"+ rs.getString(1)+">");
		    	result.put(rs.getString(1), rs.getString(2));
		    	i++;
		    }
			} catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return result;
	}
	
	/**
	 * Returns the defined foldernames to the files
	 */
	public Hashtable GetWeeks() {
		//Statement s;
		int i = 0;
		Hashtable result = new Hashtable();
		ResultSet rs = null;
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String selection = "SELECT ID, WEEK FROM "+ getWeekTableName();
		    log.log(Level.INFO,"Query to execute: " + selection);
		    s.execute(selection);
		    rs = s.getResultSet();
		    while((rs!=null) && (rs.next())){
		    	result.put(rs.getString(1), rs.getString(2));
		    	i++;
		    }
		    s.close();
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return result;
	}
	
	/**
	 * Returns the defined foldernames to the files
	 */
	public Hashtable GetHoliday() {
		//Statement s;
		int i = 0;
		Hashtable result = new Hashtable();
		ResultSet rs = null;
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String selection = "SELECT ID, WEEK FROM "+ getHolidayTableName();
		    log.log(Level.INFO,"Query to execute: " + selection);
		    s.execute(selection);
		    rs = s.getResultSet();
		    s.close();
		    while((rs!=null) && (rs.next())){
		    	log.log(Level.INFO,"Data form Database fetched: <"+ rs.getString(1)+">");
		    	result.put(rs.getString(1), rs.getString(2));
		    	i++;
		    }
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return result;
	}
	/** 
	 * Returns the defined foldernames to the files
	 */
	public boolean onHoliday(String employee, String week) {
		boolean result = false;
		ResultSet rs = null;
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String query = "SELECT * FROM "+ getHolidayTableName() +" WHERE EMPLOYEE = '"+employee+"' AND WEEK = '"+week+"'";
		    log.log(Level.INFO,"Query to execute: " + query);
		    s.execute(query);
		    rs = s.getResultSet();
		    while((rs!=null) && (rs.next())){
		    	result = true;
		    }
		    s.close();
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return result;
	}
	
	/** 
	 * Returns the defined foldernames to the files
	 */
	public boolean isPlanned(String customer, String week) {
		boolean result = false;
		ResultSet rs = null;
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String query = "SELECT * FROM "+ getPlanningTableName() +" WHERE CUSTOMER = '"+customer+"' AND WEEK = '"+week+"'";
		    log.log(Level.INFO,"Query to execute: " + query);
		    s.execute(query);
		    rs = s.getResultSet();
		    while((rs!=null) && (rs.next())){
		    	result = true;
		    }
		    s.close();
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return result;
	}
	/**
	 * Returns the defined foldernames to the files
	 */
	public Hashtable plannedWeek(String employee) {
		int i = 0;
		ResultSet rs = null;
		Hashtable result = new Hashtable();
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String query = "SELECT DISTINCT WEEK FROM "+ getPlanningTableName() +" WHERE EMPLOYEE = '"+employee+"'";
		    log.log(Level.INFO,"Query to execute: " + query);
		    s.execute(query);
		    rs = s.getResultSet();
		    while((rs!=null) && (rs.next())){
		    	result.put(i, rs.getString(1));
		    	i++;
		    }
		    s.close();
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return result;
	}
	/**
	 * Returns the defined foldernames to the files
	 */
	public ResultSet GetPlanning(int numEmpl) {
		//Statement s;
		String selection = "";
		for (int j=1; j<= numEmpl; j++) {
			selection += "F"+j;
			if(j != numEmpl) selection += ", ";
		}
		int i = 0;
		ResultSet rs = null;
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String query = "SELECT ID, "+selection+" FROM "+ getPlanningTableName();
		    log.log(Level.INFO,"Query to execute: " + query);
		    s.execute(query);
		    rs = s.getResultSet();
		    s.close();
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return rs;
	}
	/**
	 * Returns the defined foldernames to the files
	 */
	public ResultSet GetMapping() {
		//Statement s;
		int i = 0;
		ResultSet rs = null;
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String selection = "SELECT ID, CUSTOMER, EMPLOYEE FROM "+ getMappingTableName();
		    log.log(Level.INFO,"Query to execute: " + selection);
		    s.execute(selection);
		    rs = s.getResultSet();
		    s.close();
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return rs;
	}
	/**
	 * Returns the defined foldernames to the files
	 */
	public Hashtable GetEmployeeFromCustomer(String customer, int week) {
		//Statement s;
		int i = 0;
		ResultSet rs = null;
		Hashtable result = new Hashtable();
		String weekAfterHoliday = String.format("%02d", week+1);
		String weekToPlan =  String.format("%02d", week);
		try {
			Statement s = conn.createStatement();
		    log.log(Level.INFO,"Statement established");
		    // Fetch table
		    String selection = "SELECT mapp.employee FROM  "+ getMappingTableName() +" mapp WHERE  mapp.customer = '"+customer+"' and mapp.employee NOT IN (select hol.employee from v_whd_holiday hol where hol.WEEK IN ('"+weekToPlan+"', '"+weekAfterHoliday+"')) order by (select count(*) from v_whd_prefer where customer = '"+customer+"' and employee = mapp.employee and WEEK = '"+week+"') desc, (select count(*) from v_whd_planning where customer = '"+customer+"' and employee = mapp.employee) asc, (select count(*) from v_whd_planning where week = '"+weekToPlan+"' and employee = mapp.employee) desc";
		    log.log(Level.INFO,"Query to execute: " + selection);
		    s.execute(selection);
		    rs = s.getResultSet();
		    while((rs!=null) && (rs.next())){
		    	result.put(i, rs.getString(1));
		    	i++;
		    }
		    s.close();
		    } catch (SQLException e) {
		    	conn.closeConnection();
		    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
		    }
		return result;
	}
	/**
	 * In this method the result from the checksum calculation is stored in the database table 
	 * @param ID Idetifier from the record
	 * @param checksum calculated checksum
	 */
	public void updatePath(String ID, String checksum) {
	try {
		Statement s = conn.createStatement();
	    log.log(Level.INFO,"Statement established");
	    // Fetch table
	    String query = "UPDATE "+getMappingTableName()+" SET Checksum = '"+checksum+"' WHERE ID = "+ID;
	    log.log(Level.INFO,"Query to execute: " + query);
	    s.execute(query);
	    ResultSet rs = s.getResultSet();
	    s.close();
	    } catch (SQLException e) {
	    	conn.closeConnection();
	    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
	    }
	}
	/**
	 * In this method the result from the checksum calculation is stored in the database table 
	 * @param ID Idetifier from the record
	 * @param checksum calculated checksum
	 */
	public void planWeek(String customer, String week){
	try {
		Statement s = conn.createStatement();
	    log.log(Level.INFO,"Statement established");
	    // Fetch table
	    String query = "INSERT INTO "+getPlanningTableName()+" (CUSTOMER, EMPLOYEE, WEEK) VALUES('"+customer+"','"+getDefaultEmployee()+"','"+week+"')";
	    log.log(Level.INFO,"Query to execute: " + query);
	    s.execute(query);
	    ResultSet rs = s.getResultSet();
	    s.close();
	    } catch (SQLException e) {
	    	conn.closeConnection();
	    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
	    }
	}
	/**
	 * In this method the result from the checksum calculation is stored in the database table 
	 * @param ID Idetifier from the record
	 * @param checksum calculated checksum
	 */
	public void planWeek(String customer, String employee, int week){
	try {
		Statement s = conn.createStatement();
	    log.log(Level.INFO,"Statement established");
	    // Fetch table
	    String weekToPlan =  String.format("%02d", week);
	    String query = "INSERT INTO "+getPlanningTableName()+" (CUSTOMER, EMPLOYEE, WEEK) VALUES('"+customer+"','"+employee+"','"+weekToPlan+"')";
	    log.log(Level.INFO,"Query to execute: " + query);
	    s.execute(query);
	    ResultSet rs = s.getResultSet();
	    s.close();
	    } catch (SQLException e) {
	    	conn.closeConnection();
	    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
	    }
	}
	/**
	 * In this method the result from the checksum calculation is stored in the database table 
	 * @param ID Idetifier from the record
	 * @param checksum calculated checksum
	 */
	public void insertData(String Kunde, String Mitarbeiter) {
	try {
		Statement s = conn.createStatement();
	    log.log(Level.INFO,"Statement established");
	    // Fetch table
	    String query = "INSERT INTO "+getMappingTableName()+" (Kunde, Mitarbeiter) VALUES('"+Kunde+"','"+Mitarbeiter+"')";
	    log.log(Level.INFO,"Query to execute: " + query);
	    s.execute(query);
	    ResultSet rs = s.getResultSet();
	    s.close();
	    } catch (SQLException e) {
	    	conn.closeConnection();
	    	log.log(Level.SEVERE,"Exception catched: " +e.getLocalizedMessage());
	    }
	}
}
