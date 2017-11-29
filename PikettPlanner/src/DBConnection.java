import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBConnection {
	private static final Logger log = Logger.getLogger( PikettPlanner.class.getName() );
	private String dbms;
	private String serverName;
	private String databaseName;
	private String portNumber;
	private Connection con;
	DBHandlerProperty props;
	Connection conn;
	
	public DBConnection (DBHandlerProperty dbProperties){
		props = dbProperties;
		try{
			conn = getConnection();
	    }catch(Exception ex){
	    	log.log(Level.SEVERE,"Connection exception catched" +ex.getLocalizedMessage());
	    }
		
	}
	public String getDbms(){
		return this.dbms;
	}
	public String getServerName(){
		return this.serverName;
	}
	public String getDatabaseName(){
		return this.databaseName;
	}
	public void closeConnection(){
		if (this.con != null){
			try {
				this.con.close();
			} catch (SQLException e) {
				log.log(Level.SEVERE,"call closeDB() "+e.getMessage());
			}
		}
	}
	public Connection getConnection() throws SQLException {
		if (this.con != null){
			return this.con;
		}
		else{
			Connection conn = null;
		    dbms = props.getDBProperty("dbms");
		    serverName = props.getDBProperty("Server");
		    databaseName = props.getDBProperty("Database");
		    portNumber = props.getDBProperty("Port");
		    
		    
		    Properties connectionProps = new Properties();
		    connectionProps.put("userid", props.getDBProperty("user"));
		    connectionProps.put("password", props.getDBProperty("password"));
		    log.log(Level.INFO,"call getConnection("+connectionProps.toString()+")");

		    if (this.dbms.equals("mysql")) {
		    	try{
			        conn = (Connection)DriverManager.getConnection(
			                   "jdbc:" + dbms + "://" +
			                   serverName +
			                   ":" + portNumber + "/" +
			                   databaseName,
			                   connectionProps);
			        log.log(Level.INFO,"Connected to database");
		    	}catch (SQLException e){
		    		log.log(Level.SEVERE,"DBConnection error " +e);
		    	}
		    }
	    	else if (this.dbms.equals("ORCL")) {
		    	try{
		    		String URL = "jdbc:oracle:thin:@localhost:1521:xe"+ props.getDBProperty("user")+ props.getDBProperty("password");
		    		log.log(Level.INFO,"Connection URL: "+ URL);
		    		log.log(Level.INFO,"Try to connect database");
		    		//conn=DriverManager.getConnection( "jdbc:oracle:thin:@localhost:1521:xe","system","oracle");  
		    		conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", props.getDBProperty("user"), props.getDBProperty("password"));
		    		log.log(Level.INFO,"Connected to database");
			    	}catch (SQLException e){
			    		log.log(Level.SEVERE,"DBConnection error " +e);
			    	}
		    }else {
		    	conn = null;
		    	log.log(Level.SEVERE,"Database unknown");
		    }
		    this.con = conn;
		    return conn;
		}
	}
	/**
	 * Creates a statement
	 * @return Statement
	 */

	Statement createStatement() throws SQLException { 
		return conn.createStatement();
	}
}