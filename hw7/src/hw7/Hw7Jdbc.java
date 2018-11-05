/**
 * HW 7
 */
package hw7;
import java.sql.*;
import java.util.Random;

/**
 * Hw7Jdbc class runs the hw6 database tasks depending on user input.
 * 
 * @author Jose Carlos Martinez Garcia-Vaso <carlosgvaso@gmail.com>
 */
public class Hw7Jdbc {
	// Class variables
	private static final String usageStr = "Usage:\njava -jar hw7jdbc.jar <DB host> <DB user> <DB password>\n\nDB host:\n\t<host>:<port>//<sid> - Use the previous format to provide the DB host information.";
	private static final int tableSize = 5000000;
	private static final int batchSize = 5000;
	private static final int randIntMin = 0;
	private static final int randIntMaxHt = 99999;
	private static final int randIntMaxTt = 9999;
	private static final int randIntMaxOt = 999;
	private static final int randIntMaxHund = 99;
	private static final int randIntMaxTen = 9;
	private static final String allowedChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-!@#$%^&*()[]{}+='\"`~,.;:/\\|?<> ";
	private static final int randStrMin = 256;
	private static final int randStrMax = 263;
	
	// Instance variables
	private String dbHost;
	private String dbUser;
	private String dbPassword;
	private Connection dbConn;
	
	
	/**
	 * Constructor: Initialize instance variables.
	 * 
	 * @param dataGen		Data generator used.
	 * @param physicalOrg	Physical organization used.
	 */
	private Hw7Jdbc(String host, String user, String password) {
		this.dbHost = host;
		this.dbUser = user;
		this.dbPassword = password;
	}
	
	/**
	 * Copy data from table A to B, C, Aprime, Bprime and Cprime.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void copyTableData() throws SQLException, SQLTimeoutException, NullPointerException {
		System.out.println("Copying data from table A...");
		
		// Create a statement
		Statement stmt = this.dbConn.createStatement();
		
		// Copy data
	    stmt.executeUpdate("INSERT INTO B SELECT * FROM A");
	    System.out.println("\tTable B");
		
	    stmt.executeUpdate("INSERT INTO C SELECT * FROM A");
	    System.out.println("\tTable C");
		
	    stmt.executeUpdate("INSERT INTO Aprime SELECT * FROM A");
	    System.out.println("\tTable Aprime");
		
	    stmt.executeUpdate("INSERT INTO Bprime SELECT * FROM A");
	    System.out.println("\tTable Bprime");
		
	    stmt.executeUpdate("INSERT INTO Cprime SELECT * FROM A");
	    System.out.println("\tTable Cprime");
		
		System.out.println("Done");
	}
	
	/**
	 * Create tables A, B, C, Aprime, Bprime and Cprime without indexes.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void createTables() throws SQLException, SQLTimeoutException, NullPointerException {
		System.out.println("Creating tables...");
		
		// Create a Statement
	    Statement stmt = this.dbConn.createStatement();

	    // Create tables A, B, C (no secondary indexes)
	    stmt.executeUpdate("CREATE TABLE A (pk NUMBER(7) PRIMARY KEY, ht NUMBER(5), " +
	    		"tt NUMBER(4), ot NUMBER(3), hund NUMBER(2), ten NUMBER(1), filler CHAR(264))");
	    System.out.println("\tTable A");
	    
	    stmt.executeUpdate("CREATE TABLE B (pk NUMBER(7) PRIMARY KEY, ht NUMBER(5), " + 
	    		"tt NUMBER(4), ot NUMBER(3), hund NUMBER(2), ten NUMBER(1), filler CHAR(264))");
	    System.out.println("\tTable B");

	    
	    stmt.executeUpdate("CREATE TABLE C (pk NUMBER(7) PRIMARY KEY, ht NUMBER(5), " +
	    		"tt NUMBER(4), ot NUMBER(3), hund NUMBER(2), ten NUMBER(1), filler CHAR(264))");
	    System.out.println("\tTable C");
	    
	    // Create tables Aprime, Bprime, Cprime (no secondary indexes added here)
	    stmt.executeUpdate("CREATE TABLE Aprime (pk NUMBER(7) PRIMARY KEY, ht NUMBER(5), " +
	    		"tt NUMBER(4), ot NUMBER(3), hund NUMBER(2), ten NUMBER(1), filler CHAR(264))");
	    System.out.println("\tTable Aprime");
	    
	    stmt.executeUpdate("CREATE TABLE Bprime (pk NUMBER(7) PRIMARY KEY, ht NUMBER(5), " +
	    		"tt NUMBER(4), ot NUMBER(3), hund NUMBER(2), ten NUMBER(1), filler CHAR(264))");
	    System.out.println("\tTable Bprime");

	    
	    stmt.executeUpdate("CREATE TABLE Cprime (pk NUMBER(7) PRIMARY KEY, ht NUMBER(5), " +
	    		"tt NUMBER(4), ot NUMBER(3), hund NUMBER(2), ten NUMBER(1), filler CHAR(264))");
	    System.out.println("\tTable Cprime");
	    
	    System.out.println("Done");
	}
	
	/**
	 * Create the indexes in the prime tables.
	 * 
	 * Indexes are created for all prime tables in columns ht, tt, ot, hund and ten.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void createIndexes() throws SQLException, SQLTimeoutException, NullPointerException {
		System.out.println("Creating indexes...");
		
		// Create statement for index creation
	    Statement stmt = this.dbConn.createStatement();

	    // Create indexes
	    stmt.executeUpdate("CREATE INDEX AprimeHtIdx ON Aprime (ht)");
	    stmt.executeUpdate("CREATE INDEX AprimeTtIdx ON Aprime (tt)");
	    stmt.executeUpdate("CREATE INDEX AprimeOtIdx ON Aprime (ot)");
	    stmt.executeUpdate("CREATE INDEX AprimeHundIdx ON Aprime (hund)");
	    stmt.executeUpdate("CREATE INDEX AprimeTenIdx ON Aprime (ten)");
	    System.out.println("\tTable Aprime indexes: ht, tt, ot, hund, ten");
	    
	    stmt.executeUpdate("CREATE INDEX BprimeHtIdx ON Bprime (ht)");
	    stmt.executeUpdate("CREATE INDEX BprimeTtIdx ON Bprime (tt)");
	    stmt.executeUpdate("CREATE INDEX BprimeOtIdx ON Bprime (ot)");
	    stmt.executeUpdate("CREATE INDEX BprimeHundIdx ON Bprime (hund)");
	    stmt.executeUpdate("CREATE INDEX BprimeTenIdx ON Bprime (ten)");
	    System.out.println("\tTable Bprime indexes: ht, tt, ot, hund, ten");
	    
	    stmt.executeUpdate("CREATE INDEX CprimeHtIdx ON Cprime (ht)");
	    stmt.executeUpdate("CREATE INDEX CprimeTtIdx ON Cprime (tt)");
	    stmt.executeUpdate("CREATE INDEX CprimeOtIdx ON Cprime (ot)");
	    stmt.executeUpdate("CREATE INDEX CprimeHundIdx ON Cprime (hund)");
	    stmt.executeUpdate("CREATE INDEX CprimeTenIdx ON Cprime (ten)");
	    System.out.println("\tTable Cprime indexes: ht, tt, ot, hund, ten");
	    
	    System.out.println("Done");
	}
	
	/**
	 * Connect to the database.
	 * 
	 * @throws SQLException
	 * @throws SQLTimeoutException
	 */
	private void dbConnect() throws SQLException, SQLTimeoutException {
		// Load the Oracle JDBC driver
	    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

	    // Connect to the database
	    // You must put a database name after the @ sign in the connection URL.
	    // You can use either the fully specified SQL*net syntax or a short cut
	    // syntax as <host>:<port>/<sid>.  The example uses the short cut syntax.
	    this.dbConn = DriverManager.getConnection ("jdbc:oracle:thin:@" + this.dbHost,
					   this.dbUser, this.dbPassword);
	}
	
	/**
	 * Disconnect form the database.
	 * 
	 * @throws SQLException
	 * @throws NullPointerException
	 */
	private void dbDisconnect() throws SQLException, NullPointerException {
		// Check we are connected, and disconnect from DB
		if (!this.dbConn.isClosed()) {
			this.dbConn.close();
		}
	}
	
	/**
	 * Drop and purge A, B, C, Aprime, Bprime and Cprime tables from database.
	 * 
	 * @throws SQLException
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void dropTables() throws SQLException, SQLTimeoutException, NullPointerException {
		System.out.println("Droping tables...");
		
		// Create a Statement
	    Statement stmt = this.dbConn.createStatement();

	    // Drop and purge tables
	    stmt.executeUpdate ("BEGIN " + 
	    		"EXECUTE IMMEDIATE 'DROP TABLE A PURGE'; " + 
	    		"EXCEPTION " + 
	    		"WHEN OTHERS THEN " + 
	    		"IF SQLCODE != -942 THEN " + 
	    		"RAISE; " + 
	    		"END IF; " + 
	    		"END;");
	    System.out.println("\tTable A");
	    
	    stmt.executeUpdate ("BEGIN " + 
	    		"EXECUTE IMMEDIATE 'DROP TABLE B PURGE'; " + 
	    		"EXCEPTION " + 
	    		"WHEN OTHERS THEN " + 
	    		"IF SQLCODE != -942 THEN " + 
	    		"RAISE; " + 
	    		"END IF; " + 
	    		"END;");
	    System.out.println("\tTable B");
	    
	    stmt.executeUpdate ("BEGIN " + 
	    		"EXECUTE IMMEDIATE 'DROP TABLE C PURGE'; " + 
	    		"EXCEPTION " + 
	    		"WHEN OTHERS THEN " + 
	    		"IF SQLCODE != -942 THEN " + 
	    		"RAISE; " + 
	    		"END IF; " + 
	    		"END;");
	    System.out.println("\tTable C");
	    
	    stmt.executeUpdate ("BEGIN " + 
	    		"EXECUTE IMMEDIATE 'DROP TABLE Aprime PURGE'; " + 
	    		"EXCEPTION " + 
	    		"WHEN OTHERS THEN " + 
	    		"IF SQLCODE != -942 THEN " + 
	    		"RAISE; " + 
	    		"END IF; " + 
	    		"END;");
	    System.out.println("\tTable Aprime");
	    
	    stmt.executeUpdate ("BEGIN " + 
	    		"EXECUTE IMMEDIATE 'DROP TABLE Bprime PURGE'; " + 
	    		"EXCEPTION " + 
	    		"WHEN OTHERS THEN " + 
	    		"IF SQLCODE != -942 THEN " + 
	    		"RAISE; " + 
	    		"END IF; " + 
	    		"END;");
	    System.out.println("\tTable Bprime");
	    
	    stmt.executeUpdate ("BEGIN " + 
	    		"EXECUTE IMMEDIATE 'DROP TABLE Cprime PURGE'; " + 
	    		"EXCEPTION " + 
	    		"WHEN OTHERS THEN " + 
	    		"IF SQLCODE != -942 THEN " + 
	    		"RAISE; " + 
	    		"END IF; " + 
	    		"END;");
	    System.out.println("\tTable Cprime");
	    
	    System.out.println("Done");
	}
	
	
	/**
	 * Generate table statistics.
	 * 
	 * @throws SQLException
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void generateStatistics() throws SQLException, SQLTimeoutException, NullPointerException {
		System.out.println("Gathering statistics...");
		
		// Create a Statement
	    Statement stmt = this.dbConn.createStatement();

	    // Gather statistics
	    stmt.executeUpdate("analyze table HR.A VALIDATE structure");
	    stmt.executeUpdate("begin " + 
	    		"DBMS_STATS.GATHER_TABLE_STATS (ownname => 'HR', tabname => 'A', estimate_percent => 100); " + 
	    		"end;");
	    System.out.println("\tTable A");
	    
	    stmt.executeUpdate("analyze table HR.B VALIDATE structure");
	    stmt.executeUpdate("begin " + 
	    		"DBMS_STATS.GATHER_TABLE_STATS (ownname => 'HR', tabname => 'B', estimate_percent => 100); " + 
	    		"end;");
	    System.out.println("\tTable B");
	    
	    stmt.executeUpdate("analyze table HR.C VALIDATE structure");
	    stmt.executeUpdate("begin " + 
	    		"DBMS_STATS.GATHER_TABLE_STATS (ownname => 'HR', tabname => 'C', estimate_percent => 100); " + 
	    		"end;");
	    System.out.println("\tTable C");
	    
	    stmt.executeUpdate("analyze table HR.Aprime VALIDATE structure");
	    stmt.executeUpdate("begin " + 
	    		"DBMS_STATS.GATHER_TABLE_STATS (ownname => 'HR', tabname => 'Aprime', estimate_percent => 100); " + 
	    		"end;");
	    System.out.println("\tTable Aprime");
	    
	    stmt.executeUpdate("analyze table HR.Bprime VALIDATE structure");
	    stmt.executeUpdate("begin " + 
	    		"DBMS_STATS.GATHER_TABLE_STATS (ownname => 'HR', tabname => 'Bprime', estimate_percent => 100); " + 
	    		"end;");
	    System.out.println("\tTable Bprime");
	    
	    stmt.executeUpdate("analyze table HR.Cprime VALIDATE structure");
	    stmt.executeUpdate("begin " + 
	    		"DBMS_STATS.GATHER_TABLE_STATS (ownname => 'HR', tabname => 'Cprime', estimate_percent => 100); " + 
	    		"end;");
	    System.out.println("\tTable Cprime");
	    
	    System.out.println("Done");
		
	}
	
	/**
	 * Generate a pseudorandom in between min and max inclusive.
	 * 
	 * @param min	Min int boundary.
	 * @param max	Max int boundary.
	 * @return	Random int between min and max inclusive.
	 */
 	private int getRandomInt(int min, int max) {
		Random rand = new Random();
		return rand.nextInt((max - min) + 1) + min;
	}
	
	/**
	 * Generate a pseudorandom String.
	 * 
	 * @param len	Length of the string.
	 * @return	Random string of lenght l.
	 */
	private String getRandomStr(int len) {
		Random rand = new Random();
		StringBuilder sb = new StringBuilder(len);
		for(int i=0; i<len; i++) {
			sb.append(Hw7Jdbc.allowedChars.charAt(rand.nextInt(Hw7Jdbc.allowedChars.length())));
		}
		return sb.toString();
	}
	
	/**
	 * Load table A with data.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void loadTableA() throws SQLException, SQLTimeoutException, NullPointerException {
		System.out.println("Loading data to table A...");
		
		// Create a prepared statement
		String sql = "INSERT INTO A (pk, ht, tt, ot, hund, ten, filler) values (?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement ps = this.dbConn.prepareStatement(sql);
		
		for (int i=0; i<Hw7Jdbc.tableSize; i++) {
			ps.setInt(1, i+1);	// pk is ordered, and is starts at 1
			ps.setInt(2, this.getRandomInt(Hw7Jdbc.randIntMin, Hw7Jdbc.randIntMaxHt));		// ht ia a random int [0 - 99999]
			ps.setInt(3, this.getRandomInt(Hw7Jdbc.randIntMin, Hw7Jdbc.randIntMaxTt));		// tt is a random int [0 - 9999]
			ps.setInt(4, this.getRandomInt(Hw7Jdbc.randIntMin, Hw7Jdbc.randIntMaxOt));		// ot is a random int [0 - 999]
			ps.setInt(5, this.getRandomInt(Hw7Jdbc.randIntMin, Hw7Jdbc.randIntMaxHund));	// hund is a random int [0 - 99]
			ps.setInt(6, this.getRandomInt(Hw7Jdbc.randIntMin, Hw7Jdbc.randIntMaxTen));		// ten is a random int [0 - 9]
			ps.setString(7, this.getRandomStr(this.getRandomInt(Hw7Jdbc.randStrMin, Hw7Jdbc.randStrMax)));	// filler is a random string of length [0 - 247]
			ps.addBatch();
			
			if ((i+1) % Hw7Jdbc.batchSize == 0) {
				try {
					ps.executeBatch();
				} catch (BatchUpdateException e) {
					System.out.println("ERROR: Error executing a batch insert: " + e.getMessage());
				}
			}
		}
		try {
			ps.executeBatch();
		} catch (BatchUpdateException e) {
			System.out.println("ERROR: Error executing a batch insert: " + e.getMessage());
		}
		ps.close();
		
		System.out.println("Done");
	}
	
	/**
	 * Creates and loads the tables.
	 * 
	 * @throws SQLException
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 */
	private void setUpTables() throws SQLException, NullPointerException, IllegalArgumentException {
		try {
			this.dropTables();
			this.createTables();
			this.loadTableA();
			this.copyTableData();
			this.createIndexes();
			this.generateStatistics();
		} catch (SQLException | NullPointerException e) {
			throw e;
		}
	}
	
	/**
	 * Read input arguments, and run the corresponding tasks.
	 * 
	 * Entry point.
	 * 
	 * @param args	Input arguments. 
	 */
	public static void main(String[] args) {
		// Check the input arguments
		if (args.length != 3) {
			System.out.println("ERROR: wrong number of arguments.");
			System.out.println("\n" + usageStr);
			return;
		}
		
		Hw7Jdbc hw7 = new Hw7Jdbc(args[0].toString(), args[1].toString(), args[2].toString());
		
		try {
			// Connect to DB
			try {
				hw7.dbConnect();
			} catch (SQLTimeoutException e) {
				System.out.println("ERROR: Timeout connecting to database: " + e.getMessage());
				try {
					hw7.dbDisconnect();
				} catch (SQLException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				} catch (NullPointerException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				}
				return;
			} catch (SQLException e) {
				System.out.println("ERROR: Cannot connect to database: " + e.getMessage());
				try {
					hw7.dbDisconnect();
				} catch (SQLException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				} catch (NullPointerException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				}
				return;
			}
			
			// Load tables
			try {
				hw7.setUpTables();
			} catch (SQLTimeoutException e) {
				System.out.println("ERROR: Timeout loading tables: " + e.getMessage());
				try {
					hw7.dbDisconnect();
				} catch (SQLException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				} catch (NullPointerException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				}
				return;
			} catch (SQLException e) {
				System.out.println("ERROR: Exception loading tables: " + e.getMessage());
				try {
					hw7.dbDisconnect();
				} catch (SQLException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				} catch (NullPointerException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				}
				return;
			} catch (NullPointerException e) {
				System.out.println("ERROR: Exception loading tables because of DB connection: " + e.getMessage());
				try {
					hw7.dbDisconnect();
				} catch (SQLException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				} catch (NullPointerException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				}
				return;
			}
		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
		} finally {
			try {
				hw7.dbDisconnect();
			} catch (SQLException e2) {
				System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
			} catch (NullPointerException e2) {
				System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
			}
		}
		
	}
	
}
