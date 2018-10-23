/**
 * HW 6
 */
package hw6;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

/**
 * Hw6Jdbc class runs the hw6 database tasks depending on user input.
 * 
 * @author Jose Carlos Martinez Garcia-Vaso <carlosgvaso@gmail.com>
 */
public class Hw6Jdbc {
	// Class variables
	private static final String usageStr = "Usage:\njava -jar hw6jdbc.jar <data generator> <physical organization> <index creation> <DB host> <DB user> <DB password>\n\nData generator:\n\tdgI - Use variation I of the data generator to load the database.\n\tdgII - Use variation II of the data generator to load the database.\n\nPhysical Organization:\n\tpo1 - Use the physical organization 1.\n\tpo2 - Use the physical organization 2.\n\tpo3 - Use the physical organization 3.\n\tpo4 - Use the physical organization 4.\n\nIndex creation:\n\tbefore - Create indexes before loading table.\n\tafter - Create indexes after laoding table.\n\nDB host:\n\t<host>:<port>//<sid> - Use the previous format to provide the DB host information.";
	private static enum DataGenerator { dgI, dgII, dgU; }
	private static enum PhysicalOrganization { po1, po2, po3, po4, poU; }
	private static enum IndexCreation { before, after, icU; }
	private static final int tableSize = 5000000;
	private static final int batchSize = 5000;
	private static final int randIntMin = 1;
	private static final int randIntMax = 50000;
	private static final String allowedChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-!@#$%^&*()[]{}+='\"`~,.;:/\\|?<> ";
	private static final int randStrMin = 1;
	private static final int randStrMax = 247;
	
	// Instance variables
	private DataGenerator dg;
	private PhysicalOrganization po;
	private IndexCreation ic;
	private String dbHost;
	private String dbUser;
	private String dbPassword;
	private Connection dbConn;
	private ArrayList<Integer> dgIIPriKey;
	private int queryVal[];
	
	
	/**
	 * Constructor: Initialize instance variables.
	 * 
	 * @param dataGen		Data generator used.
	 * @param physicalOrg	Physical organization used.
	 */
	private Hw6Jdbc(String dataGen, String physicalOrg, String indexCreation, String host, String user, String password) {
		System.out.print("Data generator: ");
		switch (dataGen) {
			case "dgI":
				System.out.println("I");
				this.dg = DataGenerator.dgI;
				break;
			case "dgII":
				System.out.println("II");
				this.dg = DataGenerator.dgII;
				break;
			default:
				System.out.println(dataGen);
				this.dg = DataGenerator.dgU;
				break;
		}
		System.out.print("Physical organization: ");
		switch (physicalOrg) {
			case "po1":
				System.out.println("1");
				this.po = PhysicalOrganization.po1;
				break;
			case "po2":
				System.out.println("2");
				this.po = PhysicalOrganization.po2;
				break;
			case "po3":
				System.out.println("3");
				this.po = PhysicalOrganization.po3;
				break;
			case "po4":
				System.out.println("4");
				this.po = PhysicalOrganization.po4;
				break;
			default:
				System.out.println(physicalOrg);
				this.po = PhysicalOrganization.poU;
				break;
		}
		System.out.print("Index Creation: ");
		switch (indexCreation) {
			case "before":
				System.out.println("before table loading");
				this.ic = IndexCreation.before;
				break;
			case "after":
				System.out.println("after table loading");
				this.ic = IndexCreation.after;
				break;
			default:
				System.out.println(indexCreation);
				this.ic = IndexCreation.icU;
				break;
		}
		this.dbHost = host;
		this.dbUser = user;
		this.dbPassword = password;
		
		/* Generate an array list with the values of theKey for DGII.
		 * It should be unique randomly ordered numbers from 1 to 5,000,000 inclusive.
		 */
		if (this.dg == Hw6Jdbc.DataGenerator.dgII) {
			System.out.print("Generating randomly ordered array for theKey...");
			
			this.dgIIPriKey = new ArrayList<>(Hw6Jdbc.tableSize);
			for (int i=1; i<=Hw6Jdbc.tableSize; i++){ 
				this.dgIIPriKey.add(i);
			}
			
			// Shuffle twice for better random order
			Collections.shuffle(this.dgIIPriKey);
			Collections.shuffle(this.dgIIPriKey);
			
			System.out.println(" Done");
		}
		// Generate 10 random query numbers to between 1 and 50,000
		System.out.print("Generating random query numbers...");
		this.queryVal = new int[10];
		boolean isRepeated = false;
		for (int i=0; i<this.queryVal.length; i++) {
			isRepeated = false;
			
			do {
				this.queryVal[i] = this.getRandomInt(Hw6Jdbc.randIntMin, Hw6Jdbc.randIntMax);
				
				for (int j=0; j<this.queryVal.length; j++) {
					if (i == j) {
						continue;
					} else if (this.queryVal[i] == this.queryVal[j]) {
						isRepeated = true;
						break;
					} 
				}
				
			} while (isRepeated == true);
		}
		System.out.print(" Done\nQuery numbers: [");
		for (int i=0; i<this.queryVal.length; i++) {
			if (i == (this.queryVal.length - 1) ) {
				System.out.println(this.queryVal[i] + "]");
			} else {
				System.out.print(this.queryVal[i] + ", ");
			}
		}
	}
	
	/**
	 * Create benchmark table with physical organization 1.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void createTablePO1() throws SQLException, SQLTimeoutException, NullPointerException {
		//System.out.print("Creating table...");
		
		// Create a Statement
	    Statement stmt = this.dbConn.createStatement();

	    // Create table benchmark with physical organization 1 (no secondary indexes)
	    stmt.executeUpdate ("CREATE TABLE benchmark (" + 
	    		"theKey NUMBER PRIMARY KEY, columnA NUMBER, " + 
	    		"columnB NUMBER, filler CHAR(247))");
	    
	    //System.out.println(" Done");
	}
	
	/**
	 * Create benchmark table with physical organization 2.
	 * 
	 * It creates the table as with createTablePO1 and then creates an index in columnA
	 * of the benchmark table.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void createIndexPO2() throws SQLException, SQLTimeoutException, NullPointerException {
		//System.out.print("Creating index...");
		
		// Create statement for index creation
	    Statement stmt = this.dbConn.createStatement();

	    // Create index on columnA of benchmark table for physical organization 2
	    stmt.executeUpdate ("CREATE INDEX colAidx ON benchmark (columnA)");
	    
	    //System.out.println(" Done");
	}
	
	/**
	 * Create benchmark table with physical organization 3.
	 * 
	 * It creates the table as with createTablePO1 and then creates an index in columnB
	 * of the benchmark table.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void createIndexPO3() throws SQLException, SQLTimeoutException, NullPointerException {
		//System.out.print("Creating index...");
		
		// Create statement for index creation
	    Statement stmt = this.dbConn.createStatement();

	    // Create index on columnB of benchmark table for physical organization 3
	    stmt.executeUpdate ("CREATE INDEX colBidx ON benchmark (columnB)");
	    
	    //System.out.println(" Done");
	}
	
	/**
	 * Create benchmark table with physical organization 4.
	 * 
	 * It creates the table as with createTablePO1 and then creates indexes in columnA
	 *  and columnB of the benchmark table.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void createIndexPO4() throws SQLException, SQLTimeoutException, NullPointerException {
		//System.out.print("Creating indexes...");
		
		// Create statements for index creation
	    Statement stmtA = this.dbConn.createStatement();
	    Statement stmtB = this.dbConn.createStatement();

	    // Create indexes in columnA and columnB of benchmark table for physical organization 4
	    stmtA.executeUpdate ("CREATE INDEX colAidx ON benchmark (columnA)");
	    stmtB.executeUpdate ("CREATE INDEX colBidx ON benchmark (columnB)");
	    
	    //System.out.println(" Done");
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
	 * Drop and purge benchmark table from database.
	 * 
	 * @throws SQLException
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void dropTable()  throws SQLException, SQLTimeoutException, NullPointerException {
		//System.out.print("Droping table...");
		
		// Create a Statement
	    Statement stmt = this.dbConn.createStatement();

	    // Drop and purge benchmark table
	    stmt.executeUpdate ("DROP TABLE benchmark PURGE");
	    
	    //System.out.println(" Done");
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
			sb.append(Hw6Jdbc.allowedChars.charAt(rand.nextInt(Hw6Jdbc.allowedChars.length())));
		}
		return sb.toString();
	}
	
	/**
	 * Load benchmark table with data generator I.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void loadTableDGI() throws SQLException, SQLTimeoutException, NullPointerException {
		//System.out.print("Loading data...");
		
		// Create a prepared statement
		String sql = "INSERT INTO benchmark (theKey, columnA, columnB, filler) values (?, ?, ?, ?)";
		PreparedStatement ps = this.dbConn.prepareStatement(sql);
		
		for (int i=0; i<Hw6Jdbc.tableSize; i++) {
			ps.setInt(1, i+1);	// theKey is ordered, and is starts at 1
			ps.setInt(2, this.getRandomInt(Hw6Jdbc.randIntMin, Hw6Jdbc.randIntMax));	// columnA ia a random int [1 - 50000]
			ps.setInt(3, this.getRandomInt(Hw6Jdbc.randIntMin, Hw6Jdbc.randIntMax));	// columnB is a random int [1 - 50000]
			ps.setString(4, this.getRandomStr(this.getRandomInt(Hw6Jdbc.randStrMin, Hw6Jdbc.randStrMax)));	// filler is a random string of length [0 - 247]
			ps.addBatch();
			
			if ((i+1) % Hw6Jdbc.batchSize == 0) {
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
		
		//System.out.println(" Done");
	}
	
	/**
	 * Load benchmark table with data generator II.
	 * 
	 * @throws SQLException 
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void loadTableDGII() throws SQLException, SQLTimeoutException, NullPointerException {
		//System.out.print("Loading data...");
		
		// Create a prepared statement
		String sql = "INSERT INTO benchmark (theKey, columnA, columnB, filler) values (?, ?, ?, ?)";
		PreparedStatement ps = this.dbConn.prepareStatement(sql);
		
		for (int i=0; i<Hw6Jdbc.tableSize; i++) {
			ps.setInt(1, this.dgIIPriKey.get(i));	// theKey is randomly ordered
			ps.setInt(2, this.getRandomInt(Hw6Jdbc.randIntMin, Hw6Jdbc.randIntMax));	// columnA ia a random int [1 - 50000]
			ps.setInt(3, this.getRandomInt(Hw6Jdbc.randIntMin, Hw6Jdbc.randIntMax));	// columnB is a random int [1 - 50000]
			ps.setString(4, this.getRandomStr(this.getRandomInt(Hw6Jdbc.randStrMin, Hw6Jdbc.randStrMax)));	// filler is a random string of length [0 - 247]
			ps.addBatch();
			
			if ((i+1) % Hw6Jdbc.batchSize == 0) {
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
		
		//System.out.println(" Done");
	}
	
	/**
	 * Run query 1 10 times for 10 random values.
	 * 
	 * Set up and drop the table every time to ensure the table is not cached.
	 * 
	 * @throws SQLException
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void query1() throws SQLException, SQLTimeoutException, NullPointerException {
		// Set up table
		try {
			this.setUpTable();
		} catch (SQLTimeoutException e) {
			System.out.println("ERROR: Timeout setting up table: " + e.getMessage());
			throw e;
		} catch (SQLException e) {
			System.out.println("ERROR: Cannot set up table: " + e.getMessage());
			throw e;
		} catch (NullPointerException e) {
			System.out.println("ERROR: Cannot set up table because of DB connection: " + e.getMessage());
			throw e;
		} catch (IllegalArgumentException e) {
			System.out.println("ERROR: Cannot set up table because of bad argument: " + e.getMessage());
			throw e;
		}
		
		System.out.println("Running query 1...");
		//System.out.print("\tRuntimes: ");
		
		long timing[];
		@SuppressWarnings("unused")
		int rowCount;
		
		// Create statement for query 1
	    Statement stmt = this.dbConn.createStatement();
	    
		// Run query 1 10 times with 10 different values
	    timing = new long[this.queryVal.length];
		for (int i=0; i<this.queryVal.length; i++) {
			timing[i] = System.currentTimeMillis();
			
			ResultSet rs = stmt.executeQuery("SELECT * FROM benchmark WHERE benchmark.columnA = " + this.queryVal[i]);
			
			rowCount = 0;
			while (rs.next()) {
				rowCount++;
			}
			timing[i] = System.currentTimeMillis() - timing[i];
			
			//System.out.println("Query 1 run " + (i+1) + " results:");
			//System.out.println("\tSearch value: " + this.queryVal[i]);
			//System.out.println("\tRows returned: " + rowCount);
			//System.out.println("\tRuntime: " + timing[i] + "ms / " + (timing[i]/60000.0) + "min");
			/*
			if (i != (this.queryVal.length - 1)) {
				System.out.print(timing[i] + ", ");
			} else {
				System.out.println(timing[i]);
			}
			*/
		}
		
		// Calculate average time
		double tSum = 0.0;
		for (int i=0; i<timing.length; i++) {
			tSum += timing[i];
		}
		System.out.println("Done");
		System.out.println("Query 1 average runtime: " + (tSum/timing.length) + "ms / " + ((tSum/timing.length)/60000.0) + "min");

		// Drop table
		try {
			this.dropTable();
		} catch (SQLTimeoutException e) {
			System.out.println("ERROR: Timeout dropping table: " + e.getMessage());
			throw e;
		} catch (SQLException e) {
			System.out.println("ERROR: Cannot drop table: " + e.getMessage());
			throw e;
		} catch (NullPointerException e) {
			System.out.println("ERROR: Cannot drop table because of DB connection: " + e.getMessage());
			throw e;
		}
	}
	
	/**
	 * Run query 2 10 times for 10 random values.
	 * 
	 * Set up and drop the table every time to ensure the table is not cached.
	 * 
	 * @throws SQLException
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void query2() throws SQLException, SQLTimeoutException, NullPointerException {
		// Set up table
		try {
			this.setUpTable();
		} catch (SQLTimeoutException e) {
			System.out.println("ERROR: Timeout setting up table: " + e.getMessage());
			throw e;
		} catch (SQLException e) {
			System.out.println("ERROR: Cannot set up table: " + e.getMessage());
			throw e;
		} catch (NullPointerException e) {
			System.out.println("ERROR: Cannot set up table because of DB connection: " + e.getMessage());
			throw e;
		} catch (IllegalArgumentException e) {
			System.out.println("ERROR: Cannot set up table because of bad argument: " + e.getMessage());
			throw e;
		}
		
		System.out.println("Running query 2...");
		//System.out.print("\tRuntimes: ");
		
		long timing[];
		@SuppressWarnings("unused")
		int rowCount;
		
		// Create statement for query 2
	    Statement stmt = this.dbConn.createStatement();
	    
		// Run query 2 10 times with 10 different values
	    timing = new long[this.queryVal.length];
		for (int i=0; i<this.queryVal.length; i++) {
			
			timing[i] = System.currentTimeMillis();
			
			ResultSet rs = stmt.executeQuery("SELECT * FROM benchmark WHERE benchmark.columnB = " + this.queryVal[i]);
			
			rowCount = 0;
			while (rs.next()) {
				rowCount++;
			}
			timing[i] = System.currentTimeMillis() - timing[i];
			
			//System.out.println("Query 2 run " + (i+1) + " results:");
			//System.out.println("\tSearch value: " + this.queryVal[i]);
			//System.out.println("\tRows returned: " + rowCount);
			//System.out.println("\tRuntime: " + timing[i] + "ms / " + (timing[i]/60000.0) + "min");
			/*
			if (i != (this.queryVal.length - 1)) {
				System.out.print(timing[i] + ", ");
			} else {
				System.out.println(timing[i]);
			}
			*/
		}
		
		// Calculate average time
		double tSum = 0.0;
		for (int i=0; i<timing.length; i++) {
			tSum += timing[i];
		}
		System.out.println("Done");
		System.out.println("Query 2 average runtime: " + (tSum/timing.length) + "ms / " + ((tSum/timing.length)/60000.0) + "min");
		
		// Drop table
		try {
			this.dropTable();
		} catch (SQLTimeoutException e) {
			System.out.println("ERROR: Timeout dropping table: " + e.getMessage());
			throw e;
		} catch (SQLException e) {
			System.out.println("ERROR: Cannot drop table: " + e.getMessage());
			throw e;
		} catch (NullPointerException e) {
			System.out.println("ERROR: Cannot drop table because of DB connection: " + e.getMessage());
			throw e;
		}
	}
	
	/**
	 * Run query 3 10 times for 10 random values.
	 * 
	 * Set up and drop the table every time to ensure the table is not cached.
	 * 
	 * @throws SQLException
	 * @throws SQLTimeoutException
	 * @throws NullPointerException
	 */
	private void query3() throws SQLException, SQLTimeoutException, NullPointerException {
		// Set up table
		try {
			this.setUpTable();
		} catch (SQLTimeoutException e) {
			System.out.println("ERROR: Timeout setting up table: " + e.getMessage());
			throw e;
		} catch (SQLException e) {
			System.out.println("ERROR: Cannot set up table: " + e.getMessage());
			throw e;
		} catch (NullPointerException e) {
			System.out.println("ERROR: Cannot set up table because of DB connection: " + e.getMessage());
			throw e;
		} catch (IllegalArgumentException e) {
			System.out.println("ERROR: Cannot set up table because of bad argument: " + e.getMessage());
			throw e;
		}
		
		System.out.println("Running query 3...");
		//System.out.print("\tRuntimes: ");
		
		long timing[];
		@SuppressWarnings("unused")
		int rowCount;
		
		// Create statement for query 3
	    Statement stmt = this.dbConn.createStatement();
	    
		// Run query 3 10 times with 10 different values
	    timing = new long[this.queryVal.length];
		for (int i=0; i<this.queryVal.length; i++) {
			timing[i] = System.currentTimeMillis();
			
			ResultSet rs = stmt.executeQuery("SELECT * FROM benchmark WHERE benchmark.columnA = " + this.queryVal[i] +
					"AND benchmark.columnB = " + this.queryVal[i]);
			
			rowCount = 0;
			while (rs.next()) {
				rowCount++;
			}
			timing[i] = System.currentTimeMillis() - timing[i];
			
			//System.out.println("Query 3 run " + (i+1) + " results:");
			//System.out.println("\tSearch value: " + this.queryVal[i]);
			//System.out.println("\tRows returned: " + rowCount);
			//System.out.println("\tRuntime: " + timing[i] + "ms / " + (timing[i]/60000.0) + "min");
			/*
			if (i != (this.queryVal.length - 1)) {
				System.out.print(timing[i] + ", ");
			} else {
				System.out.println(timing[i]);
			}
			*/
		}
		
		// Calculate average time
		double tSum = 0.0;
		for (int i=0; i<timing.length; i++) {
			tSum += timing[i];
		}
		System.out.println("Done");
		System.out.println("Query 3 average runtime: " + (tSum/timing.length) + "ms / " + ((tSum/timing.length)/60000.0) + "min");
		
		// Drop table
		try {
			this.dropTable();
		} catch (SQLTimeoutException e) {
			System.out.println("ERROR: Timeout dropping table: " + e.getMessage());
			throw e;
		} catch (SQLException e) {
			System.out.println("ERROR: Cannot drop table: " + e.getMessage());
			throw e;
		} catch (NullPointerException e) {
			System.out.println("ERROR: Cannot drop table because of DB connection: " + e.getMessage());
			throw e;
		}
	}
	
	/**
	 * Creates and loads the benchmark table.
	 * 
	 * @throws SQLException
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 */
	private void setUpTable() throws SQLException, NullPointerException, IllegalArgumentException {
		long tStart;
		long tStop;
		
		// Create the table with the specified physical organization
		switch (this.po) {
		case po1:
			try {
				// Track insert time
				tStart = System.currentTimeMillis();
				this.createTablePO1();
				
				// Load table with specified data generator
				switch (this.dg) {
				case dgI:
					try {
						this.loadTableDGI();
					} catch (SQLException | NullPointerException e) {
						throw e;
					}
					break;
				case dgII:
					try {
						this.loadTableDGII();
					} catch (SQLException | NullPointerException e) {
						throw e;
					}
					break;
				case dgU:
				default:
					System.out.println("ERROR: unknown data generator.");
					throw new IllegalArgumentException("Unknown data generator");
				}
				
				tStop = System.currentTimeMillis();
				System.out.println("Loading time: " + (tStop - tStart) + "ms / " + ((tStop - tStart)/60000.0) + "min");
			} catch (SQLException | NullPointerException e) {
				throw e;
			}
			break;
		case po2:
			try {
				switch (this.ic) {
				case before:
					// Track insert time
					tStart = System.currentTimeMillis();
					this.createTablePO1();
					this.createIndexPO2();
					
					// Load table with specified data generator
					switch (this.dg) {
					case dgI:
						try {
							this.loadTableDGI();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgII:
						try {
							this.loadTableDGII();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgU:
					default:
						System.out.println("ERROR: unknown data generator.");
						throw new IllegalArgumentException("Unknown data generator");
					}
					
					tStop = System.currentTimeMillis();
					System.out.println("Loading time before: " + (tStop - tStart) + "ms / " + ((tStop - tStart)/60000.0) + "min");
					break;
				case after:
					// Track insert time
					tStart = System.currentTimeMillis();
					this.createTablePO1();
					
					// Load table with specified data generator
					switch (this.dg) {
					case dgI:
						try {
							this.loadTableDGI();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgII:
						try {
							this.loadTableDGII();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgU:
					default:
						System.out.println("ERROR: unknown data generator.");
						throw new IllegalArgumentException("Unknown data generator");
					}
					
					this.createIndexPO2();
					tStop = System.currentTimeMillis();
					System.out.println("Loading time after: " + (tStop - tStart) + "ms / " + ((tStop - tStart)/60000.0) + "min");
					break;
				case icU:
				default:
					System.out.println("ERROR: unknown index creation.");
					throw new IllegalArgumentException("Unknown index creation");
				}
			} catch (SQLException | NullPointerException e) {
				throw e;
			}
			break;
		case po3:
			try {
				switch (this.ic) {
				case before:
					// Track insert time
					tStart = System.currentTimeMillis();
					this.createTablePO1();
					this.createIndexPO3();
					
					// Load table with specified data generator
					switch (this.dg) {
					case dgI:
						try {
							this.loadTableDGI();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgII:
						try {
							this.loadTableDGII();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgU:
					default:
						System.out.println("ERROR: unknown data generator.");
						throw new IllegalArgumentException("Unknown data generator");
					}
					
					tStop = System.currentTimeMillis();
					System.out.println("Loading time before: " + (tStop - tStart) + "ms / " + ((tStop - tStart)/60000.0) + "min");
					break;
				case after:
					// Track insert time
					tStart = System.currentTimeMillis();
					this.createTablePO1();
					
					// Load table with specified data generator
					switch (this.dg) {
					case dgI:
						try {
							this.loadTableDGI();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgII:
						try {
							this.loadTableDGII();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgU:
					default:
						System.out.println("ERROR: unknown data generator.");
						throw new IllegalArgumentException("Unknown data generator");
					}
					
					this.createIndexPO3();
					tStop = System.currentTimeMillis();
					System.out.println("Loading time after: " + (tStop - tStart) + "ms / " + ((tStop - tStart)/60000.0) + "min");
					break;
				case icU:
				default:
					System.out.println("ERROR: unknown index creation.");
					throw new IllegalArgumentException("Unknown index creation");
				}
			} catch (SQLException | NullPointerException e) {
				throw e;
			}
			break;
		case po4:
			try {
				switch (this.ic) {
				case before:
					// Track insert time
					tStart = System.currentTimeMillis();
					this.createTablePO1();
					this.createIndexPO4();
					
					// Load table with specified data generator
					switch (this.dg) {
					case dgI:
						try {
							this.loadTableDGI();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgII:
						try {
							this.loadTableDGII();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgU:
					default:
						System.out.println("ERROR: unknown data generator.");
						throw new IllegalArgumentException("Unknown data generator");
					}
					
					tStop = System.currentTimeMillis();
					System.out.println("Loading time before: " + (tStop - tStart) + "ms / " + ((tStop - tStart)/60000.0) + "min");
					break;
				case after:
					// Track insert time
					tStart = System.currentTimeMillis();
					this.createTablePO1();
					
					// Load table with specified data generator
					switch (this.dg) {
					case dgI:
						try {
							this.loadTableDGI();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgII:
						try {
							this.loadTableDGII();
						} catch (SQLException | NullPointerException e) {
							throw e;
						}
						break;
					case dgU:
					default:
						System.out.println("ERROR: unknown data generator.");
						throw new IllegalArgumentException("Unknown data generator");
					}
					
					this.createIndexPO4();
					tStop = System.currentTimeMillis();
					System.out.println("Loading time after: " + (tStop - tStart) + "ms / " + ((tStop - tStart)/60000.0) + "min");
					break;
				case icU:
				default:
					System.out.println("ERROR: unknown index creation.");
					throw new IllegalArgumentException("Unknown index creation");
				}
			} catch (SQLException | NullPointerException e) {
				throw e;
			}
			break;
		case poU:
		default:
			System.out.println("ERROR: unknown physical organization.");
			throw new IllegalArgumentException("Unknown physical organization");
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
		if (args.length != 6) {
			System.out.println("ERROR: wrong number of arguments.");
			System.out.println("\n" + usageStr);
			return;
		}
		
		Hw6Jdbc hw6 = new Hw6Jdbc(args[0].toString(), args[1].toString(), args[2].toString(), args[3].toString(), args[4].toString(), args[5].toString());
		
		// Check we got good DG and PO
		boolean err = false;
		if (hw6.dg == Hw6Jdbc.DataGenerator.dgU) {
			System.out.println("ERROR: unknown data generator.");
			err = true;
		}
		if (hw6.po == Hw6Jdbc.PhysicalOrganization.poU) {
			System.out.println("ERROR: unknown physical organization.");
			err = true;
		}
		if (err) {
			System.out.println("\n" + usageStr);
			return;
		}
		
		try {
			// Connect to DB
			try {
				hw6.dbConnect();
			} catch (SQLTimeoutException e) {
				System.out.println("ERROR: Timeout connecting to database: " + e.getMessage());
				try {
					hw6.dbDisconnect();
				} catch (SQLException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				} catch (NullPointerException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				}
				return;
			} catch (SQLException e) {
				System.out.println("ERROR: Cannot connect to database: " + e.getMessage());
				try {
					hw6.dbDisconnect();
				} catch (SQLException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				} catch (NullPointerException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				}
				return;
			}
			
			// Run queries
			try {
				hw6.query1();
				hw6.query2();
				hw6.query3();
			} catch (SQLTimeoutException e) {
				System.out.println("ERROR: Timeout running queries: " + e.getMessage());
				try {
					hw6.dbDisconnect();
				} catch (SQLException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				} catch (NullPointerException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				}
				return;
			} catch (SQLException e) {
				System.out.println("ERROR: Exception running queries: " + e.getMessage());
				try {
					hw6.dbDisconnect();
				} catch (SQLException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				} catch (NullPointerException e2) {
					System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
				}
				return;
			} catch (NullPointerException e) {
				System.out.println("ERROR: Exception running queries because of DB connection: " + e.getMessage());
				try {
					hw6.dbDisconnect();
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
				hw6.dbDisconnect();
			} catch (SQLException e2) {
				System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
			} catch (NullPointerException e2) {
				System.out.println("ERROR: Cannot disconnect from database: " + e2.getMessage());
			}
		}
		
	}
	
}
