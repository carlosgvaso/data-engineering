/**
 * 
 */
package hw6;
import java.sql.*;

/**
 * Hw6Jdbc class runs the hw6 tasks depending on user input.
 * 
 * @author Jose Carlos Martinez Garcia-Vaso <carlosgvaso@gmail.com>
 *
 */
public class Hw6Jdbc {
	// Class variables
	private static String usageStr = "Usage:\njava -jar hw6jdbc.jar <data generator> <physical organization> <DB host> <DB user> <DB password>\n\nData generator:\n\tdgI - Use variation I of the data generator to load the database.\n\tdgII - Use variation II of the data generator to load the database.\n\nPhysical Organization:\n\tpo1 - Use the physical organization 1.\n\tpo2 - Use the physical organization 2.\n\tpo3 - Use the physical organization 3.\n\tpo4 - Use the physical organization 4.\n\nDB host:\n\t<host>:<port>:<sid> - Use the previous format to provide the DB host information.";
	private static enum DataGenerator { dgI, dgII, dgU; }
	private static enum PhysicalOrganization { po1, po2, po3, po4, poU; }
	
	// Instance variables
	private DataGenerator dg;
	private PhysicalOrganization po;
	private String dbHost;
	private String dbUser;
	private String dbPassword;
	
	
	/**
	 * Constructor: Initialize instance variables.
	 * 
	 * @param dataGen		Data generator used.
	 * @param physicalOrg	Physical organization used.
	 */
	private Hw6Jdbc(String dataGen, String physicalOrg, String host, String user, String password) {
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
		this.dbHost = host;
		this.dbUser = user;
		this.dbPassword = password;
	}
	
	/**
	 * Connect to the database.
	 */
	private void dbConnect() throws SQLException {
		// Load the Oracle JDBC driver
	    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

	    // Connect to the database
	    // You must put a database name after the @ sign in the connection URL.
	    // You can use either the fully specified SQL*net syntax or a short cut
	    // syntax as <host>:<port>:<sid>.  The example uses the short cut syntax.
	    Connection conn =
	      DriverManager.getConnection ("jdbc:oracle:thin:@" + this.dbHost,
					   this.dbUser, this.dbPassword);

	    // Create a Statement
	    Statement stmt = conn.createStatement ();

	    // Select the table names from the user_tables
	    ResultSet rset = stmt.executeQuery ("select TABLE_NAME from USER_TABLES");

	    // Iterate through the result and print out the table names
	    while (rset.next ())
	      System.out.println (rset.getString (1));
	}
	
	/**
	 * Read input arguments, and run the corresponding tasks.
	 * @param args	Input arguments. 
	 */
	public static void main(String[] args) {
		// Check the input arguments
		if (args.length != 5) {
			System.out.println("ERROR: wrong number of arguments.");
			System.out.println("\n" + usageStr);
			return;
		}
		
		Hw6Jdbc hw6 = new Hw6Jdbc(args[0].toString(), args[1].toString(), args[2].toString(), args[3].toString(), args[4].toString());
		
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
	}

}
