package dbexample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class UsersTable {

	public static void main(String[] args) throws SQLException {
		Connection conn = null;
		Statement stmt = null;
		
		System.out.print( "\nLoading JDBC driver...\n\n" );
	      try {
	         Class.forName("oracle.jdbc.OracleDriver");
	         }
	      catch(ClassNotFoundException e) {
	         System.out.println(e);
	         System.exit(1);
	         }
	      
	      try {
	          System.out.print( "Connecting to CSC352 database...\n\n" );

	          conn = DriverManager.getConnection("jdbc:oracle:thin:@acadoradbprd01.dpu.depaul.edu:1521:ACADPRD0", "ytalaty", "cdm1538550");

	          System.out.println( "Connected to database CSC352..." );
	          System.out.println();
	          stmt = conn.createStatement();
	      }
	      catch (SQLException se) {
	          System.out.println(se);
	          System.exit(1);
	          }
	      
	      try {
	    	  //String dropString = "DROP TABLE Users CASCADE CONSTRAINTS";
	    	  //stmt.executeUpdate(dropString);
	    	  //System.out.println("Users successfully table dropped");
	    	  stmt.execute("create or replace PROCEDURE userstable AS " + " " +
	    				"BEGIN" + " " +
	    			    "DECLARE" + " " +
	    			        "sqlstring VARCHAR2(1000) :=" +
	    			            "'CREATE TABLE userss (userid NUMBER(4), gender VARCHAR2(2), agecode NUMBER(3)," +
	    			                "occupation NUMBER(4), zipcode VARCHAR2(20)," +
	    			                    "CONSTRAINT USERSID_PK PRIMARY KEY(userid))';" +
	    			    "BEGIN" + " " +
	    			        "EXECUTE IMMEDIATE sqlstring;" +
	    			        "DBMS_OUTPUT.PUT_LINE('Users Table Created');" +
	    			    "END;" +
	    			"END userstable;");
	    	  stmt.close();
		      conn.close();
		      //System.out.println("UsersTable successfully created");
	      }
	      catch (SQLException se) {
	    	  stmt.close();
	    	  conn.close();
	    	  System.out.println( "SQL ERROR: " + se );
	      }

	}

}
