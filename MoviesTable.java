package dbexample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MoviesTable {

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
	    	  //String dropString = "DROP TABLE Movies CASCADE CONSTRAINTS";
	    	  //stmt.executeUpdate(dropString);
	    	  //System.out.println("Movies successfully table dropped");
	    	  stmt.execute("create or replace PROCEDURE movietable AS" + " " +
	    				"BEGIN" + " " +
	    			    "DECLARE" + " " +
	    			        "sqlstring VARCHAR2(1000) :=" + 
	    			            "'CREATE TABLE movies (movieid NUMBER(4), movietitle VARCHAR2(200), year NUMBER(4)," +  
	    			            "category CHAR(200), CONSTRAINT MOVIEID_PK PRIMARY KEY(movieid))';" + 
	    			    "BEGIN" + " " +
	    			        "EXECUTE IMMEDIATE sqlstring;" + 
	    			        "DBMS_OUTPUT.PUT_LINE('Movies Table Created');" + 
	    			    "END;" + 
	    			"END movietable;");
	    	  stmt.close();
		      conn.close();
		      //System.out.println("MoviesTable successfully created");
	      }
	      catch (SQLException se) {
	    	  stmt.close();
	    	  conn.close();
	    	  System.out.println( "SQL ERROR: " + se );
	      }

	}

}
