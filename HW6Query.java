package dbexample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HW6Query {

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
	      ResultSet rset = stmt.executeQuery( "SELECT DISTINCT(USERSS.USERID), AVG(RATINGS.RATING) AS RATING FROM USERSS INNER JOIN RATINGS ON USERSS.USERID = RATINGS.USERID WHERE AGECODE=56 AND OCCUPATION=13 GROUP BY USERSS.USERID ORDER BY USERSS.USERID");
	      System.out.println("Running Query for Homework 6 ...\n");
	      System.out.println("UserID                 AVG_RATING");
	      System.out.println("------------------------------------------------------");
	      while( rset.next() ) {
	    	  System.out.println( rset.getString("USERID") + "         " + rset.getString("RATING"));
	      }
	      rset.close();
	      stmt.close();
          conn.close();
	      }
	      catch (SQLException se) {
	    	  conn.close();
	          System.out.println( "SQL ERROR: " + se );
	          }

	}

}
