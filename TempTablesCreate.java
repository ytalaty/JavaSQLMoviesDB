package dbexample;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class TempTablesCreate {

	public static void main(String[] args) throws SQLException, FileNotFoundException {
		String moviestemp = "Movies_Temp";
		String userstemp = "Users_Temp";
		String ratingstemp = "Ratings_Temp";
		
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
	          String dropString = "DROP TABLE Movies_Temp CASCADE CONSTRAINTS";
	          String dropString1 = "DROP TABLE Users_Temp CASCADE CONSTRAINTS";
	          String dropString2 = "DROP TABLE Ratings_Temp CASCADE CONSTRAINTS";
	          stmt.executeUpdate(dropString);
	          stmt.executeUpdate(dropString1);
	          stmt.executeUpdate(dropString2);
	          }
	       catch (SQLException se) {/*do nothing*/} // table doesn't exist
	      try {
	    	  System.out.print( "Building new " + moviestemp + " table...\n\n" );
	          String createString =
	             "CREATE TABLE " + moviestemp +
	             "  (MovieID NUMBER(4) PRIMARY KEY," +
	             "   MovieTitle VARCHAR2(20)," +
	             "   YEAR NUMBER(4)," +
	             "   Category CHAR(10) )";
	          stmt.executeUpdate(createString);
	          
	          System.out.print( "Building new " + userstemp + " table...\n\n" );
	          String createString1 = 
	        		  "CREATE TABLE " + userstemp +
	        		  " (UserID NUMBER(4) PRIMARY KEY," +
	        				"GENDER CHAR(1)," +
	        		  " AgeCode NUMBER(2)," +
	        		  " Occupation NUMBER(2)," +
	        		  " ZipCode VARCHAR2(10) )";
	          stmt.executeUpdate(createString1);
	          
	          System.out.print( "Building new " + ratingstemp + " table...\n\n" );
	          String createString2 =
	        		  "CREATE TABLE " + ratingstemp +
	        		  " (UserID NUMBER(4)," +
	        			" MovieID NUMBER(4)," +
	        		  " Rating NUMBER(1)," +
	        			"Timestamp NUMBER(9)," +
	        		  " CONSTRAINT PK_RATTEMP_UIDMID PRIMARY KEY (UserID, MovieID)," + 
	        		  "	CONSTRAINT FK_RATTEMP_TOUSER FOREIGN KEY (UserID) references Users_Temp(UserID)," + 
	        		  "	CONSTRAINT FK_RATTEMP_TOMOVIE FOREIGN KEY (MovieID) references Movies_Temp(MovieID) )";
	          stmt.executeUpdate(createString2);
	          
	      } catch (SQLException se) {
	    	  stmt.close();
	    	  conn.close();
	          System.out.println( "SQL ERROR: " + se );
	          }

	}

}
