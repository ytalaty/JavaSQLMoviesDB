package dbexample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RatingsImport {

	public static void main(String[] args) throws SQLException {
		ResultSet rs = null;
		int records = 0;
		String Ratings = "RATINGS";
	      String Ratings_Temp = "RATINGS_TEMP";
	      
	      
	      //Connections and Statements Tracker
	      Connection conn = null; 
	      Statement stmt = null;
	      Statement stmt4 = null; //Movies_Temp
	    
	      /***********************************************************************
	      *  determine if the JDBC driver exists and load it...
	      ***********************************************************************/
	      System.out.print( "\nLoading JDBC driver...\n\n" );
	      try {
	         Class.forName("oracle.jdbc.OracleDriver");
	         }
	      catch(ClassNotFoundException e) {
	         System.out.println(e);
	         System.exit(1);
	         }

	      /***********************************************************************
	      *  establish a connection to the database...
	      ***********************************************************************/
	      try {
	         System.out.print( "Connecting to ACADPRD0 database...\n\n" );
	         //String url = dataSource + dbName;

	         conn = DriverManager.getConnection("jdbc:oracle:thin:@acadoradbprd01.dpu.depaul.edu:1521:ACADPRD0", 
	        		 "ytalaty", "cdm1538550");


	         /*conn = dbms.equals("localAccess") ? DriverManager.getConnection(url)
	            : DriverManager.getConnection(url, userName, password );*/
	         System.out.println( "Connected to database ACADPRD0...\n" );

	         /***********************************************************************
	         *  create an object by which we will pass SQL stmts to the database...
	         ***********************************************************************/
	         stmt4 = conn.createStatement();
	         stmt = conn.createStatement();
	         }
	      catch (SQLException se) {
	         System.out.println(se);
	         System.exit(1);
	         }
	      
	      
	      /***********************************************************************
	       *  Create Temporary Movies_Temp Table
	       ***********************************************************************/
	      try {
	          String mIDT_drop = "DROP TABLE " + Ratings;
	          stmt4.executeUpdate(mIDT_drop);
	   	          }
	   	catch (SQLException se) {/*do nothing*/} // table doesn't exist
	    	
	    	try {
	   	   
	    		 System.out.print( "Building new " + Ratings + " table...\n\n" );
	    		 String createString1 =
		        		  "CREATE TABLE " + Ratings +
		        		  " (UserID NUMBER(4)," +
		        			" MovieID NUMBER(4)," +
		        		  " Rating NUMBER(1)," +
		        			"Timestamp NUMBER(20)," +
		        		  " CONSTRAINT PK_RATTEMP_UIDMID PRIMARY KEY (UserID, MovieID)," + 
		        		  "	CONSTRAINT FK_RATTEMP_TOUSER FOREIGN KEY (UserID) references userss(UserID)," + 
		        		  "	CONSTRAINT FK_RATTEMP_TOMOVIE FOREIGN KEY (MovieID) references Movies_Temp(MovieID) )";
		          stmt.executeUpdate(createString1);
	          
	         
	        PreparedStatement uRT = conn.prepareStatement("INSERT INTO RATINGS (SELECT RATINGS_TEMP.USERID, RATINGS_TEMP.MOVIEID, RATINGS_TEMP.RATING, RATINGS_TEMP.TIMESTAMP FROM RATINGS_TEMP)");
	        conn.setAutoCommit(false);
	           uRT.execute();
	           conn.commit();
	        
	        uRT.close();
	        System.out.println("Data has been imported into Ratings Table\n");
	        String sql = "SELECT COUNT(*) FROM RATINGS";
	        PreparedStatement prest = conn.prepareStatement(sql);
	        rs = prest.executeQuery();
	        while (rs.next())
	        	records = rs.getInt(1);
	        System.out.println("Number of records loaded: " + records);
	        conn.close();
	        stmt4.close();
	   	   
	      }
	      
	      catch (SQLException se) {
	          System.out.println( "SQL ERROR: " + se );
	          conn.close();
	          }
	        		
	    		
	       /***********************************************************************
	       *  finally, display all the rows in the database...
	       ***********************************************************************/

	        conn.close();
	       
	      } // end procedure
}
