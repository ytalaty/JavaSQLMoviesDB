package dbexample;

import java.sql.*;
import java.io.*;

public class AgeDecode {
	
	

  	public static void main ( String args []) throws SQLException {
	   ResultSet rs = null;
	   int records = 0;
  		
	  // Temporary Tables 
      String Userss = "USERSS";
      String Users_Temp = "USERS_TEMP";
      
      String AgeDecode = "AGEDECODE";
      String OccupationDecode = "OCCUPATIONDECODE";
      
      
      //Connections and Statements Tracker
      Statement stmt1 = null;
      Connection conn = null; 
      Statement stmt2 = null;
      Statement stmt6 = null; //Movies_Temp
      Statement stmt7 = null;
      Statement stmt8 = null;
    
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
         stmt6 = conn.createStatement();
         stmt2 = conn.createStatement();
         stmt1 = conn.createStatement();
         stmt7 = conn.createStatement();
         stmt8 = conn.createStatement();
         }
      catch (SQLException se) {
         System.out.println(se);
         System.exit(1);
         }
      
      try {
          String users_drop = "DROP TABLE " + Users_Temp;
	          stmt2.executeUpdate(users_drop);
	          }
	    catch (SQLException se) {/*do nothing*/} // table doesn't exist
      
      

	      
      
      
      /***********************************************************************
       *  Create Temporary Movies_Temp Table
       ***********************************************************************/
      try {
          String ageD_drop = "DROP TABLE " + AgeDecode;
          stmt7.executeUpdate(ageD_drop);
   	          }
   	catch (SQLException se) {/*do nothing*/} // table doesn't exist
    	
    	try {
   	   
    		System.out.print( "Creating new " + AgeDecode + " table...\n" );
            String ageDecodeTable =
               "CREATE TABLE " + AgeDecode +
               " (AGECODE NUMBER(4), AGEDECODED CHAR(10))";
            stmt7.executeUpdate(ageDecodeTable);
            
          System.out.println("AgeDecode Table has been created succesfully.");
          
         
        PreparedStatement iAgeDec = conn.prepareStatement("INSERT INTO " + AgeDecode + "(SELECT AGECODE, "
        		+ "case when AGECODE=1 then 'Under 18' when AGECODE=18 then '18-24' when AGECODE=25 then '25-34' when AGECODE=35 then '35-44' when AGECODE=45 then '45-49' when AGECODE=50 then '50-55' when AGECODE=56 then '56+' else 'None' end as AGECODED FROM "+ Userss + ")");
        conn.setAutoCommit(false);
        	iAgeDec.execute();
           conn.commit();
        iAgeDec.close();
        String sql = "SELECT COUNT(*) FROM AGEDECODE";
        PreparedStatement prest = conn.prepareStatement(sql);
        rs = prest.executeQuery();
        while (rs.next())
        	records = rs.getInt(1);
        System.out.println("Number of records loaded: " + records);
        stmt7.close();
   	   
      }
      
      catch (SQLException se) {
          System.out.println( "SQL ERROR: " + se );
          conn.close();
          }
        		
    		
       /***********************************************************************
       *  finally, display all the rows in the database...
       ***********************************************************************/
    	
    /*	ResultSet rset = stmt8.executeQuery( "SELECT AGECODE, AGECODED FROM AGEDECODE INNER JOIN USERSS ON AGEDECODE.AGECODE=USERSS.AGEDECODE)" );
        while( rset.next() )
           System.out.println( rset.getString("USERID") + ": " +
              rset.getString("GENDER") + ": " +
              rset.getString("AGECODE")+ ": " +
              rset.getString("OCCUPATION") ); 

        rset.close();  */

        conn.close();
       
      } // end procedure
      
   } // end class

