package dbexample;

import java.sql.*;
import java.io.*;

public class OccDecode {
	
	

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
          String occupD_drop = "DROP TABLE " + OccupationDecode;
          stmt8.executeUpdate(occupD_drop);
   	          }
   	catch (SQLException se) {/*do nothing*/} // table doesn't exist
    	
    	try {
   	   
    		System.out.print( "Creating new " + OccupationDecode + " table...\n" );
            String occupDecodeTable =
               "CREATE TABLE " + OccupationDecode +
               " (OCCUPATION NUMBER(2), OCCUPDEC VARCHAR2(25))";
            stmt8.executeUpdate(occupDecodeTable);
            
          System.out.println("OccupationDecode Table has been created succesfully.");
          
         
          PreparedStatement iOccDec = conn.prepareStatement("INSERT INTO " + OccupationDecode + "(SELECT Occupation, " + 
                " case " + 
          		"                when Occupation=0 then 'other' " +
        		"                when Occupation=1 then 'academic/educator' " + 
          		"                when Occupation=2 then 'artist' " + 
          		"                when Occupation=3 then 'clerical/admin' " + 
          		"                when Occupation=4 then 'college/grad student' " + 
          		"                when Occupation=5 then 'customer service' " + 
          		"                when Occupation=6 then 'doctor/health care' " + 
          		"                when Occupation=7 then 'executive/managerial' " + 
          		"                when Occupation=8 then 'farmer' " + 
          		"                when Occupation=9 then 'homemaker' " + 
          		"                when Occupation=10 then 'K-12 student' " + 
          		"                when Occupation=11 then 'lawyer' " + 
          		"                when Occupation=12 then 'programmer' " + 
          		"                when Occupation=13 then 'retired' " + 
          		"                when Occupation=14 then 'sales/marketing' " + 
          		"                when Occupation=15 then 'scientist' " + 
          		"                when Occupation=16 then 'self-employed' " + 
          		"                when Occupation=17 then 'technician/engineer' " + 
          		"                when Occupation=18 then 'tradesman/craftsman' " + 
          		"                when Occupation=19 then 'unemployed' " + 
          		"                when Occupation=20 then 'writer' " + 
          		"                else 'none' " + 
          		"                end as RealOcc " + 
          		"            FROM "+ Userss + ")");
        
          conn.setAutoCommit(false);
           iOccDec.execute();
           conn.commit();
        
        iOccDec.close();
        System.out.println("Occupation Decode Table Populated. \n");
        String sql = "SELECT COUNT(*) FROM OCCUPATIONDECODE";
        PreparedStatement prest = conn.prepareStatement(sql);
        rs = prest.executeQuery();
        while (rs.next())
        	records = rs.getInt(1);
        System.out.println("Number of records loaded: " + records);
        stmt8.close();
   	   
      }
      
      catch (SQLException se) {
          System.out.println( "SQL ERROR: " + se );
          conn.close();
          }
        		
    		
    

        conn.close();
       
      } // end procedure
      
   } // end class

