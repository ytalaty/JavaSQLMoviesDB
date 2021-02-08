package dbexample;

import java.sql.*;
import java.util.Scanner;
import java.io.*;

public class hw6 {
	
	public static void test() throws FileNotFoundException {
		
		Scanner read = new Scanner (new File("/Users/Yusuf/Desktop/DePaul/CSC 352 Database Systems/ml-1m/movies.dat"));
		read.useDelimiter("::");
		   
		   String movieid, movietitle, category;

		   while (read.hasNext()){
		       movieid = read.next();
		       movietitle = read.next();
		       category = read.next();
		       System.out.println(movieid + "--" + movietitle + "--" + category); //just for debugging
		   }
		   read.close();
		
	}

  	public static void main ( String args []) throws SQLException {
	   
  	 /*try {
		test();
	} catch (FileNotFoundException e1) {
		e1.printStackTrace();
	} */
  		
	  // Temporary Tables 
      String Movies_Temp = "MOVIES_TEMP";
      String Users_Temp = "USERS_TEMP";
      String Ratings_Temp = "RATINGS_TEMP";
      
      
      //Connections and Statements Tracker
      Connection conn = null; 
      
      Statement stmt = null; //Movies_Temp
      Statement stmt2 = null; //Ratings_Temp
      Statement stmt3 = null; //Users_Temp

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
         stmt = conn.createStatement();
         stmt2 = conn.createStatement();
         stmt3 = conn.createStatement();
         }
      catch (SQLException se) {
         System.out.println(se);
         System.exit(1);
         }

      
      /***********************************************************************
       *  Create Temporary Movies_Temp Table
       ***********************************************************************/
      try {
          String movies_drop = "DROP TABLE " + Movies_Temp;
          stmt.executeUpdate(movies_drop);
          }
       catch (SQLException se) {/*do nothing*/} // table doesn't exist


       try {
    	   
    	   System.out.print( "Creating new " + Movies_Temp + " table...\n\n" );
           String moviesTempTable =
        		   "CREATE TABLE " + Movies_Temp +
                   "  (MOVIEID NUMBER(4,0) PRIMARY KEY," + "    MOVIETITLEYEAR VARCHAR2(100)," + "   CATEGORY CHAR(60) )";
         stmt.executeUpdate(moviesTempTable);
           
         BufferedReader in = new BufferedReader(new FileReader("/Users/Yusuf/Desktop/DePaul/CSC 352 Database Systems/ml-1m/movies.dat"));
         
         String str;
         
         while ((str = in.readLine()) != null ) {
        	 //c++;
        	 String[] ar = str.split("::");
        	 PreparedStatement uMT = conn.prepareStatement( "INSERT INTO " + Movies_Temp + " VALUES ( ?, ?, ? )" );
        	 conn.setAutoCommit(false);
             uMT.setInt(1, Integer.parseInt(ar[0]));
        	 uMT.setString(2, ar[1]);
       		 uMT.setString(3, ar[2]);
       		 uMT.executeUpdate();
           	 conn.commit();
         }
         in.close();
         stmt.close();
    	   
       }
       
       catch (SQLException |  IOException se) {
           System.out.println( "SQL ERROR: " + se );
           conn.close();
           }
      
      
      /***********************************************************************
       *  Create Temporary Ratings_Temp Table
       ***********************************************************************/
    	try {
              String users_drop = "DROP TABLE " + Users_Temp;
   	          stmt2.executeUpdate(users_drop);
   	          }
   	    catch (SQLException se) {/*do nothing*/} // table doesn't exist
 
    	try {
    		
    		System.out.print( "Creating new " + Users_Temp + " table...\n\n" );
            String usersTempTable =
               "CREATE TABLE " + Users_Temp +
               "  (USERID NUMBER(4,0)," + "    GENDER CHAR(1 BYTE)," + "    AGECODE NUMBER(2,0)," 
            		   + "   OCCUPATION NUMBER(2,0)," + "   ZIPCODE VARCHAR2(20 BYTE) )";
            stmt2.executeUpdate(usersTempTable);
            
            stmt2.close();
    		
    	}
    	
    	catch (SQLException se) {
            System.out.println( "SQL ERROR: " + se );
            stmt2.close();}
    	   
    	   
      /***********************************************************************
       *  Create Temporary Users_Temp Table
       ***********************************************************************/
    	try {
            String ratings_drop = "DROP TABLE " + Ratings_Temp;
            stmt3.executeUpdate(ratings_drop);
     	          }
     	catch (SQLException se) {/*do nothing*/} // table doesn't exist
   
      	try {
      		
      		System.out.print( "Creating new " + Ratings_Temp + " table...\n\n" );
            String ratingsTempTable =
               "CREATE TABLE " + Ratings_Temp +
               "  (USERID NUMBER(4,0)," + "    MOVIEID NUMBER," + "    RATING NUMBER," 
            		   + "   TIMESTAMP NUMBER(20,0) )";
            stmt3.executeUpdate(ratingsTempTable);
            
            stmt3.close();	
      	}
      	
      	catch (SQLException se) {
            System.out.println( "SQL ERROR: " + se );
            stmt3.close();
            conn.close();
            }
    		
    		
       /***********************************************************************
       *  finally, display all the rows in the database...
       ***********************************************************************/

        conn.close();
       
      } // end procedure
      
   } // end class

