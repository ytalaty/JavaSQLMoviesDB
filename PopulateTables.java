package dbexample;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class PopulateTables {

	public static void main(String[] args) throws FileNotFoundException, SQLException {
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
		 Scanner read = new Scanner (new File("/Users/Yusuf/Desktop/DePaul/CSC 352 Database Systems/ml-1m/movies.dat"));
	  		read.useDelimiter("::");
	  		
	  		String movieid, movietitle, category;
	  		int c=0;
	  		
	  		while(read.hasNext()) {
	  			movieid=read.next();
	  			movietitle=read.next();
	  			category=read.next();
	  			//PreparedStatement updateMovies =
	  			//		 conn.prepareStatement( "INSERT INTO Movies_Temp(MovieID,MovieTitle, Category) " + "VALUES ( ?, ?, ?)" );
	  			//conn.setAutoCommit(false);
	  			//updateMovies.setString( 1, movieid );
	            //updateMovies.setString( 2, movietitle );
	            //updateMovies.setString(3, category);
	            //updateMovies.executeUpdate();
	            //conn.commit();
	  			String insertString = 
	  					"INSERT INTO Movies_Temp VALUES('" + movieid + "','" + movietitle + "','" + category + "')";
	  			stmt.executeUpdate(insertString);
	  			//System.out.println(movieid + " " +  movietitle + " " +  category);
	  		}
	  		stmt.close();
	  		conn.close();
	  		read.close();
	     }
	  		catch (SQLException se) {
	  			stmt.close();
	  			conn.close();
		          System.out.println( "SQL ERROR: " + se );
		          }
	}

}
