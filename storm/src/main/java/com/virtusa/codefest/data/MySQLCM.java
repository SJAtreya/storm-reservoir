package com.virtusa.codefest.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLCM {

	// Notice, do not import com.mysql.jdbc.*
	// or you will have problems!

	public static Connection getConnection() throws SQLException {
		Connection conn = null;
		try {
			// The newInstance() call is a work around for some
			// broken Java implementations

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager
					.getConnection("jdbc:mysql://localhost/reservoir?"
							+ "user=root&password=root");
			cleanUp(conn);
		} catch (Exception ex) {
			// handle the error
			System.out.println("SQLException: " + ex.getMessage());
		}
		return conn;
	}
	
	public static void returnConnection(Connection conn) {
		try {
			conn.rollback();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void cleanUp(Connection conn) {
		// TODO Auto-generated method stub
		try {
			conn.setAutoCommit(false);
			conn.rollback();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
