package com.x5dbTest.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgreSQLConnUtils {

    public static Connection getMySQLConnection() {

        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/Test", "postgres", "12345");

            if (conn != null) {
                System.out.println("Connected to the database!");
            } else {
                System.out.println("Failed to make connection!");
            }

        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        }
        return conn;
    }
}
