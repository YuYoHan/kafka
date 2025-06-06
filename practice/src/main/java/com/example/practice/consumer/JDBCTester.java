package com.example.practice.consumer;

import java.sql.*;

public class JDBCTester {
    public static void main(String[] args) {
        Connection c = null;
        Statement stmt = null;
        ResultSet rs = null;

        String url = "jdbc:postgresql://192.168.56.101:5432/postgres";
        String user = "postgres";
        String password = "1234";

        try {
            c = DriverManager.getConnection(url, user, password);
            stmt = c.createStatement();
            rs = stmt.executeQuery("select 'postgresql is connected' ");

            if(rs.next()) System.out.println(rs.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                stmt.close();
                c.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
