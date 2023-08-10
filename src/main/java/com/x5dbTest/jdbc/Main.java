package com.x5dbTest.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {
    public static void main(String[] args) {
        Main m = new Main();
        try {

            Connection con = PostgreSQLConnUtils.getMySQLConnection();
            try {
                // Шаг первый - выставляем свойство AutoCommit в false
                con.setAutoCommit(false);
                // В цикле вставлем несколько записей

                //long clientId = m.insert(con, "Ivanov", "Semen", "Ivanovich", 1000);
                //long clientId = m.insert(con, "Petrova", "Anastasia", "Sergeevna", 2020);
                //long clientId = m.insert(con, "Orlov", "Alexandr", "Semenovich", 3900);
                //long clientId = m.insert(con, "Ivanova", "Svetlana", "Alexandrovna", 4000);
                long clientId = m.insert(con, "Kozlova", "Elena", "Semenovna", 5000);

                System.out.println("ClientID:" + clientId);
                m.update(con, 6000, 6);
                m.delete(con, 7);
                // Завершаем транзакцию - подтверждаем
                con.commit();
                // Вызов rollback отменит все внесенные изменения
                //con.rollback();

                // Возвращаем свойство AutoCommit в true
                con.setAutoCommit(true);

                // Можно проверить результат
                m.select(con);
            } finally {
                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private long insert(Connection con, String lastName, String firstName, String middleName, Integer balance) throws SQLException {
        long clientId = -1;
        PreparedStatement stmt = con.prepareStatement("INSERT INTO Client (last_name, first_name, middle_name, balance) VALUES (?, ?, ?, ?)");
        stmt.setString(1, lastName);
        stmt.setString(2, firstName);
        stmt.setString(3, middleName);
        stmt.setInt(4, balance);
        stmt.executeUpdate();

        ResultSet gk = stmt.getGeneratedKeys();
        while (gk.next()) {
            clientId = gk.getLong("id");
        }
        stmt.close();

        return clientId;
    }

    private void update(Connection con, Integer balance, Integer id) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("update client set balance = ? where id = ?");
        stmt.setInt(1, balance);
        stmt.setInt(2, id);
        stmt.executeUpdate();
        stmt.close();
    }
    private void delete(Connection con, Integer id) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("delete from client where id = ?");
        stmt.setInt(1, id);
        stmt.executeUpdate();
        stmt.close();
    }
    private void select(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM client");
        while (rs.next()) {
            String str = rs.getString("id") + ";" + rs.getString(2) + ";" + rs.getString(3) + ";" + rs.getString(5);
            System.out.println("Client:" + str);
        }
        rs.close();
        stmt.close();
    }
}

