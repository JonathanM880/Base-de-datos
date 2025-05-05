package com.suarez;

import java.sql.*;

public class Conexion {

    private Connection connection;
    private Connection connectionH2;

    public void conectarMySQL() {
        String url = "jdbc:mysql://localhost:3340/employees";
        String usuario = "redis";
        String contrasena = "redis";

        try {
            connection = DriverManager.getConnection(url, usuario, contrasena);
            System.out.println("Conexión exitosa a la base de datos MySQL.");
        } catch (SQLException e) {
            System.out.println("Error al conectar a MySQL: " + e.getMessage());
        }
    }

    public void conectarH2() {
        String urlH2 = "jdbc:h2:mem:testdb";
        try {
            connectionH2 = DriverManager.getConnection(urlH2, "sa", "");
            System.out.println("Conexión exitosa a la base de datos H2 en memoria.");
        } catch (SQLException e) {
            System.out.println("Error al conectar a H2: " + e.getMessage());
        }
    }

    public void desconectar() {
        try {
            if (connection != null) {
                connection.close();
                System.out.println("Conexión MySQL cerrada exitosamente.");
            }
            if (connectionH2 != null) {
                connectionH2.close();
                System.out.println("Conexión H2 cerrada exitosamente.");
            }
        } catch (SQLException e) {
            System.out.println("Error al cerrar las conexiones: " + e.getMessage());
        }
    }

    public ResultSet ejecutarConsulta(String sql) {
        ResultSet resultado = null;
        if (connection != null) {
            try {
                Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                resultado = statement.executeQuery(sql);
            } catch (SQLException e) {
                System.out.println("Error al ejecutar la consulta: " + e.getMessage());
            }
        } else {
            System.out.println("No hay conexión activa a la base de datos MySQL.");
        }
        return resultado;
    }

    public Connection getConnection() {
        return connection;
    }

    public Connection getConnectionH2() {
        return connectionH2;
    }
}

