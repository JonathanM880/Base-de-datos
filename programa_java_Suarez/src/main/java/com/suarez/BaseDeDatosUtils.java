package com.suarez;

import java.sql.*;
import java.util.List;

import redis.clients.jedis.Jedis;

import java.io.FileWriter;
import java.io.IOException;

public class BaseDeDatosUtils {
    //CON ESTE METODO CREO LA TABLA EN MEMORIA
    public static void crearTablaH2EnMemoria(ResultSet resultado, String nombreTabla, Connection connectionH2) {
        try {
            ResultSetMetaData metaData = resultado.getMetaData();
            int cantidadColumnas = metaData.getColumnCount();

            StringBuilder sqlCrear = new StringBuilder("CREATE TABLE " + nombreTabla + " (");

            for (int i = 1; i <= cantidadColumnas; i++) {
                String nombreColumna = metaData.getColumnName(i);
                int tipoColumna = metaData.getColumnType(i);

                sqlCrear.append(nombreColumna).append(" ");

                switch (tipoColumna) {
                    case Types.INTEGER:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        sqlCrear.append("INT");
                        break;
                    case Types.BIGINT:
                        sqlCrear.append("BIGINT");
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        sqlCrear.append("DOUBLE");
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        sqlCrear.append("DECIMAL(15,2)");
                        break;
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP:
                        sqlCrear.append("TIMESTAMP");
                        break;
                    default:
                        sqlCrear.append("VARCHAR(255)");
                }

                if (i < cantidadColumnas) {
                    sqlCrear.append(", ");
                }
            }
            sqlCrear.append(")");

            try (Statement stmt = connectionH2.createStatement()) {
                stmt.execute(sqlCrear.toString());
                System.out.println("Tabla '" + nombreTabla + "' creada en memoria H2.");
            }
        } catch (SQLException e) {
            System.out.println("Error al crear tabla en memoria H2: " + e.getMessage());
        }
    }
    //CON ESTE METODO CREO LA TABLA EN MYSQL
    public static void crearTablaMySQL(ResultSet resultado, String nombreTabla, Connection connectionMySQL) {
        try {
            ResultSetMetaData metaData = resultado.getMetaData();
            int cantidadColumnas = metaData.getColumnCount();
    
            StringBuilder sqlCrear = new StringBuilder("CREATE TABLE IF NOT EXISTS " + nombreTabla + " (");
    
            for (int i = 1; i <= cantidadColumnas; i++) {
                String nombreColumna = metaData.getColumnName(i);
                int tipoColumna = metaData.getColumnType(i);
    
                sqlCrear.append(nombreColumna).append(" ");
    
                switch (tipoColumna) {
                    case Types.INTEGER:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                        sqlCrear.append("INT");
                        break;
                    case Types.BIGINT:
                        sqlCrear.append("BIGINT");
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        sqlCrear.append("DOUBLE");
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        sqlCrear.append("DECIMAL(15,2)");
                        break;
                    case Types.DATE:
                        sqlCrear.append("DATE");
                        break;
                    case Types.TIME:
                        sqlCrear.append("TIME");
                        break;
                    case Types.TIMESTAMP:
                        sqlCrear.append("TIMESTAMP");
                        break;
                    default:
                        sqlCrear.append("VARCHAR(255)");
                }
    
                if (i < cantidadColumnas) {
                    sqlCrear.append(", ");
                }
            }
            sqlCrear.append(")");
    
            try (Statement stmt = connectionMySQL.createStatement()) {
                stmt.execute(sqlCrear.toString());
                System.out.println("Tabla '" + nombreTabla + "' creada en MySQL.");
            }
        } catch (SQLException e) {
            System.out.println("Error al crear tabla en MySQL: " + e.getMessage());
        }
    }    

    public static void insertarDatosEnH2(ResultSet resultado, String nombreTabla, int batchSize, Connection connectionH2) {
        try {
            ResultSetMetaData metaData = resultado.getMetaData();
            int cantidadColumnas = metaData.getColumnCount();
            StringBuilder sqlInsertar = new StringBuilder("INSERT INTO " + nombreTabla + " (");

            for (int i = 1; i <= cantidadColumnas; i++) {
                sqlInsertar.append(metaData.getColumnName(i));
                if (i < cantidadColumnas) {
                    sqlInsertar.append(", ");
                }
            }
            sqlInsertar.append(") VALUES (");
            for (int i = 1; i <= cantidadColumnas; i++) {
                sqlInsertar.append("?");
                if (i < cantidadColumnas) {
                    sqlInsertar.append(", ");
                }
            }
            sqlInsertar.append(")");

            connectionH2.setAutoCommit(false);
            PreparedStatement pstmt = connectionH2.prepareStatement(sqlInsertar.toString());

            resultado.beforeFirst(); 
            int contador = 0;

            System.out.println("Usando tamaño de batch: " + batchSize);

            long inicioInsert = System.currentTimeMillis();

            while (resultado.next()) {
                for (int i = 1; i <= cantidadColumnas; i++) {
                    pstmt.setObject(i, resultado.getObject(i));
                }
                pstmt.addBatch();
                contador++;

                if (contador % batchSize == 0) {
                    pstmt.executeBatch();
                    connectionH2.commit();
                }
            }

            pstmt.executeBatch();
            connectionH2.commit();

            long finInsert = System.currentTimeMillis();
            System.out.println("Tiempo total de inserción en H2: " + (finInsert - inicioInsert) + " ms");

        } catch (SQLException e) {
            System.out.println("Error al insertar en H2: " + e.getMessage());
        }
    }

    public static void guardarDatosEnCSV(ResultSet resultado, String rutaArchivo) {
        try (FileWriter csvWriter = new FileWriter(rutaArchivo)) {
            ResultSetMetaData metaData = resultado.getMetaData();
            int colCount = metaData.getColumnCount();

            for (int i = 1; i <= colCount; i++) {
                csvWriter.append(metaData.getColumnName(i));
                if (i < colCount) {
                    csvWriter.append(",");
                }
            }
            csvWriter.append("\n");

            resultado.beforeFirst(); 

            while (resultado.next()) {
                for (int i = 1; i <= colCount; i++) {
                    Object dato = resultado.getObject(i);
                    if (dato != null) {
                        String valor = dato.toString().replace(",", " "); 
                        csvWriter.append(valor);
                    } else {
                        csvWriter.append(""); 
                    }

                    if (i < colCount) {
                        csvWriter.append(",");
                    }
                }
                csvWriter.append("\n");
            }

            System.out.println("Datos guardados en el CSV: " + rutaArchivo);
        } catch (SQLException | IOException e) {
            System.out.println("Error al guardar en CSV: " + e.getMessage());
        }
    }

    public static void imprimirUltimos15Registros(String nombreTabla, Connection connectionH2) {
        String sql = "SELECT * FROM " + nombreTabla + " ORDER BY 1 DESC LIMIT 15";
        try (Statement stmt = connectionH2.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int colCount = metaData.getColumnCount();

            System.out.println("\nÚltimos 15 registros de la tabla " + nombreTabla + ":");

            while (rs.next()) {
                for (int i = 1; i <= colCount; i++) {
                    System.out.print(metaData.getColumnName(i) + ": " + rs.getObject(i) + " | ");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            System.out.println("Error al imprimir últimos registros: " + e.getMessage());
        }
    }

   public static void consultarRedisYGuardarEnMySQL(String sql, String nombreTabla, Connection connectionMySQL) {
    // Aquí simulamos que tenemos los mismos datos que en la consulta SQL
    Jedis jedis = new Jedis("localhost"); // Suponiendo que Redis está corriendo en localhost
    try {
        // Simular que Redis tiene los datos de los empleados (esto dependerá de cómo estén almacenados)
        // Aquí simulamos la consulta en Redis, obteniendo la información como un Hash

        List<String> empleados = jedis.lrange("empleados", 0, -1); // Obtenemos todos los registros
        if (empleados != null && !empleados.isEmpty()) {
            String insertSql = "INSERT INTO " + nombreTabla + " (first_name, last_name, dept_name, salary) VALUES (?, ?, ?, ?)";
            PreparedStatement pstmt = connectionMySQL.prepareStatement(insertSql);

            for (String empleado : empleados) {
                String[] datosEmpleado = empleado.split(","); // Simulamos que los datos están separados por comas
                pstmt.setString(1, datosEmpleado[0]); // first_name
                pstmt.setString(2, datosEmpleado[1]); // last_name
                pstmt.setString(3, datosEmpleado[2]); // dept_name
                pstmt.setDouble(4, Double.parseDouble(datosEmpleado[3])); // salary
                pstmt.addBatch(); // Añadimos la consulta al batch
            }

            pstmt.executeBatch(); // Ejecutamos el batch
            System.out.println("Datos insertados desde Redis en MySQL.");
        }
    } catch (Exception e) {
        System.out.println("Error al consultar Redis o insertar en MySQL: " + e.getMessage());
    } finally {
        jedis.close(); // Cerramos la conexión con Redis
    }
}

}




