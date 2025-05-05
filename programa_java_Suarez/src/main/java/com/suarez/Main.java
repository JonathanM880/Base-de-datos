package com.suarez;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
// jjsuarezz -- Jonathan Suarez
public class Main {

    public static void main(String[] args) {
        Conexion conexion = new Conexion();
        conexion.conectarMySQL();
        conexion.conectarH2();

        String sql = "SELECT e.first_name, e.last_name, d.dept_name, s.salary " +
                     "FROM employees e " +
                     "JOIN dept_emp de ON e.emp_no = de.emp_no " +
                     "JOIN departments d ON de.dept_no = d.dept_no " +
                     "JOIN salaries s ON e.emp_no = s.emp_no";

        ResultSet resultado = null;

        try {
            long inicioConsulta = System.currentTimeMillis();
            resultado = conexion.ejecutarConsulta(sql);
            long finConsulta = System.currentTimeMillis();

            System.out.println("Tiempo de ejecución de la consulta: " + (finConsulta - inicioConsulta) + " ms");

            if (resultado != null) {
                String nombreTabla = "tabla_dinamica";

                // Crear la tabla
                BaseDeDatosUtils.crearTablaH2EnMemoria(resultado, nombreTabla, conexion.getConnectionH2());

                // Insertar los datos en H2 (MEMORIA) con batch

                // Para máximo 1000 registros
                //BaseDeDatosUtils.insertarDatosEnH2(resultado, nombreTabla, 1000, conexion.getConnectionH2());
                
                // Para máximo 10000 registros
                //BaseDeDatosUtils.insertarDatosEnH2(resultado, nombreTabla, 10000, conexion.getConnectionH2());
                
                // Para máximo 500000 registros
                //BaseDeDatosUtils.insertarDatosEnH2(resultado, nombreTabla, 500000, conexion.getConnectionH2());
                
                // Para máximo 1000000 registros
                BaseDeDatosUtils.insertarDatosEnH2(resultado, nombreTabla, 1000000, conexion.getConnectionH2());

                // Crear tabla en MySQL
                //BaseDeDatosUtils.crearTablaMySQL(resultado, "tabla_mysql", conexion.getConnection());

                // Insertar los datos en MySQL(BASE DE DATOS) con batch

                // Para máximo 1000 registros
                //BaseDeDatosUtils.insertarDatosEnH2(resultado, "tabla_mysql", 1000, conexion.getConnection());

                // Para máximo 10000 registros
                //BaseDeDatosUtils.insertarDatosEnH2(resultado, "tabla_mysql", 10000, conexion.getConnection());

                // Para máximo 500000 registros
                //BaseDeDatosUtils.insertarDatosEnH2(resultado, "tabla_mysql", 500000, conexion.getConnection());

                // Para máximo 1000000 registros
                //BaseDeDatosUtils.insertarDatosEnH2(resultado, "tabla_mysql", 1000000, conexion.getConnection());

                // Crear nombre dinámico de archivo
                String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
                String rutaArchivo = "C:/Users/user/Downloads/guardarBaseDatos/tabla_generada_" + timestamp + ".csv";

                // Guardar en CSV
                BaseDeDatosUtils.guardarDatosEnCSV(resultado, rutaArchivo);

                // Imprimir últimos 15 registros
                //BaseDeDatosUtils.imprimirUltimos15Registros(nombreTabla, conexion.getConnectionH2());

                //REDIS
                BaseDeDatosUtils.consultarRedisYGuardarEnMySQL(sql, nombreTabla, conexion.getConnection());
            }
        } catch (Exception e) {
            System.out.println("Error en main: " + e.getMessage());
        } finally {
            conexion.desconectar();
        }
    }
}

