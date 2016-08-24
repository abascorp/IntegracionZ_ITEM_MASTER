package org.inverpaca.integracion;

import java.io.IOException;
import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.dbcp.BasicDataSource;
import org.inverpaca.integracion.Integracion;


//import javax.naming.Context;
//import javax.naming.InitialContext;
//import javax.sql.DataSource;

//import javax.naming.Context;
//import javax.naming.InitialContext;
//import javax.sql.DataSource;


public class Integracion extends Conecciones {
  //Constructor
	public Integracion(){
		
	}
	//Variables seran utilizadas para capturar mensajes de errores de Oracle
	private String msj = null;
	//Variables para select
	private int columns;
	private String[][] arr;
	private int rows;
	private String[][] tabla;
	int cuenta = 0;
	int i;
	java.util.Date fecact = new java.util.Date();
	java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("dd/MM/yyyy hh:mm:ss aaa");
    String fecha = sdf.format(fecact);
    
    java.util.Date fechadiaD = new java.util.Date();
	java.text.SimpleDateFormat sdf1 = new java.text.SimpleDateFormat("dd/MM/yyyy");
    String fechadia = sdf1.format(fecact);
    
   

	
	 /**
     * Inserta logs.
     **/
    void insertLog(String detfaz, String estatus, String area) {
        try {
        	//Context initContext = new InitialContext();
            //DataSource ds = (DataSource) initContext.lookup("jdbc/orabiz");
        	
        	BasicDataSource ds = new BasicDataSource();
	   		ds.setDriverClassName(getBi_driver());
	   		ds.setUrl(getBi_url());
	   		ds.setUsername(getBi_usuario());
	   		ds.setPassword(getBi_clave());
	   		ds.setMaxActive(-1);
		  	ds.setMaxActive(-1);
	   		
	   		
            Connection con = ds.getConnection();
            PreparedStatement pstmt = null;
            //Class.forName(getDriver());
            //con = DriverManager.getConnection(
              //    getUrl(), getUsuario(), getClave());
            String query = "INSERT INTO BIAUDIT VALUES ('" + fecha + "','" + detfaz + "','" + estatus + "','" + area + "',TO_DATE('" + fechadia + "','dd/MM/yyyy'),'1')";
            pstmt = con.prepareStatement(query);
            //System.out.println(query);
            try {
                //Avisando
                pstmt.executeUpdate();
                System.out.println("Se ha insertado registro en el log");
            } catch (SQLException e)  {
                msj =   e.getMessage();
            }
            
            pstmt.close();
            con.close();
        } catch (Exception e) {
        }
    }
    				
	/**
     * Lee datos de MAESTRO DE SAP
     **/
	private void  selectZ_ITEM_MASTER() {
		//System.out.println("entre al select");
       try {
           Statement stmt;
           ResultSet rs = null;
           //Context initContext = new InitialContext();
           //DataSource ds = (DataSource) initContext.lookup("jdbc/TUBDER03");
           BasicDataSource ds = new BasicDataSource();
	  	   ds.setDriverClassName(getSybase_driver());
	  	   ds.setUrl(getSybase_url());
	  	   ds.setUsername(getSybase_usuario());
	  	   ds.setPassword(getSybase_clave());
	  	   ds.setMaxActive(-1);
	  	   ds.setMaxActive(-1);
	  	   
  		   		 
           Connection con = ds.getConnection();
           stmt = con.createStatement(
              		ResultSet.TYPE_SCROLL_INSENSITIVE,
                    	ResultSet.CONCUR_READ_ONLY);
					   String query  = "SELECT ";
					   query += "MARA.MANDT, "; 
					   query += "MARA.MATNR, "; 
					   query += "MAKT.MAKTX, "; 
					   query += "MAKT.MAKTG, "; 
					   query += "MARA.MATKL, "; 
					   query += "T023T.WGBEZ, "; 
					   query += "T023T.WGBEZ60, "; 
					   query += "MARA.BRGEW, "; 
					   query += "MARA.NTGEW, "; 
					   query += "MARA.GEWEI, "; 
					   query += "MARA.MEINS, "; 
					   query += "MARA.SPART, "; 
					   query += "TSPAT.VTEXT ";
					   query += "FROM ";
					   query += "R3P.SAPSR3.MARA MARA LEFT OUTER JOIN R3P.SAPSR3.MAKT MAKT ON MARA.MANDT = MAKT.MANDT AND ";
					   query += "                                                             MARA.MATNR = MAKT.MATNR ";
					   query += "                     LEFT OUTER JOIN R3P.SAPSR3.T023T T023T ON MARA.MANDT = T023T.MANDT AND ";
					   query += "                                                               MARA.MATKL = T023T.MATKL AND ";
					   query += "                                                               T023T.SPRAS = 'S' ";
					   query += "                     LEFT OUTER JOIN R3P.SAPSR3.TSPAT TSPAT ON MARA.MANDT = TSPAT.MANDT AND "; 
					   query += "                                                               MARA.SPART = TSPAT.SPART AND ";
					   query += "                                                               TSPAT.SPRAS = 'S' ";
					   query += "WHERE "; 
					   query += "MARA.MANDT = '400' ";
					   query += "AND MARA.MATNR BETWEEN '000000000010000000' AND '000000000026999999' ";
					   query += "ORDER BY ";
					   query += "MARA.MANDT,  ";
					   query += "MARA.MATNR ";


					   //System.out.println(query);
           
					   try{
           rs = stmt.executeQuery(query);
           rows = 1;
		    rs.last();
		    rows = rs.getRow();
           System.out.println("Cantidad de Registros:" + rows);

           ResultSetMetaData rsmd = rs.getMetaData();
       	   columns = rsmd.getColumnCount();
		   System.out.println("Cantidad de Columnas:" +columns);
       	   arr = new String[rows][columns];
       	   
           int i = 0;
		    rs.beforeFirst();
           while (rs.next()){
               for (int j = 0; j < columns; j++)
				arr [i][j] = rs.getString(j+1);
                i++;
              /* System.out.println(arr[i][0] +","
                                 +arr[i][1] +","
            		             +arr[i][2] +","
                                 +arr[i][3] +","
            		             +arr[i][4] +","
                                 +arr[i][5] +","
            		             +arr[i][6] +","
                                 +arr[i][7] +","
            		             +arr[i][8] +","
                                 +arr[i][9] +","
            		             +arr[i][10]+","
                                 +arr[i][11]+","
            		             +arr[i][12]+"."); 
            		*/             
               
              }
                   } catch (SQLException e) {
                   e.printStackTrace();
                   insertLog("Z_ITEM_MASTER: " + e.getMessage(),"error", "Z_ITEM_MASTER");
               }
           stmt.close();
           con.close();
           rs.close();

       } catch (Exception e) {
           e.printStackTrace();
           msj  = e.getMessage();
           insertLog("Z_ITEM_MASTER: " + e.getMessage(),"error", "Z_ITEM_MASTER");
       }
   }
	
	/**
     * Inserta registros utiliza multithread 100 hilos
     **/
	public void insertDatos()  {
		//Borra registros
		System.out.println("Entre al metodo insertDatos");
		
        BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(getBi_driver());
		ds.setUrl(getBi_url());
		ds.setUsername(getBi_usuario());
		ds.setPassword(getBi_clave());
		ds.setMaxActive(-1);
		ds.setMaxActive(-1);
        ExecutorService ex = Executors.newFixedThreadPool(10);   //Numero de hilos a usar para el insert
        
	    try {
	    	
	    Integracion q = new Integracion();
		
	    String
	    qryDelete = "DELETE Z_ITEM_MASTER";
	    
	    QueryGenericThread thd;
        thd = new QueryGenericThread(qryDelete, ds); //Insert Generic
        ex.execute(thd);
        Thread.sleep(1);
        System.out.println("DELETE EJECUTADO.");
	    
	    q.selectZ_ITEM_MASTER();
        tabla = q.getArr();
        rows = q.getRows();
	    
        //Inicia el Ciclo For
	    for (int i = 0; i < rows; i++) {
	    String 
    	qryInsert  = "INSERT INTO Z_ITEM_MASTER values (" 
				+ "'" + tabla[i][0] + "', "
				+ "'" + tabla[i][1] + "', "
				+ "'" + tabla[i][2].replace("'", "") + "', "
				+ "'" + tabla[i][3].replace("'", "") + "', "
				+ "'" + tabla[i][4].replace("'", "") + "', " 
				+ "'" + tabla[i][5].replace("'", "") + "', " 
				+ "'" + tabla[i][6].replace("'", "") + "', " 
				+ tabla[i][7] + ", "
				+ tabla[i][8] + ", "
				+ "'" + tabla[i][9].replace("'", "") + "', " 
				+ "'" + tabla[i][10].replace("'", "") + "', "
				+ "'" + tabla[i][11].replace("'", "") + "', "
				+ "'" + tabla[i][12].replace("'", "") + "')";
				

        
    	 //System.out.println(qryInsert);    			     		
	             
       	 QueryGenericThread th;
         th = new QueryGenericThread(qryInsert, ds); //Insert Generic
         ex.execute(th);
         Thread.sleep(1);
         
	    } // Fin del loop for
     
	    if(rows==0){
        msj = "Z_ITEM_MASTER" + " : Lectura de datos no realizada, no se encontraron registros";
        insertLog(msj, "error", "Z_ITEM_MASTER");
        } else {
        msj = "Z_ITEM_MASTER" + " : Registros insertados con exito. " + rows + " registros";
        insertLog(msj, "exito", "Z_ITEM_MASTER");
        }
        
	        ds.close();
	} catch (Exception e) {
	        insertLog(e.getMessage(),"error", "Z_ITEM_MASTER");
	    }
	    
	}
  /**
	 * @return the arr
	 */
	public String[][] getArr() {
		return arr;
	}


	/**
	 * @return the rows
	 */
	public int getRows() {
		return rows;
	}

	

     /**
	 * @return the msj
	 */
	public String getMsj() {
		return msj;
	}
	

	//Ejecuta el programa
	public static void main (String args []) throws IOException{
	//System.out.println("aqui inicio");
	Integracion a = new Integracion();
	//Ejecuta TUBDER03
	
	System.out.println("Iniciando Interfaz");
	a.insertDatos();
	System.out.println("FIN Z_ITEM_MASTER");
	
     {
        System.exit(0);
    }
	            
	}
	
}