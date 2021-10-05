package edu.pitt.dbmi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// Make sure to use version 4.2, as SQLServerBulkCSVFileRecord
//is not included in version 4.1
import com.microsoft.sqlserver.jdbc.*;

public class ACTImportOntologyTest {


    // Connect to your database.
    // Replace server name, username, and password with your credentials
    public static void main(String[] args) {
	int batchSize = 200000;
	//String password = "<YourStrong@Passw0rd>";
	//String username = "SA";
	//String serverName = "localhost";
	//String database = "i2b2metadata";
	//String port = "1433";

	String username = args[0];
	String password = args[1];
	String serverName = args[2];
	String database = args[3];
	String port = args[4];

	//System.out.println("Args: " + args[0] + " " + args[2]);

	String[] ontologyFileName = {"ACT_MED_ALPHA_V4.tsv",
				     "ACT_MED_VA_V4.tsv",
				     "ACT_COVID_V4.tsv",
				     "ACT_CPT4_PX_V4.tsv",
				     "ACT_DEM_V4.tsv",
				     "ACT_HCPCS_PX_V4.tsv",
				     "ACT_ICD10CM_DX_V4.tsv",
				     "ACT_ICD10PCS_PX_V4.tsv",
				     "ACT_ICD10_ICD9_DX_V4.tsv",
				     "ACT_ICD9CM_DX_V4.tsv",
				     "ACT_ICD9CM_PX_V4.tsv",
				     "ACT_LOINC_LAB_PROV_V4.tsv",
				     "ACT_LOINC_LAB_V4.tsv",
				     "ACT_SDOH_V4.tsv",
				     "ACT_VISIT_DETAILS_V4.tsv",
				     "ACT_VITAL_SIGNS_V4.tsv"};
	

	String[] ontologyTable = {"ACT_MED_ALPHA_V4",
				  "ACT_MED_VA_V4",
				  "ACT_COVID_V4",
				  "ACT_CPT4_PX_V4",
				  "ACT_DEM_V4",
				  "ACT_HCPCS_PX_V4",
				  "ACT_ICD10CM_DX_V4",
				  "ACT_ICD10PCS_PX_V4",
				  "ACT_ICD10_ICD9_DX_V4",
				  "ACT_ICD9CM_DX_V4",
				  "ACT_ICD9CM_PX_V4",
				  "ACT_LOINC_LAB_PROV_V4",
				  "ACT_LOINC_LAB_V4",
				  "ACT_SDOH_V4",
				  "ACT_VISIT_DETAILS_V4",
				  "ACT_VITAL_SIGNS_V4"};

	String [] tableAccessColumnName = {
	    "C_TABLE_CD",
	    "C_TABLE_NAME",
	    "C_PROTECTED_ACCESS",
	    "C_HLEVEL",
	    "C_FULLNAME",
	    "C_NAME",
	    "C_SYNONYM_CD",
	    "C_VISUALATTRIBUTES",
	    "C_TOTALNUM",
	    "C_BASECODE",
	    "C_METADATAXML",
	    "C_FACTTABLECOLUMN",
	    "C_DIMTABLENAME",
	    "C_COLUMNNAME",
	    "C_COLUMNDATATYPE",
	    "C_OPERATOR",
	    "C_DIMCODE",
	    "C_COMMENT",
	    "C_TOOLTIP",
	    "C_ENTRY_DATE",
	    "C_CHANGE_DATE",
	    "C_STATUS_CD",
	    "VALUETYPE_CD",
	    "C_ONTOLOGY_PROTECTION"
	};

	String [] schemesColumnName = {
	    "C_KEY",
	    "C_NAME",
	    "C_DESCRIPTION"
	};

	String [] ontologyColumnName = {
	    "C_HLEVEL",
	    "C_FULLNAME",
	    "C_NAME",
	    "C_SYNONYM_CD",
	    "C_VISUALATTRIBUTES",
	    "C_TOTALNUM",
	    "C_BASECODE",
	    "C_METADATAXML",
	    "C_FACTTABLECOLUMN",
	    "C_TABLENAME",
	    "C_COLUMNNAME",
	    "C_COLUMNDATATYPE",
	    "C_OPERATOR",
	    "C_DIMCODE",
	    "C_COMMENT",
	    "C_TOOLTIP",
	    "M_APPLIED_PATH",
	    "UPDATE_DATE",
	    "DOWNLOAD_DATE",
	    "IMPORT_DATE",
	    "SOURCESYSTEM_CD",
	    "VALUETYPE_CD",
	    "M_EXCLUSION_CD",
	    "C_PATH",
	    "C_SYMBOL"
	};

        String connectionUrl =
                "jdbc:sqlserver://"+serverName+":"+port+";"
                + "database="+database+";"
                + "user="+username+";"
                + "password="+password+";"
                + "encrypt=true;"
                + "trustServerCertificate=true;"
                + "loginTimeout=30;"
                + "queryTimeout=0;";
	
        ResultSet resultSet = null;
	
        try (
	     Connection connection = DriverManager.getConnection(connectionUrl);
	     Statement statement = connection.createStatement();
	     )
	    {

		long startTime = System.currentTimeMillis();
		 
		SQLServerBulkCSVFileRecord fileRecord = null;  
		for ( int i = 0; i < 16; i++) {
		    // Clean up
		    String dropTable = "DROP TABLE IF EXISTS " + ontologyTable[i];
		    statement.executeUpdate(dropTable);
		    System.out.println("Drop table in given database..."
				       + ontologyTable[i]);   	  

		    // Create table TODO: separate method
		    String createTable = "CREATE TABLE " + ontologyTable[i] 
			+ "("
			+ "C_HLEVEL    INT    NOT NULL,"
			+ "C_FULLNAME  VARCHAR(700)   NOT NULL,"
			+ "C_NAME      VARCHAR(2000)  NOT NULL,"
			+ "C_SYNONYM_CD     CHAR(1) NOT NULL,"
			+ "C_VISUALATTRIBUTES    CHAR(3) NOT NULL,"
			+ "C_TOTALNUM       INT    NULL,"
			+ "C_BASECODE       VARCHAR(50)    NULL,"
			+ "C_METADATAXML    VARCHAR(MAX)    NULL,"
			+ "C_FACTTABLECOLUMN VARCHAR(50)    NOT NULL,"
			+ "C_TABLENAME      VARCHAR(50)    NOT NULL,"
			+ "C_COLUMNNAME     VARCHAR(50)    NOT NULL,"
			+ "C_COLUMNDATATYPE VARCHAR(50)    NOT NULL,"
			+ "C_OPERATOR       VARCHAR(10)    NOT NULL,"
			+ "C_DIMCODE   VARCHAR(700)   NOT NULL,"
			+ "C_COMMENT   VARCHAR(MAX)    NULL,"
			+ "C_TOOLTIP   VARCHAR(900)   NULL,"
			+ "M_APPLIED_PATH   VARCHAR(700)   NOT NULL,"
			+ "UPDATE_DATE      DATETIME    NOT NULL,"
			+ "DOWNLOAD_DATE    DATETIME    NULL,"
			+ "IMPORT_DATE      DATETIME    NULL,"
			+ "SOURCESYSTEM_CD  VARCHAR(50)    NULL,"
			+ "VALUETYPE_CD     VARCHAR(50)    NULL,"
			+ "M_EXCLUSION_CD   VARCHAR(25) NULL,"
			+ "C_PATH      VARCHAR(700)   NULL,"
			+ "C_SYMBOL    VARCHAR(50)    NULL )";
		    
		    statement.executeUpdate(createTable);
		    System.out.println("Created table in given database..."
				       + ontologyTable[i]);   	  
		    
		    // TODO: Convert Parquet format into TSV file
		    // Need to make sure the file column order matches
		    // the table column order
		    //foreach (DataColumn col in table.Columns)
		    //{
		    //bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
		    //}		
		    // Read in tab separated file
		    fileRecord = new SQLServerBulkCSVFileRecord(ontologyFileName[i],
								null, "\t", true);   
		    // Assign database data types
		    fileRecord.addColumnMetadata(1, "C_HLEVEL",
						 java.sql.Types.INTEGER,0,0);
		    fileRecord.addColumnMetadata(2, "C_FULLNAME",
						 java.sql.Types.NVARCHAR,700,0); 
		    fileRecord.addColumnMetadata(3, "C_NAME",
						 java.sql.Types.NVARCHAR,2000,0); 
		    fileRecord.addColumnMetadata(4, "C_SYNONYM_CD",
						 java.sql.Types.NVARCHAR,1,0); 
		    fileRecord.addColumnMetadata(5, "C_VISUALATTRIBUTES",
						 java.sql.Types.NVARCHAR,3,0); 
		    fileRecord.addColumnMetadata(6, "C_TOTALNUM",
						 java.sql.Types.INTEGER, 22,0);
		    fileRecord.addColumnMetadata(7, "C_BASECODE",
						 java.sql.Types.NVARCHAR,50,0);
		    fileRecord.addColumnMetadata(8, "C_METADATAXML",
						 java.sql.Types.LONGVARCHAR,0, 0);
		    fileRecord.addColumnMetadata(9, "C_FACTTABLECOLUMN",
						 java.sql.Types.NVARCHAR,50,0); 
		    fileRecord.addColumnMetadata(10, "C_TABLENAME",
						 java.sql.Types.NVARCHAR,50,0); 
		    fileRecord.addColumnMetadata(11, "C_COLUMNNAME",
						 java.sql.Types.NVARCHAR,50,0); 
		    fileRecord.addColumnMetadata(12, "C_COLUMNDATATYPE",
						 java.sql.Types.NVARCHAR,700,0);
		    fileRecord.addColumnMetadata(13, "C_OPERATOR",
						 java.sql.Types.NVARCHAR,10,0); 
		    fileRecord.addColumnMetadata(14, "C_DIMCODE",
						 java.sql.Types.NVARCHAR,700,0); 
		    fileRecord.addColumnMetadata(15, "C_COMMENT",
						 java.sql.Types.LONGVARCHAR,0, 0);
		    fileRecord.addColumnMetadata(16, "C_TOOLTIP",
						 java.sql.Types.NVARCHAR,900,0); 
		    fileRecord.addColumnMetadata(17, "M_APPLIED_PATH",
						 java.sql.Types.NVARCHAR,700,0); 
		    fileRecord.addColumnMetadata(18, "UPDATE_DATE",
						 java.sql.Types.DATE, 0,0);
		    fileRecord.addColumnMetadata(19, "DOWNLOAD_DATE",
						 java.sql.Types.DATE,  0,0);
		    fileRecord.addColumnMetadata(20, "IMPORT_DATE",
						 java.sql.Types.DATE,  0,0);
		    fileRecord.addColumnMetadata(21, "SOURCESYSTEM_CD",
						 java.sql.Types.NVARCHAR,50,0); 
		    fileRecord.addColumnMetadata(22, "VALUETYPE_CD",
						 java.sql.Types.NVARCHAR,50,0);
		    fileRecord.addColumnMetadata(23, "M_EXCLUSION_CD",
						 java.sql.Types.NVARCHAR,25,0); 
		    fileRecord.addColumnMetadata(24, "C_PATH",
						 java.sql.Types.NVARCHAR,700,0); 
		    fileRecord.addColumnMetadata(25, "C_SYMBOL",
						 java.sql.Types.NVARCHAR,50,0);
		
		    // Connection destinationConnection =
		    //       DriverManager.getConnection(connectionUrl);
		    SQLServerBulkCopyOptions copyOptions =
			new SQLServerBulkCopyOptions();  
		
		    // Depending on the size of the data being uploaded,
		    // and the amount of RAM, an optimum can be found here.
		    //Play around with this to improve performance.
		    copyOptions.setBatchSize(batchSize); 
		    
		    // This is crucial to get good performance
		    copyOptions.setTableLock(true);  
		    
		    SQLServerBulkCopy bulkCopy =
			new SQLServerBulkCopy(connection);
		    bulkCopy.clearColumnMappings();
		    for ( int m = 0; m < 25; m++) {
			bulkCopy.addColumnMapping(ontologyColumnName[m],
						  ontologyColumnName[m]);
		    }

		    bulkCopy.setBulkCopyOptions(copyOptions);  
		    bulkCopy.setDestinationTableName(ontologyTable[i]);
		    bulkCopy.writeToServer(fileRecord);
		    
		    long endTime   = System.currentTimeMillis();
		    long totalTime = endTime - startTime;
		    System.out.println(totalTime + "ms");
		}
		// Update TABLE_ACCESS
		// TODO: Separate method
		String truncateTable = "TRUNCATE TABLE TABLE_ACCESS";
		statement.executeUpdate(truncateTable);
		System.out.println("Truncate table TABLE_ACCESS");

		// Read in tab separated file
		SQLServerBulkCSVFileRecord fileRecord2 =
		    new SQLServerBulkCSVFileRecord("TABLE_ACCESS.tsv",
						   null, "\t", true);   
		// Assign database data types
		fileRecord2.addColumnMetadata(1, "C_TABLE_CD",
					     java.sql.Types.NVARCHAR,50,0);
		fileRecord2.addColumnMetadata(2, "C_TABLE_NAME",
					     java.sql.Types.NVARCHAR,50,0);
		fileRecord2.addColumnMetadata(3, "C_PROTECTED_ACCESS",
					     java.sql.Types.NVARCHAR,1,0);
		fileRecord2.addColumnMetadata(4, "C_HLEVEL",
					     java.sql.Types.INTEGER,22,0);
		fileRecord2.addColumnMetadata(5, "C_FULLNAME",
					     java.sql.Types.NVARCHAR,700,0); 
		fileRecord2.addColumnMetadata(6, "C_NAME",
					     java.sql.Types.NVARCHAR,2000,0); 
		fileRecord2.addColumnMetadata(7, "C_SYNONYM_CD",
					     java.sql.Types.NVARCHAR,1,0); 
		fileRecord2.addColumnMetadata(8, "C_VISUALATTRIBUTES",
					     java.sql.Types.NVARCHAR,3,0); 
		fileRecord2.addColumnMetadata(9, "C_TOTALNUM",
					     java.sql.Types.INTEGER, 22,0);
		fileRecord2.addColumnMetadata(10, "C_BASECODE",
					     java.sql.Types.NVARCHAR,50,0);
		fileRecord2.addColumnMetadata(11, "C_METADATAXML",
					     java.sql.Types.LONGVARCHAR,0, 0);
		fileRecord2.addColumnMetadata(12, "C_FACTTABLECOLUMN",
					     java.sql.Types.NVARCHAR,50,0); 
		fileRecord2.addColumnMetadata(13, "C_DIMTABLENAME",
					     java.sql.Types.NVARCHAR,50,0); 
		fileRecord2.addColumnMetadata(14, "C_COLUMNNAME",
					     java.sql.Types.NVARCHAR,50,0); 
		fileRecord2.addColumnMetadata(15, "C_COLUMNDATATYPE",
					     java.sql.Types.NVARCHAR,700,0);
		fileRecord2.addColumnMetadata(16, "C_OPERATOR",
					     java.sql.Types.NVARCHAR,10,0); 
		fileRecord2.addColumnMetadata(17, "C_DIMCODE",
					     java.sql.Types.NVARCHAR,700,0); 
		fileRecord2.addColumnMetadata(18, "C_COMMENT",
					     java.sql.Types.LONGVARCHAR,0, 0);
		fileRecord2.addColumnMetadata(19, "C_TOOLTIP",
					     java.sql.Types.NVARCHAR,900,0); 
		fileRecord2.addColumnMetadata(20, "C_ENTRY_DATE",
					     java.sql.Types.DATE, 0,0);
		fileRecord2.addColumnMetadata(21, "C_CHANGE_DATE",
					     java.sql.Types.DATE,  0,0);
		fileRecord2.addColumnMetadata(22, "C_STATUS_CD",
					     java.sql.Types.NVARCHAR,1,0);
		fileRecord2.addColumnMetadata(23, "VALUETYPE_CD",
					     java.sql.Types.NVARCHAR,50,0); 
		fileRecord2.addColumnMetadata(24, "C_ONTOLOGY_PROTECTION",
					     java.sql.Types.LONGVARCHAR,0,0); 
		
		// Connection destinationConnection =
		//       DriverManager.getConnection(connectionUrl);
		SQLServerBulkCopyOptions copyOptions =
		    new SQLServerBulkCopyOptions();  
		
		// Depending on the size of the data being uploaded,
		// and the amount of RAM, an optimum can be found here.
		//Play around with this to improve performance.
		copyOptions.setBatchSize(batchSize); 
		
		// This is crucial to get good performance
		copyOptions.setTableLock(true);  
		
		SQLServerBulkCopy bulkCopy =
		    new SQLServerBulkCopy(connection);
		bulkCopy.clearColumnMappings();
		for ( int j = 0; j < 24; j++) {
		    bulkCopy.addColumnMapping(tableAccessColumnName[j],
					      tableAccessColumnName[j]);
		}
		bulkCopy.setBulkCopyOptions(copyOptions);  
		bulkCopy.setDestinationTableName("TABLE_ACCESS");
		bulkCopy.writeToServer(fileRecord2);

		// Update SCHEMES
		// TODO: Separate method
		// TODO: Check if prefixes are already in the table before trying
		//       to append
		String truncateSchemesTable = "TRUNCATE TABLE SCHEMES";
		statement.executeUpdate(truncateSchemesTable);
		System.out.println("Truncate table SCHEMES");

		// Read in tab separated file
		SQLServerBulkCSVFileRecord fileRecord3 =
		    new SQLServerBulkCSVFileRecord("SCHEMES.tsv",
						   null, "\t", true);   
		// Assign database data types
		fileRecord3.addColumnMetadata(1, "C_KEY",
					     java.sql.Types.NVARCHAR,50,0);
		fileRecord3.addColumnMetadata(2, "C_NAME",
					     java.sql.Types.NVARCHAR,50,0);
		fileRecord3.addColumnMetadata(3, "C_DESCRIPTION",
					     java.sql.Types.NVARCHAR,100,0);
		
		// Connection destinationConnection =
		//       DriverManager.getConnection(connectionUrl);
		copyOptions =
		    new SQLServerBulkCopyOptions();  
		
		// Depending on the size of the data being uploaded,
		// and the amount of RAM, an optimum can be found here.
		//Play around with this to improve performance.
		copyOptions.setBatchSize(batchSize); 
		
		// This is crucial to get good performance
		copyOptions.setTableLock(true);  
		
		bulkCopy =
		    new SQLServerBulkCopy(connection);
		bulkCopy.clearColumnMappings();
		for ( int k = 0; k < 3; k++) {
		    bulkCopy.addColumnMapping(schemesColumnName[k],
					      schemesColumnName[k]);
		}
		bulkCopy.setBulkCopyOptions(copyOptions);  
		bulkCopy.setDestinationTableName("SCHEMES");
		bulkCopy.writeToServer(fileRecord3);



	    }
        catch (SQLException e) {
	    System.out.println("SQLException");
            e.printStackTrace();
        }
    }
    
}

