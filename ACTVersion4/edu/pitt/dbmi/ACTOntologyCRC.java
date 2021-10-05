package edu.pitt.dbmi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// Make sure to use version 4.2, as SQLServerBulkCSVFileRecord
//is not included in version 4.1
import com.microsoft.sqlserver.jdbc.*;

public class ACTOntologyCRC {


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

	String[] ontologyFileName = {"ACT_MED_ALPHA_V4_CD.tsv",
				     "ACT_MED_VA_V4_CD.tsv",
				     "ACT_COVID_V4_CD.tsv",
				     "ACT_CPT4_PX_V4_CD.tsv",
				     "ACT_DEM_V4_CD.tsv",
				     "ACT_HCPCS_PX_V4_CD.tsv",
				     "ACT_ICD10CM_DX_V4_CD.tsv",
				     "ACT_ICD10PCS_PX_V4_CD.tsv",
				     "ACT_ICD10_ICD9_DX_V4_CD.tsv",
				     "ACT_ICD9CM_DX_V4_CD.tsv",
				     "ACT_ICD9CM_PX_V4_CD.tsv",
				     "ACT_LOINC_LAB_PROV_V4_CD.tsv",
				     "ACT_LOINC_LAB_V4_CD.tsv",
				     "ACT_SDOH_V4_CD.tsv",
				     "ACT_VISIT_DETAILS_V4_CD.tsv",
				     "ACT_VITAL_SIGNS_V4_CD.tsv"};
	


	String [] conceptDimensionColumnName = {
	    "CONCEPT_PATH",
	    "CONCEPT_CD",
	    "NAME_CHAR",
	    "CONCEPT_BLOB",
	    "UPDATE_DATE",
	    "DOWNLOAD_DATE",
	    "IMPORT_DATE",
	    "SOURCESYSTEM_CD",
	    "UPLOAD_ID"
	};

	String [] breakdownColumnName = {
	    "NAME",
	    "VALUE",
	    "CREATE_DATE",
	    "UPDATE_DATE",
	    "USER_ID"
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
		    // String dropTable = "DROP TABLE IF EXISTS " + ontologyTable[i];
		    // statement.executeUpdate(dropTable);
		    //  System.out.println("Drop table in given database..."
		    //	       + ontologyTable[i]);   	  
		    
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
		    fileRecord.addColumnMetadata(1, "CONCEPT_PATH",
						 java.sql.Types.NVARCHAR,700,0);
		    fileRecord.addColumnMetadata(2, "CONCEPT_CD",
						 java.sql.Types.NVARCHAR,50,0);
		    fileRecord.addColumnMetadata(3, "NAME_CHAR",
						 java.sql.Types.NVARCHAR,2000,0);
		    fileRecord.addColumnMetadata(4, "CONCEPT_BLOB",
						 java.sql.Types.LONGVARCHAR,0, 0);
		    fileRecord.addColumnMetadata(5, "UPDATE_DATE",
						 java.sql.Types.DATE, 0,0);
		    fileRecord.addColumnMetadata(6, "DOWNLOAD_DATE",
						 java.sql.Types.DATE,  0,0);
		    fileRecord.addColumnMetadata(7, "IMPORT_DATE",
						 java.sql.Types.DATE,  0,0);
		    fileRecord.addColumnMetadata(8, "SOURCESYSTEM_CD",
						 java.sql.Types.NVARCHAR,50,0); 
		    fileRecord.addColumnMetadata(9, "UPLOAD_ID",
						 java.sql.Types.INTEGER,0,0);		
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
		    for ( int m = 0; m < 9; m++) {
			bulkCopy.addColumnMapping(conceptDimensionColumnName[m],
						  conceptDimensionColumnName[m]);
		    }

		    bulkCopy.setBulkCopyOptions(copyOptions);  
		    bulkCopy.setDestinationTableName("CONCEPT_DIMENSION");
		    //bulkCopy.writeToServer(fileRecord);
		    
		    long endTime   = System.currentTimeMillis();
		    long totalTime = endTime - startTime;
		    System.out.println(totalTime + "ms");
		}


		// Update QT_BREAKDOWN_PATH
		// TODO: Separate method
		String truncateTable = "TRUNCATE TABLE QT_BREAKDOWN_PATH";
		statement.executeUpdate(truncateTable);
		System.out.println("Truncate table QT_BREAKDOWN_PATH");

		// Read in tab separated file
		SQLServerBulkCSVFileRecord fileRecord2 =
		    new SQLServerBulkCSVFileRecord("QT_BREAKDOWN_PATH.tsv",
						   null, "\t", true);   
		// Assign database data types
		fileRecord2.addColumnMetadata(1, "NAME",
					     java.sql.Types.NVARCHAR,100,0);
		fileRecord2.addColumnMetadata(2, "VALUE",
					     java.sql.Types.NVARCHAR,2000,0);
		fileRecord2.addColumnMetadata(3, "CREATE_DATE",
					     java.sql.Types.DATE, 0,0);
		fileRecord2.addColumnMetadata(4, "UPDATE_DATE",
					     java.sql.Types.DATE,  0,0);
		fileRecord2.addColumnMetadata(5, "USER_ID",
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
		for ( int j = 0; j < 5; j++) {
		    bulkCopy.addColumnMapping(breakdownColumnName[j],
					      breakdownColumnName[j]);
		}
		bulkCopy.setBulkCopyOptions(copyOptions);  
		bulkCopy.setDestinationTableName("QT_BREAKDOWN_PATH");
		bulkCopy.writeToServer(fileRecord2);
		
	    }
        catch (SQLException e) {
	    System.out.println("SQLException");
            e.printStackTrace();
        }
    }
    
}

