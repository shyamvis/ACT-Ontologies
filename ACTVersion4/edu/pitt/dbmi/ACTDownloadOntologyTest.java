package edu.pitt.dbmi;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

public class ACTDownloadOntologyTest {

    public static void main(String[] args) {
	// TODO: What should the args be
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
				     "ACT_VITAL_SIGNS_V4.tsv",
				     "TABLE_ACCESS.tsv",
				     "SCHEMES.tsv"
	};

	String[] conceptDimensionFileNames = {"ACT_MED_ALPHA_V4_CD.tsv",
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
					      "ACT_VITAL_SIGNS_V4_CD.tsv",
					      "QT_BREAKDOWN_PATH.tsv"
		};
	
	for ( int i = 0; i < 18; i++) {
	    Wget.wGet(ontologyFileName[i],
		      "https://act-ontology-v4-test.s3.amazonaws.com/metadata/"
		      +ontologyFileName[i]);
	}
	for ( int i = 0; i < 17; i++) {
	    Wget.wGet(conceptDimensionFileNames[i],
		      "https://act-ontology-v4-test.s3.amazonaws.com/crc/"
		      +conceptDimensionFileNames[i]);
	}
	//https://act-ontology-v4-test.s3.amazonaws.com/crc/ACT_COVID_V4_CD.tsv	
	
    }

    public static WgetStatus wGet(String saveAsFile, String urlOfFile) {
	System.out.println("urlOfFile: " + urlOfFile +" saveAsFile: " + saveAsFile);
	InputStream httpIn = null;
	OutputStream fileOutput = null;
	OutputStream bufferedOut = null;
	try {
	    // check the http connection before we do anything to the fs
	    URL url = new URL(urlOfFile);
	    httpIn = new BufferedInputStream(url.openStream());
	    // prep saving the file
	    fileOutput = new FileOutputStream(saveAsFile);
	    bufferedOut = new BufferedOutputStream(fileOutput, 1024);
	    byte data[] = new byte[1024];
	    boolean fileComplete = false;
	    int count = 0;
	    while (!fileComplete) {
		count = httpIn.read(data, 0, 1024);
		if (count <= 0) {
		    fileComplete = true;
		} else {
		    bufferedOut.write(data, 0, count);
		}
	    }
	} catch (MalformedURLException e) {
	    System.out.println("MalformedURLException");
	    return WgetStatus.MalformedUrl;
	} catch (IOException e) {
	    System.out.println("IOException");
	    return WgetStatus.IoException;
	}
	finally {
	    try {
		bufferedOut.close();
		fileOutput.close();
		httpIn.close();
	    } catch (IOException e) {
		return WgetStatus.UnableToCloseOutputStream;
	    }
	}
	return WgetStatus.Success;
    }
    
    
}
