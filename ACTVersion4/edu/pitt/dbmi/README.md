**** THIS IS GARBAGE PROOF OF CONCEPT TEST CODE *****
Add SqlServer JDBC Driver to your classpath
Compile code javac edu/pitt/dbmi/*.java
Download ACT Ontology V4 TSV files  - should take about 5 minutes
  java ACTDownloadOntologyTest
Import ACT metadata files into your SQLServer i2b2 metadata schema. This will create tables and import data
  java edu.pitt.dbmi.ACTImportOntologyTest username password localhost  i2b2metadata 1433
Import ACT crc files into your SQLServer i2b2 crcdata schem This will truncate concept_dimension and table access and import the data
  java edu.pitt.dbmi.ACTOntologyCRC username password localhost  i2b2demodata 1433

 
