Instructions for importing using SQL Developer and .dsv files
1. Truncate TABLE_ACCESS *This only applies when testing
2. Import TABLE_ACCESS ( table_access.csv ) 
3. Create the tables:
	create table ACT_SDOH_V4 as select * from NCATS_DEMOGRAPHICS where 1=0;
	create table ACT_VITAL_SIGNS_V4 as select * from NCATS_DEMOGRAPHICS where 1=0;
	create table ACT_DEMOGRAPHICS_V4 as select * from NCATS_DEMOGRAPHICS where 1=0;

Import the tables ( for now sdoh.dsv, vitalsigns.dsv and demographics.dsv)
4. Right click on the table.
5. Select "Import"
6. Select the appropriate pipe delimited file name (i.e. sdoh.dsv for the ACT_SDOH_V4 table)
7. Set the delimiter to pipe "|"
8. Set enclosed in quotes "
9. On the Column Definition page make sure that th 3 date fields update_date, download_date and import_date have the format RRRR-MM-DD
10. Import the data
