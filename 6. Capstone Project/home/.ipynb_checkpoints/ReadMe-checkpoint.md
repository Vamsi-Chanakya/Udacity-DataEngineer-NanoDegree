# Data Engineer - Capstone Project with Apache Spark

## Project Summary
In our research company, capstone we are tasked to do trend analysis on tourism behaviour patterns. We build a ETL pipeline with Start Schema Model which takes raw data inform of csv files and creates dimension and fact tables, which will later be used by our research team to do the trend analysis. 

## The project overview steps:

### Step 1: Scope the Project and Gather Data

#### Lookup Dimension Tables

For this project we used I94_SAS_Label_Descriptions dataset to create 5 lookup dimension tables, which are addr table, country table, mode table, port table and visa table.

addr table is the cross reference for state code and state name.
country table is the lookup table for country code and country name.
mode table is the lookup for mode of transport.
port table is the lookup for port details based on port code. 
visa table is the lookup for visa type.

Duplicate and null record removal is implemented for the addr, country and port lookup tables.

#### I94 Immigration Data

The I94 immigration data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace.
This data was already provided inside sas_data folder in parquet file type. We will use the following columns for our ETL Pipeline:

cicid    - Unique Id for each record
i94yr    - 4 digit year
i94mon   - Numeric month 
i94cit   - City Code
i94res   - Resident Country Code
i94port  - 3 char port code
i94mode  - Mode of transport
i94addr  - 2 char State code
i94bir   - Age of Respondent in Years
i94visa  - Visa codes collapsed into three categories
biryear  - 4 digit year of birth
gender   - Non-immigrant sex
visatype - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
arrdate  - Is the Arrival Date in the USA. It is a SAS date numeric field.
depdate  - Is the Departure Date from the USA. It is a SAS date numeric field


#### U.S. City Demographic Data 

This data comes from OpenSoft, with following detetails.

City (USA)
Male Population
Median Age (overall median age within the city population)
Male population
Female Population
Total Population
No of veterans
Foreign-Born (number of foreign born residences)
State Code (USA state abbreviation of the City column)
Race (specifies race category suchas Asian, Alaskan Indian, Black, Hispanic, etc)
Count (number of people under specific race category anotated by Race column)

### Step 2: Explore and Assess the Data

#### US DEMOGRAPHICS DATA SET 
Transform the US Demo dataset by doing a pivot on the Race column to convert Race column values into individual columns. We will accomplish this transformation by separating both Race and Count columns from the US demographics dataset and create two separate datasets called cities_df and cities_pivot_df.

cities_pivot_df dataset will also include City and State Code columns so we can use them to join back with US dataset. Before we join the two datasets we first remove duplicate rows from the cities_df and pivot the cities_df Count dataset. In theory both datasets should have equal rows to join them back to single transformed table.

#### Lookup Tables
From lookup tables also duplicates and null key value columns are removed.

### Step 3: Define the Data Model
Using the dimensional tables saved as parquet files we can implement them on any columnar database in Star Schema model. Star Schema model was chosen because it will be easier for Data Analysts and Data Scientists to understand and apply queries with best performance outcomes and flexibility.

![Star%20Schema%20for%20Data%20Modeling](screenshots/Star%20Schema.png)

Created 5 lookup tables, which are addr, country, mode, port and visa tables. And 3 dim dataframes which are, immigrant, demographic and arrival_season and finally 2 fact tables which are immigrant fact table and demographic fact table. The lookup and dim data is finally transformed into fact tables.

### Step 4: Run ETL to Model the Data
The data pipeline is built inside the ETL.py file included with this Capstone Project.

#### 4.1 Extraction Process:
Extraction process is comprised of extracting Immigration information from immegration_data.csv file and demographic information is extracted from us-cities-demographics.csv and lookup tables from SAS Labels dataset. 

### Transformation process:
In this stage we transform raw immigration and demographic information into fact data.

### Load process:
In this stage we create final Fact dataframe which is immigrant_fact_table and us_cities_demographic parquet files.

### 4.2 Data Quality Checks

After completing the loading process, we perform a data quality check through the step Data_Quality_Checks to make sure everything was OK. In this check we verify if every table was actually loaded with count check in all the tables of the model.

#### Integrity constraints on the relational database (e.g., unique key, data type, etc.)
we are taking distinct records (unique) for address, port and country lookup 

#### Unit tests for the scripts to ensure they are doing the right thing
unit tests will be performed to ensure all the records in immigration dim data is loaded into immigration fact data and pivoting happened properly for us-cities-demographics table.

#### Source/Count checks to ensure completeness
1. count check for immigration dim table count should be equal to immoigration fact table count
2. distinct city count from us-cities-demographics table should be equal to distinct cities count in us_cities_demographics output fact table 

![Data_Quality_Checks_in_code](screenshots/Data_Quality_Checks_in_code.png)

![Data_Quality_Checks_Counts](screenshots/Data_Quality_Checks_Counts.png)

Similar way we can include zero records count checks for other DIM and FACT tables as well.

So the above count checks ensure that our data is loaded as per process.

### Step 5: Complete Project Write Up
Clearly state the rationale for the choice of tools and technologies for the project?
Because we are dealing with big data and cloud technologies for solutions it made economical sense to use opensource Apache PySpark and Python tools that can be easily ported over to cloud solution such as AWS.

Propose how often the data should be updated and why?
Lookup Dimenstion tables only have to be updated when a new category is created by immigration department. However, the I94 non-immigrant port of entry data along with the time dimension table (arrival_season) can be updated every month. The US Cities Demographics data is updated when ever the new data is available.

Write a description of how you would approach the problem differently under the following scenarios:
The data was increased by 100x?
Deploy this Spark solution on a cluster using AWS (EMR cluster) and use S3 for data and parquet file storage. AWS will easily scale when data increases by 100x

The data populates a dashboard that must be updated on a daily basis by 7am every day?
Use Apache Airflow for scheduling the pipeline.

The database needed to be accessed by 100+ people?
The saved parquet files can be bulk copied over to AWS Redshift cluster where it can scale big data requirements and has 'massively parallel' and 'limitless concurrency' for thousands of concurrent queries executed by users.