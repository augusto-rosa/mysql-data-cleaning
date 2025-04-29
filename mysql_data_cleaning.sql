/*

Author: Augusto Rosa

Source: Layoffs Dataset

Description: Perform a data cleaning using MySQL

Language Used: SQL

Skills used: Joins, CTE's, Windows Functions, Aggregate Functions, Converting Data Types

*/

USE layoffs_database

-- Analyzing a portion of the data
SELECT * FROM layoffs_database.layoffs LIMIT 10;

-- First thing let's create a staging table. 
-- We will keep the original data in the raw table to ensure we have a backup if something happens and to keep the original dataset intact

CREATE TABLE layoffs_database.layoffs_staging_table
LIKE
layoffs_database.layoffs;

-- Now, let's populate the staging table with the same data from the raw table

INSERT INTO layoffs_database.layoffs_staging_table
SELECT * FROM layoffs_database.layoffs;

-- Validating the data in the layoffs_staging_table

SELECT * FROM layoffs_database.layoffs_staging_table LIMIT 10;

-- ** 1. Remove Duplicates **

-- Creating a custom column to set a unique ID per row using ROW_NUMBER()
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, `date`) AS ROW_NUM
FROM layoffs_database.layoffs_staging_table;

-- Creating a subquery to analyze which exact records are duplicated

SELECT *
FROM (
		SELECT *,
			ROW_NUMBER() OVER(
				PARTITION BY company, location, industry, total_laid_off, `date`) AS ROW_NUM
		FROM layoffs_database.layoffs_staging_table
) AS Duplicates
WHERE 1=1
AND ROW_NUM > 1;

-- Looking at companies like Casper and Oda, for example

SELECT * 
FROM layoffs_database.layoffs_staging_table
WHERE 1=1
AND company IN ('Casper', 'Oda');

-- Some records (like Casper) appear to be truly duplicated, while others (like Oda) seem legitimate. 
-- More information would be needed for a trully confirmation

-- Now, finding truly duplicated records by partitioning across all fields

SELECT *
FROM (
		SELECT 
		 company
		,location
		,industry
		,total_laid_off
		,percentage_laid_off
		,`date`
		,stage
		,country
		,funds_raised_millions
		,ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS ROW_NUM
		FROM layoffs_database.layoffs_staging_table
) AS duplicates_values
WHERE 1=1
AND ROW_NUM > 1;

-- Now let's delete the duplicated records where ROW_NUM > 1

-- A good practice for this case is to create a temporary table to store the duplicates and then use it to delete from the staging table

CREATE TEMPORARY TABLE layoffs_database.temp_duplicates AS
SELECT *
FROM (
		SELECT 
		 company
		,location
		,industry
		,total_laid_off
		,percentage_laid_off
		,`date`
		,stage
		,country
		,funds_raised_millions
		,ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS ROW_NUM
		FROM layoffs_database.layoffs_staging_table
) AS duplicates_values
WHERE 1=1
AND ROW_NUM > 1;

-- Validating if the data has been populated in the temporary table (temp_duplicates)

SELECT * FROM layoffs_database.temp_duplicates LIMIT 10;

-- Now we will delete the duplicated records in the staging table based on the temporary table (temp_duplicates)
DELETE FROM layoffs_database.layoffs_staging_table
WHERE (  company
		,location
		,industry
		,total_laid_off
		,percentage_laid_off
		,`date`
		,stage
		,country
		,funds_raised_millions) IN (
		
		SELECT 
		 company
		,location
		,industry
		,total_laid_off
		,percentage_laid_off
		,`date`
		,stage
		,country
		,funds_raised_millions
FROM layoffs_database.temp_duplicates);

-- Dropping the temporary table to free up memory
DROP TABLE IF EXISTS layoffs_database.temp_duplicates;

-- Validating that the records have been successfully deleted
-- Let's use Casper as an example

SELECT * 
FROM layoffs_database.layoffs_staging_table
WHERE 1=1
AND company IN ('Casper');

-- Records were successfully deleted

-- ** 2. Standardize Data **


-- We have some blank or NULL fields in the "industry" column

SELECT * 
FROM layoffs_database.layoffs_staging_table
WHERE 1=1
AND (industry IS NULL OR industry = '' OR industry = 'NULL')
ORDER BY industry;

-- I will set all empty or 'NULL' string values to real NULLs, making it easier to perform further cleaning

UPDATE layoffs_database.layoffs_staging_table
SET industry = NULL
WHERE 1=1
AND (industry = '' OR industry = 'NULL');

-- After updating, I noticed that there are three different names for "Crypto"

SELECT DISTINCT industry
FROM layoffs_database.layoffs_staging_table
ORDER BY industry;

-- Checking all records related to Crypto, as it seems to have multiple variations

SELECT * 
FROM layoffs_database.layoffs_staging_table
WHERE 1=1
AND industry IN ('Crypto', 'Crypto Currency', 'CryptoCurrency');

-- Now I will update all variations to a standardized value: "Crypto"

UPDATE layoffs_database.layoffs_staging_table
SET industry = 'Crypto'
WHERE 1=1
AND industry IN ('Crypto Currency', 'CryptoCurrency');

-- Validating that the data has been successfully standardized

SELECT DISTINCT industry
FROM layoffs_database.layoffs_staging_table
ORDER BY industry;

-- Removing "NULL" strings from fields related to numeric values like total_laid_off and percentage_laid_off,
-- and changing their data types to appropriate types

-- total_laid_off

UPDATE layoffs_database.layoffs_staging_table
SET total_laid_off = NULL
WHERE 1=1
AND total_laid_off = 'NULL';

-- Updating the column data type
ALTER TABLE layoffs_database.layoffs_staging_table
MODIFY COLUMN total_laid_off INT;

-- percentage_laid_off

UPDATE layoffs_database.layoffs_staging_table
SET percentage_laid_off = NULL
WHERE 1=1
AND percentage_laid_off = 'NULL';

-- Updating the column data type
ALTER TABLE layoffs_database.layoffs_staging_table
MODIFY COLUMN percentage_laid_off DECIMAL(10,2);

-- Validating the updates

SELECT * FROM layoffs_database.layoffs_staging_table LIMIT 10;

-- Now I need to update the 'date' field: fix the format and convert to a DATE type

-- Removing string "NULL" values from the date field first

UPDATE layoffs_database.layoffs_staging_table
SET `date` = NULL
WHERE 1=1
AND `date` = 'NULL';

-- Now adjusting the date field to the correct format

UPDATE layoffs_database.layoffs_staging_table
SET `date` = STR_TO_DATE(`date`, "%m/%d/%Y");

-- Now that the values are adjusted, let's change the column type to DATE

ALTER TABLE layoffs_database.layoffs_staging_table
MODIFY COLUMN `date` DATE;

-- Removing "NULL" string values from the STAGE and FUNDS_RAISED_MILLIONS columns

-- stage column

UPDATE layoffs_database.layoffs_staging_table
SET stage = 'Unknown'
WHERE 1=1
AND (stage IS NULL OR stage = 'NULL');

-- Coluna funds_raised_millions

UPDATE layoffs_database.layoffs_staging_table
SET funds_raised_millions = NULL
WHERE 1=1
AND funds_raised_millions = 'NULL';


-- Now in the Country column, for the United States we have two values: 
-- One as 'United States' and another as 'United States.'
-- So, let's standardize this to be just 'United States' (without the period at the end)

UPDATE layoffs_database.layoffs_staging_table
SET country = 'United States'
WHERE 1=1
AND country = 'United States.';

-- Validating the values:

SELECT * FROM layoffs_database.layoffs_staging_table LIMIT 10;

-- Now let's create the final table that will be used for analysis by the end user

DROP TABLE IF EXISTS layoffs_database.layoffs_analytics;

CREATE TABLE layoffs_database.layoffs_analytics(
 `company` VARCHAR(50) DEFAULT NULL,
 `location` VARCHAR(50) DEFAULT NULL,
 `industry` VARCHAR(50) DEFAULT NULL,
 `total_laid_off` INT DEFAULT NULL,
 `percentage_laid_off` DECIMAL(10,2) DEFAULT NULL,
 `date` DATE DEFAULT NULL,
 `stage` VARCHAR(50) DEFAULT NULL,
 `country` VARCHAR(50) DEFAULT NULL,
 `funds_raised_millions` VARCHAR(10) DEFAULT NULL
);

-- Creating some indexes

CREATE INDEX idx_company ON layoffs_database.layoffs_analytics (company);
CREATE INDEX idx_industry ON layoffs_database.layoffs_analytics (industry);
CREATE INDEX idx_date ON layoffs_database.layoffs_analytics (date);

-- Inserting the data from the staging table into the analytics table

INSERT INTO layoffs_database.layoffs_analytics
SELECT			
		 company
		,location
		,industry
		,total_laid_off
		,percentage_laid_off
		,`date`
		,stage
		,country
		,funds_raised_millions
FROM layoffs_database.layoffs_staging_table;

-- Validating the analytics table
SELECT * FROM layoffs_database.layoffs_analytics LIMIT 10;







