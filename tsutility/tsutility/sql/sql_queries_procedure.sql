/****** Object:  StoredProcedure [fcst].[Apollo_WRTGB01_Update]    Script Date: 1/23/2020 12:15:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      <Kevin Choi, Apollo WRT GB01 ETL>
-- Create Date: <2020-01-22>
-- Description: <Get Apollo Results for WRT GB01 for implemented items, and new high performance items >
-- =============================================
ALTER PROCEDURE [fcst].[Apollo_WRTGB01_Update] AS

-- ====TO DO====

-- ====Contents====
--1. SET PARAMETERS
--2. BASE FORECAST TABLE
--3. NEW WINNERS AND HIGH POSITION TABLE
--4. Create Tables

------------------------------------------------------------------------------------------------------
-- 1. SET PARAMETERS
------------------------------------------------------------------------------------------------------

DECLARE @run_date AS DATE = '2020-01-22';

DECLARE @division_filter varchar(255) = 'writing';

DECLARE @distribution_filter varchar(255) = '01';

DECLARE @sales_org_filter varchar(255) = 'GB01';

DECLARE @sales_org_flter_in_model_results varchar(255) = 'gb01';


------------------------------------------------------------------------------------------------------
-- 2. BASE FORECAST TABLE
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS apollo.WRTGB01_IMPLEMENTED_RESULTS

	SELECT 

	UPPER(fcst.material) AS Material,
	distribution_channel AS 'Distribution_Channel',
	UPPER(fcst.sales_org) AS 'Sales_Org',
	fcst.location AS 'Location',
	month AS 'Month',
	fcst.apollo_class AS Class, 
	model_better as Model_Better, 
	FLOOR(IIF(winning_model_forecast = 0, 1, winning_model_forecast)) AS Forecast,
	CONCAT(fcst.material, fcst.location) AS matloca,
	@run_date as run_date

	INTO apollo.WRTGB01_IMPLEMENTED_RESULTS

	FROM apollo.ORDER_MATERIAL_LOCATION_MODEL_RESULT AS fcst

	JOIN apollo.stgGB01latestLoaded AS latest ON latest.matloc = CONCAT(fcst.material, fcst.location)

	WHERE 
	(
		division = @division_filter AND
		benchmark_type = 'consensus' AND
		data_split = 'Forecast' AND
		fcst.distribution_channel = @distribution_filter AND
		fcst.sales_org = @sales_org_filter
	)

	INSERT INTO apollo_archive.WRTGB01_IMPLEMENTED_RESULTS SELECT * FROM apollo.WRTGB01_IMPLEMENTED_RESULTS;

------------------------------------------------------------------------------------------------------
-- 3. Winners and High Performance FORECAST TABLE
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS apollo.WRTGB01_NEW_WINNERS_HIGHPOS_RESULTS
	
	SELECT

	UPPER(fcst.material) AS Material,
	distribution_channel AS 'Distribution_Channel',
	UPPER(fcst.sales_org) AS 'Sales_Org',
	fcst.location AS 'Location',
	month AS 'Month',
	FLOOR(IIF(winning_model_forecast = 0, 1, winning_model_forecast)) AS Forecast,
	CONCAT(fcst.material, fcst.location) AS matloca,
	@run_date as run_date

	INTO apollo.WRTGB01_NEW_WINNERS_HIGHPOS_RESULTS

	FROM apollo.ORDER_MATERIAL_LOCATION_MODEL_RESULT AS fcst

	WHERE 
	(
		division = @division_filter AND
		benchmark_type = 'consensus' AND
		data_split = 'Forecast' AND
		fcst.distribution_channel = @distribution_filter AND
		fcst.sales_org = @sales_org_flter_in_model_results AND
		CONCAT(fcst.material,fcst.location) NOT IN (SELECT DISTINCT(matloc) FROM apollo.stgGB01latestLoaded) AND
		location != '1040' AND
		model_better = 'Y' AND
		apollo_class = 'High Performance'
	)

	INSERT INTO apollo_archive.WRTGB01_NEW_WINNERS_HIGHPOS_RESULTS SELECT * FROM apollo.WRTGB01_NEW_WINNERS_HIGHPOS_RESULTS;

------------------------------------------------------------------------------------------------------
-- 4. Create Tables
------------------------------------------------------------------------------------------------------

/**
CREATE TABLE apollo.WRTGB01_NEW_WINNERS_HIGHPOS_RESULTS (
    Material VARCHAR(255),
    Distribution_Channel varchar(255),
    Sales_Org varchar(255),
	Location varchar(255),
	Month Date,
	Forecast INT,
	matloca varchar(255),
	run_date Date
) 


CREATE TABLE apollo.WRTGB01_IMPLEMENTED_RESULTS (
    Material VARCHAR(255),
    Distribution_Channel varchar(255),
    Sales_Org varchar(255),
	Location varchar(255),
	Month Date,
	Class varchar(255),
	Model_Better varchar(255),
	Forecast INT,
	matloca varchar(255),
	run_date Date
) **/

