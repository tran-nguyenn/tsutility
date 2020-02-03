/****** Object:  StoredProcedure [fcst].[XYZ_MATLOC_ETL]    Script Date: 1/28/2020 1:01:39 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:      <Kevin Choi, , XYZ>
-- Create Date: <2020-01-22>
-- Description: <ETL Process for XYZ Classifier using Apollo and SOH>
-- =============================================
ALTER PROCEDURE [fcst].[XYZ_MATLOC_ETL] AS

-- ====TO DO====

-- ====Contents====
--1. SET PARAMETERS
--2. SOH TABLE
--3. APOLLO RUN TABLE
--4. JOIN APOLLO AND SOH TABLE
--5. PUT JOINED APOLLO AND SOH TABLE INTO XYZ_BASE

------------------------------------------------------------------------------------------------------
-- 1. SET PARAMETERS
------------------------------------------------------------------------------------------------------

DECLARE @xyz_run_date AS DATE = '2020-01-24';

DECLARE @run_id_filter varchar(255) = '190691871';

------------------------------------------------------------------------------------------------------
-- 2. SOH TABLE
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #MATLOC

SELECT
     
Material,
Plant,
Month_Date,
Sales_Organization,
Distribution_Channel,
SUM(SALES_ORDER_HISTORY) as SUM_UNCLEAN_SOH,
SUM(CLEAN_SALES_ORDER_HISTORY) as SUM_CLEAN_SOH
     
INTO #MATLOC

FROM (

	SELECT
             
	Distribution_Channel,
    UPPER(Material_Code) as Material,
    Plant,
    UPPER(salesorg) as Sales_Organization,
    Forecast_Date,
	Forecast_Year,
    Forecast_Month,
	DATEADD(mm, DATEDIFF(mm, 0, Forecast_Date), 0) as Month_Date,
    SALES_ORDER_HISTORY,
    CLEAN_SALES_ORDER_HISTORY
    
	FROM fcst.LGCY_NWL_SAP_FCST_ORDRS

	WHERE
		(
			 FORECAST_LAG = '0'
             AND
             Forecast_Year != '2016'
		)
    
	) MATLOC
    
	GROUP BY
    
	Material, Plant, Sales_Organization, Distribution_Channel, Forecast_Year, Forecast_Month, Month_Date

------------------------------------------------------------------------------------------------------
-- 3. BASE APOLL RUN TABLE
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #apollorun

SELECT

A.*

INTO #apollorun

FROM (

	SELECT

	AT.*,
	AR.run_id as run_id_ar,
	AT.run_id as run_id_at,
	AR.RUN_DATE as run_date_ar,
	AT.RUN_DATE as run_date_at,
	UPPER(AR.sales_org) as Sales_Organization,
	AR.month,
	DATEADD(mm, DATEDIFF(mm, 0, AR.month), 0) as month_date,
	AR.model_better,
	AR.best_model_wfa,
	AR.benchmark_wfa,
	AR.best_model,
	AR.holdout_best_model_wfa_bucket,
	AR.winning_model_wfa

	FROM apollo_archive.ORDER_MATERIAL_LOCATION_TIME_SERIES_ATTRIBUTES_TMP as AT

	JOIN apollo_archive.ORDER_MATERIAL_LOCATION_MODEL_RESULT as AR ON UPPER(AT.material) = UPPER(AR.material) AND UPPER(AT.location) = UPPER(AR.location)
	
	WHERE 

	(
	   AR.run_id = @run_id_filter
	   AND
	   AT.run_id = @run_id_filter
	)

	) A

------------------------------------------------------------------------------------------------------
-- 4. JOIN BASE APOLLO AND SOH TABLE
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #dataset

SELECT

B.Material as material,
B.Plant as location,
B.Month_Date as month_date,
B.Sales_Organization,
B.SUM_UNCLEAN_SOH,
B.SUM_CLEAN_SOH,
C.[abs_energy],
C.[absolute_sum_of_changes],
C.[coeff_of_variation],
C.[count_above_mean],
C.[count_below_mean],
C.[cov_bucket],
C.[distribution_channel],
C.[earliest_date],
C.[first_location_of_maximum],
C.[first_location_of_minimum],
C.[has_duplicate],
C.[has_duplicate_max],
C.[has_duplicate_min],
C.[holdout_training_ratio],
C.[is_disco],
C.[kurtosis],
C.[last_location_of_maximum],
C.[last_location_of_minimum],
C.[latest_date],
C.[length],
C.[long_term_max],
C.[long_term_mean],
C.[long_term_min],
C.[long_term_stdev],
C.[longest_strike_above_mean],
C.[longest_strike_below_mean],
C.[maximum],
C.[mean],
C.[mean_abs_change],
C.[mean_change],
C.[mean_second_derivative_central],
C.[median],
C.[minimum],
C.[missing_periods],
C.[near_disco],
C.[new_item],
C.[not_enough_history],
C.[percentage_of_reoccurring_datapoints_to_all_datapoints],
C.[percentage_of_reoccurring_values_to_all_values],
C.[ratio_value_number_to_time_series_length],
C.[run_id],
C.[sales_org],
C.[sample_entropy],
C.[skewness],
C.[standard_deviation],
C.[sum_of_reoccurring_data_points],
C.[sum_of_reoccurring_values],
C.[sum_values],
C.[time_series_length],
C.[time_series_length_bucket],
C.[time_series_length_in_years],
C.[training_length_bucket],
C.[training_length_in_years],
C.[variance],
C.[variance_larger_than_standard_deviation],
C.[model_better],
C.[best_model_wfa],
C.[benchmark_wfa],
C.[best_model],
C.[holdout_best_model_wfa_bucket],
C.[winning_model_wfa],
C.[month],
C.[month_date] as apollo_month_date,
@xyz_run_date as xyz_run_date

INTO #dataset

FROM #MATLOC as B

LEFT JOIN #apollorun as C ON 

(
	UPPER(B.Material) = UPPER(C.material)
	AND UPPER(B.Plant) = UPPER(C.location)
	AND B.Month_Date = C.month_date
	AND UPPER(B.Sales_Organization) = UPPER(C.sales_org)
	AND UPPER(B.Distribution_Channel) = UPPER(C.distribution_channel)
)


------------------------------------------------------------------------------------------------------
-- 5. JOIN APOLLO/SOH TABLE WITH PRODUCT CATEGORY
------------------------------------------------------------------------------------------------------

SELECT

A.*,
B.PRODUCT_CATEGORY as product_category,
B.DIVISION as division

INTO #final_dataset 

FROM #dataset as A

LEFT JOIN mstr.HT_MATERIAL_PLANT_MASTER as B ON

(
	UPPER(A.material) = UPPER(B.MATL) AND
	UPPER(A.location) = UPPER(B.Plant)
)

------------------------------------------------------------------------------------------------------
-- 6. PUT JOINED APOLLO AND SOH TABLE INTO XYZ_BASE
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS apollo.XYZ_BASE

SELECT * INTO apollo.XYZ_BASE from #final_dataset

