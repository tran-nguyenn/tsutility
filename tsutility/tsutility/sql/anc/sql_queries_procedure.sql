-- ====TO DO====

-- ====Contents====
--1. SET PARAMETERS
--2. SOH TABLE
--3. PUT JOINED APOLLO AND SOH TABLE INTO XYZ_BASE

------------------------------------------------------------------------------------------------------
-- 1. SET PARAMETERS
------------------------------------------------------------------------------------------------------



DECLARE @xyz_run_date AS varchar(255) = '2020-01-24';

--DECLARE @run_id_filter varchar(255) = '190691871';

------------------------------------------------------------------------------------------------------
-- 2. SOH TABLE
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #MATLOC

SELECT

Material as material,
Plant as location,
Month_Date as month_date,
Sales_Organization as sales_org,
Distribution_Channel as distribution_channel,
SUM(SALES_ORDER_HISTORY) as UNCLEAN_SOH,
SUM(CLEAN_SALES_ORDER_HISTORY) as CLEAN_SOH

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
-- 2. JOIN APOLLO/SOH TABLE WITH PRODUCT CATEGORY
------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #final_dataset

SELECT

A.*,
B.PRODUCT_CATEGORY as product_category,
B.DIVISION as division

INTO #final_dataset

FROM #MATLOC as A

LEFT JOIN mstr.HT_MATERIAL_PLANT_MASTER as B ON

(
	UPPER(A.material) = UPPER(B.MATL) AND
	UPPER(A.location) = UPPER(B.Plant)
)

select * from #final_dataset
------------------------------------------------------------------------------------------------------
-- 3. PUT JOINED APOLLO AND SOH TABLE INTO XYZ_BASE
------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS apollo.XYZ_BASE

SELECT * INTO apollo.XYZ_BASE from #final_dataset
