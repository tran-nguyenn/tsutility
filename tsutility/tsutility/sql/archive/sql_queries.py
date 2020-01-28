matloc_xyz = """
SELECT * FROM apollo.XYZ_BASE
"""

product_query = """
SELECT 
      MATL as material,
      PLANT as location,
      PRODUCT_CATEGORY as product_category, 
      DIVISION as division
      FROM mstr.HT_MATERIAL_PLANT_MASTER
""" 

matloc_all_sales_org_xyz = """
SELECT * FROM apollo.XYZ_ALL
"""

matloc_food_xyz = """
SELECT * FROM apollo.XYZ_FOOD
"""

# query to make matloc anc run_id 190691871 (run it in sql)
procedure_query = """
drop table if exists #MATLOC
SELECT
     Material,
     Plant,
     Month_Date,
     Sales_Organization,
     Forecast_Year,
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
			 FORECAST_LAG = '0'
             AND
             Forecast_Year != '2016'
     ) MATLOC
     GROUP BY
     Material, Plant, Sales_Organization, Distribution_Channel, Forecast_Year, Forecast_Month, Month_Date

drop table if exists #apollorun
SELECT A.*
INTO #apollorun
FROM (
 SELECT
   AT.*,
   AR.run_id as run_id_ar,
   AT.run_id as run_id_at,
   AR.RUN_DATE as run_date_ar,
   AT.RUN_DATE as run_date_at,
   UPPER(AR.sales_org) as Sales_Organization,
   UPPER(AR.material) as material_ar,
   UPPER(AT.material) as material_at,
   AR.month,
   DATEADD(mm, DATEDIFF(mm, 0, AR.month), 0) as month_date,
   AR.model_better,
   AR.best_model_wfa,
   AR.benchmark_wfa,
   AR.best_model,
   AR.holdout_best_model_wfa_bucket,
   AR.winning_model_wfa
 FROM apollo_archive.ORDER_MATERIAL_LOCATION_TIME_SERIES_ATTRIBUTES_TMP as AT
   JOIN apollo_archive.ORDER_MATERIAL_LOCATION_MODEL_RESULT as AR
   ON
   AT.material = AR.material
   AND
   AT.location = AR.location
   WHERE
   AR.run_id = '190691871'
   AND AT.run_id = '190691871'
) A

drop table if exists #dataset
SELECT
     B.SUM_UNCLEAN_SOH,
     B.SUM_CLEAN_SOH,
     B.Forecast_Year,
     C.*
INTO #dataset
FROM #MATLOC as B
LEFT JOIN #apollorun as C
ON
B.Material = C.material_ar
AND B.Plant = C.location
AND B.Month_Date = C.month_date
AND B.Sales_Organization = C.sales_org
AND B.Distribution_Channel = C.distribution_channel

drop table if exists apollo.XYZ_BASE
select * into apollo.XYZ_BASE from #dataset
"""