DECLARE @ancMatLoc = "fcst.ancCleanRawMATLOC";
DECLARE @ancMatLocCust = "fcst.ancCleanRawMATLOCCUST";
DECLARE @product = "mstr.HT_MATERIAL_PLANT_MASTER";

SELECT
      DISTRIBUTION_CHANNEL,
      StratCust,
      MATERIAL_CODE,
      PLANT_CODE,
      SALES_ORG,
      YEAR,
      MONTH,
      MONTH_DATE, 
      FORECAST_DATE,
      SUM(UNCLEAN_SOH) AS SUM_UNCLEAN_SOH,
      SUM(CLEAN_SOH) AS SUM_CLEAN_SOH
      FROM (
          SELECT
                DISTRIBUTION_CHANNEL,
                StratCust,
                MATERIAL_CODE,
                PLANT_CODE,
                SALES_ORG,
                FORECAST_DATE, format(FORECAST_DATE, 'MMMM') as MONTH,
                YEAR(FORECAST_DATE) as YEAR,
                DATEADD(mm, DATEDIFF(mm, 0, FORECAST_DATE) - 1, 0) as MONTH_DATE,
                UNCLEAN_SOH,
                CLEAN_SOH
                FROM ancMatLocCust
      ) MATLOCCUST
      GROUP BY
      DISTRIBUTION_CHANNEL, StratCust, MATERIAL_CODE, PLANT_CODE, SALES_ORG, YEAR, MONTH, MONTH_DATE, FORECAST_DATE