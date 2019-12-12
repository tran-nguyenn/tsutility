DECLARE @ancMatLoc = "fcst.ancCleanRawMATLOC";
DECLARE @ancMatLocCust = "fcst.ancCleanRawMATLOCCUST";
DECLARE @product = "mstr.HT_MATERIAL_PLANT_MASTER";

SELECT
      PLANT as PLANT_CODE,
      MATL as MATERIAL_CODE,
      PRODUCT_CATEGORY,
      DIVISION
      FROM product
