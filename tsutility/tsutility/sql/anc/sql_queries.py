division = "'Appliances & Cookware'"

matloc_xyz = """
SELECT * FROM apollo.XYZ_BASE WHERE division = """ +  division

xyz_base_anc = """
SELECT
	A.*,
	B.PRODUCT_CATEGORY as product_category,
	B.DIVISION as division
FROM
(
	SELECT

	Material as material,
	Plant as location,
	Month_Date as month_date,
	Sales_Organization as sales_org,
	Distribution_Channel as distribution_channel,
	SUM(SALES_ORDER_HISTORY) as UNCLEAN_SOH,
	SUM(CLEAN_SALES_ORDER_HISTORY) as CLEAN_SOH

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
) as A

LEFT JOIN mstr.HT_MATERIAL_PLANT_MASTER as B ON
(
	UPPER(A.material) = UPPER(B.MATL) AND
	UPPER(A.location) = UPPER(B.Plant)
)

WHERE B.DIVISION = 'Appliances & Cookware'
"""

final_anc = """
INSERT INTO apollo.XYZ_ANC_FINAL

SELECT

	a.[Matl Type] as matl_type,
    a.[PS MatlStatus] as ps_matl_status,
    a.[Valid from] as valid_from,
    a.Created as created,
    a.XYZ_ind,
	b.sales_org,
	b.distribution_channel,
	b.division,
	b.product_category,
	b.winning_model_wfa,
	b.bm_wfa_bucket,
	b.coeff_of_variation,
	b.covXYZ,
	b.rawcovXYZ,
	b.descovXYZ,
	b.raw_cov,
	b.deseasonalized_cov,
	b.xyz_run_id,
    b.xyz_run_date,
	CASE WHEN b.descovXYZ is null then 'Z' else b.covXYZ end as XYZ

FROM apollo.XYZ_ANC_REFERENCE as a LEFT JOIN apollo.XYZ_ANC as b
ON a.material = b.material and a.location = b.location
"""

truncate_anc = """
INSERT INTO apollo_archive.XYZ SELECT a.* FROM apollo.XYZ_ANC_FINAL a
TRUNCATE TABLE apollo.XYZ_ANC_FINAL;
"""
