INSERT INTO apollo.XYZ_ANC_FINAL
SELECT

	a.[Matl Type] as matl_type,
    a.[PS MatlStatus] as ps_matl_status,
    a.[Valid from] as valid_from,
    a.Created as created,
	a.material_description,
	a.material,
	a.location,
    a.XYZ_ind,
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
	CASE
    WHEN b.descovXYZ is null then 'Z'
    WHEN b.descovXYZ = 'None' then 'Z'
    else b.descovXYZ end as XYZ

FROM apollo.XYZ_ANC_REFERENCE as a LEFT JOIN apollo.XYZ_ANC as b
ON a.material = b.material and a.location = b.location
UNION ALL
SELECT

	a.[Matl Type] as matl_type,
    a.[PS MatlStatus] as ps_matl_status,
    a.[Valid from] as valid_from,
    a.Created as created,
	a.material_description,
	b.material,
	b.location,
    a.XYZ_ind,
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
	CASE
    WHEN b.descovXYZ is null then 'Z'
    WHEN b.descovXYZ = 'None' then 'Z'
    else b.descovXYZ end as XYZ

FROM apollo.XYZ_ANC_REFERENCE as a RIGHT JOIN apollo.XYZ_ANC as b
ON a.material = b.material and a.location = b.location
WHERE a.[Matl Type] is Null
