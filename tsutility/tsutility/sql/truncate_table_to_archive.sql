INSERT INTO apollo_archive.XYZ SELECT a.* FROM apollo.XYZ_ANC_FINAL a
TRUNCATE TABLE apollo.XYZ_ANC_FINAL;