DELETE FROM sttgaz.aux_mdaudit_regions;

INSERT INTO sttgaz.aux_mdaudit_regions(region_id, region_name)
SELECT DISTINCT region_id, region_name
FROM sttgaz.stage_mdaudit_checks;