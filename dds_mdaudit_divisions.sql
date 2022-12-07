DELETE FROM sttgaz.dds_mdaudit_divisions;

INSERT INTO sttgaz.dds_mdaudit_divisions (division_id, division_name)
SELECT DISTINCT division_id, division_name
FROM sttgaz.stage_mdaudit_checks;