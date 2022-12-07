DELETE FROM sttgaz.dds_mdaudit_templates;

INSERT INTO sttgaz.dds_mdaudit_templates (template_id, template_name)
SELECT DISTINCT template_id, template_name
FROM sttgaz.stage_mdaudit_checks;