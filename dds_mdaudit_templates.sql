DELETE FROM sttgaz.aux_mdaudit_templates;

INSERT INTO sttgaz.aux_mdaudit_templates (template_id, template_name)
SELECT DISTINCT template_id, template_name
FROM sttgaz.stage_mdaudit_checks;