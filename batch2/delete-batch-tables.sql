DROP TABLE IF EXISTS `batch-attributes`;
DROP TABLE IF EXISTS `jobs-parents`;
DROP TABLE IF EXISTS `jobs`;
DROP TABLE IF EXISTS `batch`;
DROP TABLE IF EXISTS `instances`;
DROP PROCEDURE IF EXISTS activate_instance;
DROP PROCEDURE IF EXISTS deactivate_instance;
DROP PROCEDURE IF EXISTS mark_instance_deleted;
DROP PROCEDURE IF EXISTS close_batch;
DROP PROCEDURE IF EXISTS schedule_job;
DROP PROCEDURE IF EXISTS unschedule_job;
DROP PROCEDURE IF EXISTS mark_job_complete;
