DELIMITER $$

DROP PROCEDURE IF EXISTS schedule_job $$
CREATE PROCEDURE schedule_job(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100)
)
BEGIN
  DECLARE cur_job_state VARCHAR(40);
  DECLARE cur_cores_mcpu INT;
  DECLARE cur_job_cancel BOOLEAN;
  DECLARE cur_instance_state VARCHAR(40);
  DECLARE cur_attempt_id VARCHAR(40);
  DECLARE delta_cores_mcpu INT;
  DECLARE cur_instance_is_pool BOOLEAN;

  START TRANSACTION;

  SELECT state, cores_mcpu, attempt_id
  INTO cur_job_state, cur_cores_mcpu, cur_attempt_id
  FROM jobs
  WHERE batch_id = in_batch_id AND job_id = in_job_id
  FOR UPDATE;

  SELECT (jobs.cancelled OR batches.cancelled) AND NOT jobs.always_run
  INTO cur_job_cancel
  FROM jobs
  INNER JOIN batches ON batches.id = jobs.batch_id
  WHERE batch_id = in_batch_id AND job_id = in_job_id
  LOCK IN SHARE MODE;

  SELECT is_pool
  INTO cur_instance_is_pool
  FROM instances
  LEFT JOIN inst_colls ON instances.inst_coll = inst_colls.name
  WHERE instances.name = in_instance_name;

  CALL add_attempt(in_batch_id, in_job_id, in_attempt_id, in_instance_name, cur_cores_mcpu, delta_cores_mcpu);

  IF cur_instance_is_pool THEN
    IF delta_cores_mcpu = 0 THEN
      SET delta_cores_mcpu = cur_cores_mcpu;
    ELSE
      SET delta_cores_mcpu = 0;
    END IF;
  END IF;

  SELECT state INTO cur_instance_state FROM instances WHERE name = in_instance_name LOCK IN SHARE MODE;

  IF (cur_job_state = 'Ready' OR cur_job_state = 'Creating') AND NOT cur_job_cancel AND cur_instance_state = 'active' THEN
    UPDATE jobs SET state = 'Running', attempt_id = in_attempt_id WHERE batch_id = in_batch_id AND job_id = in_job_id;
    COMMIT;
    SELECT 0 as rc, in_instance_name, delta_cores_mcpu;
  ELSE
    COMMIT;
    SELECT 1 as rc,
      cur_job_state,
      cur_job_cancel,
      cur_instance_state,
      in_instance_name,
      cur_attempt_id,
      delta_cores_mcpu,
      'job not Ready or cancelled or instance not active, but attempt already exists' as message;
  END IF;
END $$

DROP PROCEDURE IF EXISTS mark_job_creating $$
CREATE PROCEDURE mark_job_creating(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100),
  IN new_start_time BIGINT
)
BEGIN
  DECLARE cur_job_state VARCHAR(40);
  DECLARE cur_job_cancel BOOLEAN;
  DECLARE cur_cores_mcpu INT;
  DECLARE cur_instance_state VARCHAR(40);
  DECLARE delta_cores_mcpu INT;

  START TRANSACTION;

  SELECT state, cores_mcpu
  INTO cur_job_state, cur_cores_mcpu
  FROM jobs
  WHERE batch_id = in_batch_id AND job_id = in_job_id
  FOR UPDATE;

  SELECT (jobs.cancelled OR batches.cancelled) AND NOT jobs.always_run
  INTO cur_job_cancel
  FROM jobs
  INNER JOIN batches ON batches.id = jobs.batch_id
  WHERE batch_id = in_batch_id AND job_id = in_job_id
  LOCK IN SHARE MODE;

  CALL add_attempt(in_batch_id, in_job_id, in_attempt_id, in_instance_name, cur_cores_mcpu, delta_cores_mcpu);

  UPDATE attempts SET start_time = new_start_time
  WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id;

  SELECT state INTO cur_instance_state FROM instances WHERE name = in_instance_name LOCK IN SHARE MODE;

  IF cur_job_state = 'Ready' AND NOT cur_job_cancel AND cur_instance_state = 'pending' THEN
    UPDATE jobs SET state = 'Creating', attempt_id = in_attempt_id WHERE batch_id = in_batch_id AND job_id = in_job_id;
  END IF;

  COMMIT;
  SELECT 0 as rc, delta_cores_mcpu;
END $$

DROP PROCEDURE IF EXISTS mark_job_started $$
CREATE PROCEDURE mark_job_started(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100),
  IN new_start_time BIGINT
)
BEGIN
  DECLARE cur_job_state VARCHAR(40);
  DECLARE cur_job_cancel BOOLEAN;
  DECLARE cur_cores_mcpu INT;
  DECLARE cur_instance_state VARCHAR(40);
  DECLARE delta_cores_mcpu INT;

  START TRANSACTION;

  SELECT state, cores_mcpu
  INTO cur_job_state, cur_cores_mcpu
  FROM jobs
  WHERE batch_id = in_batch_id AND job_id = in_job_id
  FOR UPDATE;

  SELECT (jobs.cancelled OR batches.cancelled) AND NOT jobs.always_run
  INTO cur_job_cancel
  FROM jobs
  INNER JOIN batches ON batches.id = jobs.batch_id
  WHERE batch_id = in_batch_id AND job_id = in_job_id
  LOCK IN SHARE MODE;

  CALL add_attempt(in_batch_id, in_job_id, in_attempt_id, in_instance_name, cur_cores_mcpu, delta_cores_mcpu);

  UPDATE attempts SET start_time = new_start_time
  WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id;

  SELECT state INTO cur_instance_state FROM instances WHERE name = in_instance_name LOCK IN SHARE MODE;

  IF cur_job_state = 'Ready' AND NOT cur_job_cancel AND cur_instance_state = 'active' THEN
    UPDATE jobs SET state = 'Running', attempt_id = in_attempt_id WHERE batch_id = in_batch_id AND job_id = in_job_id;
  END IF;

  COMMIT;
  SELECT 0 as rc, delta_cores_mcpu;
END $$

DELIMITER ;
