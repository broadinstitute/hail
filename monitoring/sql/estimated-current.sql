CREATE TABLE IF NOT EXISTS `monitoring_billing_mark` (
  `mark` BIGINT
) ENGINE = InnoDB;

INSERT INTO `monitoring_billing_mark` (mark) VALUES (NULL);

CREATE TABLE IF NOT EXISTS `monitoring_billing_data` (
  `year` INT NOT NULL,
  `month` INT NOT NULL,
  `service_description` VARCHAR(100) NOT NULL,
  `sku_description` VARCHAR(100) NOT NULL,
  `source` VARCHAR(40),
  `cost` DOUBLE NOT NULL,
  PRIMARY KEY (`year`, `month`, `service_description`, `sku_description`, `source`)
) ENGINE = InnoDB;
CREATE INDEX monitoring_billing_data_time_period ON `monitoring_billing_data` (`year`, `month`);
