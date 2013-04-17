DROP DATABASE IF EXISTS ReqDB;
CREATE DATABASE ReqDB;

--
-- Must set passwords for database user by replacing "must_be_set".
--
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ReqDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ReqDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';

USE ReqDB;