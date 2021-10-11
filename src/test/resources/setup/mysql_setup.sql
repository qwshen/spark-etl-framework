GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'hadoop' WITH GRANT OPTION;
FLUSH PRIVILEGES;

-- create database if it doesn't exists
CREATE DATABASE IF NOT EXISTS events;

-- CREATE TABLE train
DROP TABLE IF EXISTS events.train;
-- create
CREATE TABLE IF NOT EXISTS events.train(
	train_id int AUTO_INCREMENT,
	user varchar(32),
	event varchar(32),
	invited int,
	timestamp varchar(32),
	interested int,
	not_interested int,
	PRIMARY KEY(train_id)
);

-- CREATE TABLE users
DROP TABLE IF EXISTS events.users;
-- create
CREATE TABLE IF NOT EXISTS events.users(
	user_id varchar(32),
	birthyear int,
	gender varchar(8),
	locale varchar(8),
	location varchar(128),
	timezone varchar(8),
	joinedAt varchar(32),
	PRIMARY KEY(user_id)
);

-- CREATE TABLE features
DROP TABLE IF EXISTS events.features;
-- create
CREATE TABLE IF NOT EXISTS events.features(
  user_id varchar(32),
  gender varchar(8),
  birthyear int,
  timestamp varchar(32),
  invited int,
  interested int,
  process_date varchar(16),
  PRIMARY KEY(user_id, process_date)
);

-- CREATE TABLE user_states
DROP TABLE IF EXISTS events.user_states;
-- create
CREATE TABLE IF NOT EXISTS events.user_states(
  state_id int AUTO_INCREMENT,
  gender varchar(8),
  interested int,
  minage int,
  maxage int,
  averageage float,
  start timestamp,
  end timestamp,
  PRIMARY KEY(state_id)
);

