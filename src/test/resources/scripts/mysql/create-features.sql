create table features(
  user_id varchar(32),
  timestamp varchar(32),
  gender varchar(8),
  birthyear int,
  interested int,
  process_date varchar(24),
  primary key(user_id, timestamp)
)
