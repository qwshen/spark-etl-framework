create table train(
  user varchar(12),
  event bigint(8),
  timestamp varchar(36),
  interested int,
  primary key (user, event)
)