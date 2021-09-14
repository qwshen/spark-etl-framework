with raw_train as (
  select
    cast(value as string) as value,
    __timestamp
  from train
),
t_train as (
  select
    substr(value, 1, 9) as user,
    substr(value, 10, 10) as event,
    substr(value, 20, 32) as timestamp,
    substr(value, 52, 1) as interested,
    __timestamp
  from raw_train
)
select
  u.user_id,
  u.gender,
  cast(u.birthyear as int) as birthyear,
  t.timestamp,
  cast(t.interested as int) as interested,
  '${process_date}' as process_date
from t_train t
  left join users u on t.user = u.user_id and u.__timestamp >= t.__timestamp - interval 60 seconds and u.__timestamp <= t.__timestamp + interval 60 seconds

