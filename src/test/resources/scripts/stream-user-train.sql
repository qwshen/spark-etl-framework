select
  u.user_id,
  u.gender,
  cast(u.birthyear as int) as birthyear,
  t.timestamp,
  cast(t.invited as int) as invited,
  cast(t.interested as int) as interested,
  '${process_date}' as process_date
from train t
  left join users u on t.user = u.user_id and u.__timestamp >= t.__timestamp - interval 60 seconds and u.__timestamp <= t.__timestamp + interval 60 seconds

