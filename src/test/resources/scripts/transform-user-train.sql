select distinct
  u.user_id,
  u.gender,
  cast(u.birthyear as int) as birthyear,
  t.timestamp,
  t.interested,
  '${process_date}' as process_date,
  t.event as event_id
from train t
  left join users u on t.user = u.user_id