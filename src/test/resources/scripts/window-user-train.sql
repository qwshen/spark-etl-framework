select
  u.user_id as userId,
  nvl(u.gender, 'unknown') as gender,
  (2020 - cast(u.birthyear as int)) as age,
  cast(t.invited as int) as invited,
  cast(t.interested as int) as interested,
  '${process_date}' as processDate,
  t.__timestamp as timeStamp,
  window(t.__timestamp, '10 seconds').start as windowStart,
  window(t.__timestamp, '10 seconds').end as windowEnd
from train t
  left join users u on t.user = u.user_id
