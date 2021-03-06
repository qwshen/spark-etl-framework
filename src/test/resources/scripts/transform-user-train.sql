set run_date = concat('${runActor}', ' at ', '${runTime}');
setrun train_count = (select count(*) from train);
setrun users_count = (select count(*) from users);
select distinct
  u.user_id,
  u.gender,
  cast(u.birthyear as int) as birthyear,
  t.timestamp,
  t.interested,
  concat(concat('${application.process_date}', '-', ${run_date}), '-', concat(cast(${train_count} as string), '-', cast(${users_count} as string))) as process_date,
  t.event as event_id
from train t
  left join users u on t.user = u.user_id