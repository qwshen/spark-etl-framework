set run_date = concat('${runActor}', ' at ', /* the run time */'${runTime}');
setrun train_count = (select /*+ test hint */count(*) from train);
setrun users_count = (select /*+ test hint */ count(*) from users);
-- set unknown_gender = user__gender('unknown');
--set genders = (select distinct gender, user__gender(gender) as ex_gender from users);
-- setrun max_gender = (select max(user__gender(gender)) from users);
select distinct
  u.user_id,
  u.gender,
  -- ${unknown_gender} as unknown_gender, -- add new field
  -- g.ex_gender,
  cast(u.birthyear as int) as birthyear,
  t.timestamp,
  t.interested,
  concat(concat('${application.process_date}', '-', ${run_date}), '-', concat(cast(${train_count} as string), '-', cast(${users_count} as string))) as process_date,
  t.event as event_id
from train t
  -- left join ${genders} g on u.gender = g.gender
  left join users u on t.user = u.user_id
