insert into features(user_id, gender, birthyear, timestamp, interested, process_date)
    values(@user_id, @gender, @birthyear, @timestamp, @interested, @process_date)
on duplicate key update gender = @gender, birthyear = @birthyear, interested = @interested, process_date = @process_date
