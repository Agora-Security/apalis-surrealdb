UPDATE $id SET
    status = 'Killed',
    last_result = { Err: $reason },
    done_at = time::now(),
    lock_by = NONE,
    lock_at = NONE
    WHERE status NOT IN ['Done', 'Killed']
    RETURN NONE;
