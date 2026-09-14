UPDATE $id SET
    status = 'Failed',
    attempts = $attempts,
    last_result = $result,
    lock_by = NONE,
    lock_at = NONE,
    done_at = NONE,
    run_at = time::now() + duration::from_millis($backoff)
    WHERE lock_by = $worker
    RETURN AFTER;
