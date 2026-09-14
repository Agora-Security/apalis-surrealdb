UPDATE $id SET
    status = $status,
    attempts = $attempts,
    last_result = $result,
    lock_by = NONE,
    lock_at = NONE,
    done_at = time::now()
    WHERE lock_by = $worker
    RETURN AFTER;
