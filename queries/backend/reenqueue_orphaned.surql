LET $live = SELECT VALUE instance FROM worker
    WHERE queue = $queue AND last_seen >= time::now() - duration::from_millis($dur);
LET $orphaned = SELECT id, attempts + 1 AS spent, max_attempts FROM job
    WHERE queue = $queue
      AND lock_at != NONE
      AND lock_at <= time::now() - duration::from_millis($dur)
      AND ((status IN ['Queued', 'Running'] AND lock_by != NONE AND lock_by NOTINSIDE $live)
           OR (status = 'Queued' AND lock_by IS NONE));
-- the orphaned run itself spends an attempt, so max_attempts is what caps reclaims
LET $exhausted = SELECT VALUE id FROM $orphaned WHERE spent >= max_attempts;
LET $retriable = SELECT VALUE id FROM $orphaned WHERE spent < max_attempts;
UPDATE $exhausted SET
    status = 'Killed',
    attempts = attempts + 1,
    lock_by = NONE,
    lock_at = NONE,
    done_at = time::now(),
    last_result = { Err: 'Killed: orphaned by a dead worker with no attempts left.' };
UPDATE $retriable SET
    status = 'Pending',
    attempts = attempts + 1,
    lock_by = NONE,
    lock_at = NONE,
    done_at = NONE,
    last_result = { Err: 'Re-enqueued: locking worker instance is no longer live.' };

DELETE worker WHERE queue = $queue
    AND last_seen < time::now() - duration::from_millis($dur * 4);
