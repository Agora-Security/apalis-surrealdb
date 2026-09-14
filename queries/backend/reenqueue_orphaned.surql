BEGIN;
LET $live = SELECT VALUE instance FROM worker
    WHERE queue = $queue AND last_seen >= time::now() - duration::from_millis($dur);

LET $claimed = SELECT id, attempts, max_attempts FROM job
    WHERE queue = $queue
      AND status = 'Queued'
      AND lock_at != NONE
      AND lock_at <= time::now() - duration::from_millis($dur)
      AND (lock_by IS NONE OR lock_by NOTINSIDE $live);
LET $claimed_retriable = SELECT VALUE id FROM $claimed WHERE attempts < max_attempts;
LET $claimed_exhausted = SELECT VALUE id FROM $claimed WHERE attempts >= max_attempts;

LET $running = SELECT id, attempts + 1 AS spent, max_attempts FROM job
    WHERE queue = $queue
      AND status = 'Running'
      AND lock_at != NONE
      AND lock_at <= time::now() - duration::from_millis($dur)
      AND (lock_by IS NONE OR lock_by NOTINSIDE $live);
LET $running_retriable = SELECT VALUE id FROM $running WHERE spent < max_attempts;
LET $running_exhausted = SELECT VALUE id FROM $running WHERE spent >= max_attempts;

UPDATE $claimed_retriable SET
    status = 'Pending', lock_by = NONE, lock_at = NONE, done_at = NONE;

UPDATE $claimed_exhausted SET
    status = 'Killed', lock_by = NONE, lock_at = NONE, done_at = time::now(),
    last_result = { Err: 'Killed: claimed by a worker that stopped reporting, with no attempts left.' };

UPDATE $running_retriable SET
    status = 'Pending', attempts = attempts + 1,
    lock_by = NONE, lock_at = NONE, done_at = NONE, run_at = time::now(),
    last_result = { Err: 'Re-enqueued: the worker running this task is no longer live.' };

UPDATE $running_exhausted SET
    status = 'Killed', attempts = attempts + 1,
    lock_by = NONE, lock_at = NONE, done_at = time::now(),
    last_result = { Err: 'Killed: the worker running this task stopped reporting and no attempts remain.' };

DELETE worker WHERE queue = $queue AND last_seen < time::now() - duration::from_millis($reap);
COMMIT;
