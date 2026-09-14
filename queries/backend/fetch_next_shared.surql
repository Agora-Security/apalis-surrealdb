BEGIN;
LET $now = time::now();
LET $retry = SELECT VALUE id FROM job
    WHERE queue IN $queues AND status = 'Failed' AND run_at <= $now
      AND lock_by IS NONE AND attempts < max_attempts
    ORDER BY priority DESC, run_at ASC, id ASC LIMIT $limit;
LET $fresh = SELECT VALUE id FROM job
    WHERE queue IN $queues AND status = 'Pending' AND run_at <= $now
      AND lock_by IS NONE AND attempts < max_attempts
    ORDER BY priority DESC, run_at ASC, id ASC LIMIT $limit;
LET $due = array::slice(array::union($retry, $fresh), 0, $limit);
UPDATE $due SET status = 'Queued', lock_at = time::now(), done_at = NONE
    WHERE status IN ['Pending', 'Failed'] AND lock_by IS NONE
    RETURN AFTER;
COMMIT;
