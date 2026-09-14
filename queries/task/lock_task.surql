UPDATE $id SET status = 'Running', lock_at = time::now(), lock_by = $worker, done_at = NONE
    WHERE (lock_by IS NONE OR lock_by = $worker)
      AND status IN ['Queued', 'Pending', 'Failed', 'Running']
    RETURN AFTER;
