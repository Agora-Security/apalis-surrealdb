DELETE job
    WHERE queue = $queue
      AND (status IN ['Done', 'Killed'] OR (status = 'Failed' AND attempts >= max_attempts))
      AND (done_at IS NONE OR done_at <= time::now() - $retain)
    RETURN BEFORE;
