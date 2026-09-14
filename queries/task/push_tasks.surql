BEGIN;
FOR $task IN $tasks {
    IF $task.dedupe_id IS NONE {
        IF count((SELECT VALUE id FROM $task.id)) = 0 {
            CREATE $task.id CONTENT {
                args: $task.args,
                queue: $task.queue,
                status: 'Pending',
                attempts: 0,
                max_attempts: $task.max_attempts,
                run_at: $task.run_at,
                priority: $task.priority,
                metadata: $task.metadata,
                idempotency_key: $task.idempotency_key
            };
        };
    } ELSE IF count((SELECT VALUE id FROM $task.dedupe_id)) = 0 {
        CREATE $task.dedupe_id CONTENT { job: $task.id };
        IF count((SELECT VALUE id FROM $task.id)) = 0 {
            CREATE $task.id CONTENT {
                args: $task.args,
                queue: $task.queue,
                status: 'Pending',
                attempts: 0,
                max_attempts: $task.max_attempts,
                run_at: $task.run_at,
                priority: $task.priority,
                metadata: $task.metadata,
                idempotency_key: $task.idempotency_key
            };
        };
    };
};
COMMIT;
