# todo: split in files

DEFINE TABLE IF NOT EXISTS job SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS args ON job TYPE bytes;
DEFINE FIELD IF NOT EXISTS queue ON job TYPE string;
DEFINE FIELD IF NOT EXISTS status ON job TYPE string DEFAULT 'Pending'
    ASSERT $value INSIDE ['Pending', 'Queued', 'Running', 'Done', 'Failed', 'Killed'];
DEFINE FIELD IF NOT EXISTS attempts ON job TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS max_attempts ON job TYPE int DEFAULT 25;
DEFINE FIELD IF NOT EXISTS run_at ON job TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS last_result ON job TYPE option<object> FLEXIBLE;
DEFINE FIELD IF NOT EXISTS lock_at ON job TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS lock_by ON job TYPE option<string>;
DEFINE FIELD IF NOT EXISTS done_at ON job TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS priority ON job TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS metadata ON job TYPE option<object> FLEXIBLE;
DEFINE FIELD IF NOT EXISTS idempotency_key ON job TYPE option<string>;

DEFINE INDEX IF NOT EXISTS job_fetch ON job COLUMNS queue, status, run_at;
DEFINE INDEX IF NOT EXISTS job_lock_by ON job COLUMNS lock_by;
DEFINE INDEX IF NOT EXISTS job_done ON job COLUMNS queue, done_at;
DEFINE INDEX IF NOT EXISTS job_idempotency ON job COLUMNS queue, idempotency_key;
DEFINE INDEX IF NOT EXISTS job_orphan ON job COLUMNS queue, status, lock_at;

DEFINE TABLE IF NOT EXISTS job_dedupe SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS job ON job_dedupe TYPE record<job>;
DEFINE FIELD IF NOT EXISTS created_at ON job_dedupe TYPE datetime DEFAULT time::now();

DEFINE TABLE IF NOT EXISTS worker SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS name ON worker TYPE string DEFAULT '';
DEFINE FIELD IF NOT EXISTS queue ON worker TYPE string;
DEFINE FIELD IF NOT EXISTS storage_name ON worker TYPE string;
DEFINE FIELD IF NOT EXISTS layers ON worker TYPE option<string>;
DEFINE FIELD IF NOT EXISTS instance ON worker TYPE string DEFAULT '';
DEFINE FIELD IF NOT EXISTS started_at ON worker TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS last_seen ON worker TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS worker_queue ON worker COLUMNS queue;
DEFINE INDEX IF NOT EXISTS worker_liveness ON worker COLUMNS queue, last_seen;

DEFINE TABLE IF NOT EXISTS apalis_meta SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS version ON apalis_meta TYPE int;
