-- name: CreateJob :exec
INSERT INTO jobs (job_id, status, s3_input_url)
VALUES ($1, $2, $3);

-- name: UpdateJobStatus :exec
UPDATE jobs 
SET status = $1, s3_output_url = $2 
WHERE job_id = $3;

-- name: GetJob :one
SELECT job_id, status, s3_input_url, s3_output_url, created_at
FROM jobs
WHERE job_id = $1;

-- name: ListStuckJobs :many
SELECT job_id, s3_input_url
FROM jobs
WHERE status = 'pending'
  AND created_at < CURRENT_TIMESTAMP - make_interval(secs => sqlc.arg(older_than_secs)::float8)
ORDER BY created_at
LIMIT sqlc.arg(max_rows)::int;
