-- name: CreateJob :exec
INSERT INTO jobs (job_id, status, s3_input_url)
VALUES ($1, $2, $3);

-- name: UpdateJobStatus :exec
UPDATE jobs 
SET status = $1, s3_output_url = $2 
WHERE job_id = $3;

-- name: GetJobStatus :one
SELECT status 
FROM jobs 
WHERE job_id = $1;