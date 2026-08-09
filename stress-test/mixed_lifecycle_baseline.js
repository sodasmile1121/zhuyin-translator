import http from 'k6/http';
import { sleep, check } from 'k6';

const profiles = {
    mixed: [
        { duration: '30s', target: 200 },   // 1. 溫和預熱 (Ramp-up)
        { duration: '2m', target: 1000 },   // 2. 衝擊尖峰 (Spike Peak)：拉長到 2 分鐘，塞爆 Queue
        { duration: '2m', target: 0 },      // 3. 冷卻與消化 (Drain Phase)：降為 0 VU，給 Worker 2 分鐘消化排隊
    ],
    soak: [
        { duration: '20s', target: 30 },
        { duration: '5m', target: 30 },
        { duration: '20s', target: 0 },
    ],
};

const testType = __ENV.TEST_TYPE || 'mixed';

export const options = {
    scenarios: {
        user_lifecycle: {
            executor: 'ramping-vus',
            exec: 'lifeCycle',
            stages: profiles[testType] || profiles.mixed,
            gracefulRampDown: '1m', // 擴大彈性，避免硬性截斷最後在排隊的 VU
        },
    },
};

const testPDF = open('./test.pdf', 'b');

export function lifeCycle() {
    const host = __ENV.TARGET_URL || 'http://localhost:8080';
    const uploadData = {
        files: http.file(
            testPDF,
            'k6_file.pdf',
            'application/pdf'
        ),
    };

    const uploadRes = http.post(`${host}/upload`, uploadData);

    if (!check(uploadRes, { 'upload success (200)': (r) => r.status === 200 })) {
        return;
    }

    let jobs;
    try {
        jobs = JSON.parse(uploadRes.body);
    } catch (err) {
        return;
    }

    if (!jobs || jobs.length === 0) return;

    const jobId = jobs[0].job_id;
    sleep(0.5);

    let isFinished = false;
    let isSuccess = false;
    const maxRetries = 120; // 配合兩分鐘的冷卻時間，放寬至 120 次 retry (2分鐘)
    let retries = 0;

    while (!isFinished && retries < maxRetries) {
        const statusRes = http.get(`${host}/status?job_id=${jobId}`);

        check(statusRes, {
            'status check success (200)': (r) => r.status === 200,
        });

        try {
            const statusData = JSON.parse(statusRes.body);

            if (statusData.status === 'success' || statusData.char_zy) {
                isFinished = true;
                isSuccess = true;
            } else if (statusData.status === 'failed') {
                isFinished = true;
                isSuccess = false;
            } else {
                retries++;
                sleep(1);
            }
        } catch (err) {
            retries++;
            sleep(1);
        }
    }

    check(isSuccess, {
        'job processing succeeded in worker': (s) => s === true,
    });

    if (isSuccess) {
        const downloadRes = http.get(`${host}/download?job_id=${jobId}`, {
            responseType: 'binary',
        });

        check(downloadRes, {
            'download success (200)': (r) => r.status === 200,
        });
    }

    sleep(1);
}