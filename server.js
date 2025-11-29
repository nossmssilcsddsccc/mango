// server.js
import express from 'express';
import { Pool } from 'pg';
import bodyParser from 'body-parser';

const app = express();
const PORT = process.env.PORT || 3000;

// ВАЖНО: ВСТАВЬ СВОЮ СТРОКУ ПОДКЛЮЧЕНИЯ ИЗ NEON СЮДА!
const NEON_CONNECTION_STRING = "postgres://username:password@ep-xxxxxx.eu-central-1.aws.neon.tech/neondb?sslmode=require";

const pool = new Pool({
    connectionString: NEON_CONNECTION_STRING,
    ssl: {
        rejectUnauthorized: false
    },
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

app.use(bodyParser.json({ limit: '10mb' })); // на всякий случай, если сканер шлёт много ID

pool.on('connect', () => console.log('[PG] Connected to Neon PostgreSQL'));
pool.on('error', (err) => {
    console.error('[PG CRITICAL ERROR]', err);
    process.exit(1);
});

// АТОМАРНАЯ ВЫДАЧА JOB ID
async function issueJobIdAtomic() {
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const res = await client.query(`
            SELECT job_id FROM jobs
            WHERE status = 'available'
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        `);

        if (res.rows.length === 0) {
            await client.query('ROLLBACK');
            return null;
        }

        const jobId = res.rows[0].job_id;

        await client.query(`
            UPDATE jobs
            SET status = 'issued', issued_at = NOW()
            WHERE job_id = $1
        `, [jobId]);

        await client.query('COMMIT');
        console.log(`[GET] Issued Job ID: ${jobId}`);
        return jobId;

    } catch (err) {
        console.error('[ISSUE ERROR]', err.message);
        if (client) await client.query('ROLLBACK');
        return null;
    } finally {
        if (client) client.release();
    }
}

// ПРИЁМ НОВЫХ JOB ID ОТ СКАНЕРА
app.post('/api/submit_job_ids', async (req, res) => {
    const jobIds = req.body.job_ids;

    if (!Array.isArray(jobIds) || jobIds.length === 0) {
        return res.status(400).json({ error: 'job_ids must be non-empty array' });
    }

    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const result = await client.query(`
            INSERT INTO jobs (job_id, status)
            VALUES (UNNEST($1::varchar[]), 'available')
            ON CONFLICT (job_id) DO NOTHING
            RETURNING job_id
        `, [jobIds]);

        await client.query('COMMIT');

        const added = result.rowCount;
        console.log(`[SUBMIT] +${added} new Job IDs (total received: ${jobIds.length})`);

        res.json({
            added,
            total_received: jobIds.length,
            message: `${added} new IDs saved (duplicates ignored)`
        });

    } catch (err) {
        console.error('[SUBMIT ERROR]', err.message);
        if (client) await client.query('ROLLBACK');
        res.status(500).json({ error: 'Database error' });
    } finally {
        if (client) client.release();
    }
});

// ВЫДАЧА ОДНОГО JOB ID
app.get('/api/get_job_id', async (req, res) => {
    const jobId = await issueJobIdAtomic();
    if (jobId) {
        res.json({ jobId });
    } else {
        res.status(404).json({ error: 'No available Job IDs' });
    }
});

// Статус сервера
app.get('/', (req, res) => {
    res.json({ status: 'Roblox Job ID API is running!', time: new Date().toISOString() });
});

app.listen(PORT, () => {
    console.log(`\nAPI SERVER RUNNING`);
    console.log(`→ http://localhost:${PORT}`);
    console.log(`→ POST /api/submit_job_ids ← от сканера`);
    console.log(`→ GET /api/get_job_id    ← для ботов`);
});
