// server.js
import express from 'express';
import { Pool } from 'pg';
import bodyParser from 'body-parser';

const app = express();
const PORT = process.env.PORT || 3000;

// Берём строку подключения ТОЛЬКО из секрета Render
const NEON_CONNECTION_STRING = process.env.NEON_CONNECTION_STRING;

if (!NEON_CONNECTION_STRING) {
    console.error("ОШИБКА: Добавь переменную NEON_CONNECTION_STRING в Render → Environment!");
    process.exit(1);
}

// Пул подключений к Neon
const pool = new Pool({
    connectionString: NEON_CONNECTION_STRING,
    ssl: { rejectUnauthorized: false },
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
});

app.use(bodyParser.json({ limit: '10mb' }));

pool.on('connect', () => console.log('[PG] Успешно подключено к Neon PostgreSQL'));
pool.on('error', (err) => {
    console.error('[PG КРИТИЧЕСКАЯ ОШИБКА]', err);
    process.exit(1);
});

// Атомарная выдача одного Job ID
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
            UPDATE jobs SET status = 'issued', issued_at = NOW()
            WHERE job_id = $1
        `, [jobId]);

        await client.query('COMMIT');
        console.log(`[ВЫДАНО] Job ID: ${jobId}`);
        return jobId;
    } catch (err) {
        console.error('[ОШИБКА ВЫДАЧИ]', err.message);
        if (client) await client.query('ROLLBACK');
        return null;
    } finally {
        if (client) client.release();
    }
}

// Приём новых Job ID от сканера
app.post('/api/submit_job_ids', async (req, res) => {
    const jobIds = req.body.job_ids;

    if (!Array.isArray(jobIds) || jobIds.length === 0) {
        return res.status(400).json({ error: 'job_ids должен быть массивом и не пустым' });
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

        console.log(`[ПРИНЯТО] +${added} новых Job ID (всего пришло: ${jobIds.length})`);

        res.json({
            added,
            total_received: jobIds.length,
            message: `${added} новых ID сохранено (дубли проигнорированы)`
        });

    } catch (err) {
        console.error('[ОШИБКА ПРИЁМА]', err.message);
        if (client) await client.query('ROLLBACK');
        res.status(500).json({ error: 'Ошибка базы данных' });
    } finally {
        if (client) client.release();
    }
});

// Выдача одного Job ID
app.get('/api/get_job_id', async (req, res) => {
    const jobId = await issueJobIdAtomic();
    if (jobId) {
        res.json({ jobId });
    } else {
        res.status(404).json({ error: 'Нет доступных Job ID' });
    }
});

// Главная страница — просто проверка
app.get('/', (req, res) => {
    res.json({ 
        status: 'Roblox Job ID API работает!', 
        time: new Date().toISOString(),
        endpoints: {
            "POST /api/submit_job_ids": "от сканера",
            "GET /api/get_job_id": "для ботов (по одному ID)"
        }
    });
});

app.listen(PORT, () => {
    console.log(`\nAPI ЗАПУЩЕН И ГОТОВ К БОЮ`);
    console.log(`http://localhost:${PORT}`);
    console.log(`POST → /api/submit_job_ids  ← сканер`);
    console.log(`GET  → /api/get_job_id     ← 400+ ботов каждые 5 сек`);
    console.log(`База: Neon (бесплатно навсегда)`);
});
