// server.js (PostgreSQL API Server Logic)

import express from 'express';
import { Pool } from 'pg';
import bodyParser from 'body-parser';

// --- CONFIGURATION ---
const PORT = process.env.PORT || 3000;

// 🔥 Конфигурация PostgreSQL
const PG_CONFIG = {
    user: process.env.PG_USER || 'your_user',
    host: process.env.PG_HOST || 'localhost',
    database: process.env.PG_DATABASE || 'your_db',
    password: process.env.PG_PASSWORD || 'your_password',
    port: process.env.PG_PORT || 5432,
    max: 20, // Максимальное количество соединений в пуле
    idleTimeoutMillis: 30000,
};

// --- INIT ---
const app = express();
// Создание пула соединений
const pool = new Pool(PG_CONFIG);

app.use(bodyParser.json());

// Проверка соединения с БД
pool.on('connect', () => {
    console.log('[PG] Connected to PostgreSQL.');
});
pool.on('error', (err) => {
    console.error('[PG ERROR] Unexpected error on idle client', err);
    process.exit(1);
});

// ------------------------------------------------------------------
// 1. АТОМАРНАЯ ЛОГИКА ВЫДАЧИ JOB ID (Транзакция)
// ------------------------------------------------------------------

/**
 * Атомарно выдает один Job ID, используя транзакцию SELECT FOR UPDATE SKIP LOCKED.
 * Это гарантирует, что два одновременных запроса не получат один и тот же ID.
 * * @returns {string | null} Выданный Job ID или null.
 */
async function issueJobIdAtomic() {
    let client;
    let jobId = null;
    
    try {
        client = await pool.connect();
        await client.query('BEGIN'); // 🔥 Шаг 1: Начинаем транзакцию

        // 🔥 Шаг 2: Найти свободный ID и ЗАБЛОКИРОВАТЬ его строку (SELECT FOR UPDATE)
        // SKIP LOCKED: Позволяет другим запросам не ждать, если строка уже заблокирована,
        // а сразу переходить к следующей свободной.
        const selectResult = await client.query(
            `SELECT job_id 
             FROM jobs 
             WHERE status = 'available' 
             LIMIT 1 
             FOR UPDATE SKIP LOCKED;`
        );

        if (selectResult.rows.length === 0) {
            await client.query('ROLLBACK'); // Ничего не нашли, откатываем
            return null;
        }

        jobId = selectResult.rows[0].job_id;

        // 🔥 Шаг 3: Пометить ID как "issued" (выданный)
        await client.query(
            `UPDATE jobs 
             SET status = 'issued', issued_at = NOW() 
             WHERE job_id = $1;`,
            [jobId]
        );

        await client.query('COMMIT'); // 🔥 Шаг 4: Фиксируем транзакцию (изменения становятся постоянными)
        
        console.log(`[GET] Successfully issued atomic jobId: ${jobId}`);
        return jobId;

    } catch (error) {
        console.error(`[CRITICAL PG ERROR] Failed to issue job ID. Rolling back.`, error.message);
        if (client) await client.query('ROLLBACK'); // Откат в случае любой ошибки
        return null;
    } finally {
        if (client) client.release(); // Возвращаем соединение в пул
    }
}

// ------------------------------------------------------------------
// 2. ЛОГИКА ПРИЕМА JOB ID ОТ СКАНЕРА (SADD для PostgreSQL)
// ------------------------------------------------------------------

/**
 * Просто обновляет статус Job ID на "completed" (завершен).
 * Предполагается, что IDs уже были приняты сканером.
 */
async function submitJobIds(jobIds) {
    if (!jobIds || jobIds.length === 0) return { affected: 0 };
    
    try {
        // UNNEST - функция, которая "разворачивает" массив jobIds в список строк
        const updateResult = await pool.query(
            `UPDATE jobs 
             SET status = 'completed' 
             WHERE job_id = ANY($1::varchar[]) 
             AND status = 'issued';`,
            [jobIds]
        );
        
        const affectedCount = updateResult.rowCount;
        return { affected: affectedCount };
    } catch (error) {
        console.error('[SUBMIT ERROR] Failed to update job statuses:', error);
        return { affected: 0 };
    }
}


// ------------------------------------------------------------------
// 3. МАРШРУТЫ API
// ------------------------------------------------------------------

// Маршрут для выдачи Job ID
app.get('/api/get_job_id', async (req, res) => {
    const jobId = await issueJobIdAtomic();
    if (jobId) {
        res.json({ jobId: jobId });
    } else {
        res.status(404).json({ error: 'No available Job IDs' });
    }
});

// Маршрут для приема Job ID
app.post('/api/submit_job_ids', async (req, res) => {
    const jobIds = req.body.job_ids;
    if (!jobIds || !Array.isArray(jobIds)) {
        return res.status(400).json({ error: 'Invalid or missing job_ids array' });
    }
    
    const result = await submitJobIds(jobIds);
    res.json(result);
});

// ------------------------------------------------------------------
// 4. ЗАПУСК СЕРВЕРА
// ------------------------------------------------------------------

app.listen(PORT, () => {
    console.log(`\n--- Server running on port ${PORT} ---`);
    console.log(`Using PostgreSQL at ${PG_CONFIG.host}:${PG_CONFIG.port}`);
});
