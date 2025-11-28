const express = require('express');
const bodyParser = require('body-parser');
const { Pool } = require('pg'); 
const app = express();
const PORT = process.env.PORT || 3000;

// ==========================================================
// КОНФИГУРАЦИЯ
// ==========================================================
// Строка подключения к базе данных PostgreSQL (берется из переменных окружения хостинга)
const DATABASE_URL = process.env.DATABASE_URL; 

// 🔥 Job ID считается действительным только в течение 1 часа.
const JOB_ID_TTL_HOURS = 1; 
const TABLE_NAME = 'job_ids';

// 🔥 Инициализация пула PostgreSQL
if (!DATABASE_URL) {
    console.error("FATAL: DATABASE_URL is not set. Cannot connect to PostgreSQL.");
    process.exit(1);
}
const pool = new Pool({
    connectionString: DATABASE_URL,
    // Настройки SSL/ConnectionString
});

app.use(bodyParser.json());

// ------------------------------------------------------------
// Инициализация базы данных и таблиц
// ------------------------------------------------------------
async function initDb() {
    try {
        // 1. Убеждаемся, что таблица существует
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
                job_id VARCHAR(50) PRIMARY KEY, -- PRIMARY KEY гарантирует, что job_id уникален
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                checked_at TIMESTAMP WITH TIME ZONE
            );
        `);
        
        // 2. Патч для добавления колонки 'timestamp', если она отсутствовала
        try {
             await pool.query(`
                ALTER TABLE ${TABLE_NAME} ADD COLUMN timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
             `);
             console.log(`[DB PATCH] Successfully added column 'timestamp' to existing table.`);
        } catch (e) {
            if (e.code !== '42701') {
                 console.warn(`[DB PATCH] Column 'timestamp' already existed or failed with non-fatal code: ${e.code}`);
            }
        }
        
        // 3. Создаем индекс для быстрого поиска по времени
        await pool.query(`
            CREATE INDEX IF NOT EXISTS idx_timestamp ON ${TABLE_NAME} (timestamp);
        `);
        
        console.log(`[INIT] PostgreSQL table '${TABLE_NAME}' ensured and ready.`);
    } catch (error) {
        console.error("[ERROR] Failed to initialize database:", error);
        process.exit(1);
    }
}

// ------------------------------------------------------------
// API Эндпоинты
// ------------------------------------------------------------

/** ➡️ Эндпоинт для приема Job ID от коллектора. 🔥 ОБНОВЛЯЕТ ДУБЛИКАТЫ (Сброс TTL). */
app.post('/api/submit_job_ids', async (req, res) => {
    const newJobIds = req.body.job_ids;
    if (!Array.isArray(newJobIds) || newJobIds.length === 0) {
        return res.status(400).json({ error: "job_ids array is required" });
    }

    const values = newJobIds
        .filter(id => typeof id === 'string' && id.length > 5)
        .map(id => `('${id}', NOW())`)
        .join(',');

    if (!values) {
        return res.json({ ok: true, affected: 0, total: 0 });
    }

    try {
        const query = `
            INSERT INTO ${TABLE_NAME} (job_id, timestamp) 
            VALUES ${values}
            -- 🔥 ПРИ КОНФЛИКТЕ ОБНОВИТЬ ВРЕМЯ (TTL сброшен и Job ID становится "свежим")
            ON CONFLICT (job_id) DO UPDATE SET timestamp = EXCLUDED.timestamp;
        `;
        
        const result = await pool.query(query);
        const affectedCount = result.rowCount; 

        const totalResult = await pool.query(`SELECT COUNT(*) FROM ${TABLE_NAME}`);
        const totalCount = parseInt(totalResult.rows[0].count, 10);
        
        console.log(`[SUBMIT] Affected ${affectedCount} IDs (Inserted/Updated). Total: ${totalCount}`);
        res.json({ ok: true, affected: affectedCount, total: totalCount });

    } catch (error) {
        console.error("[DB SUBMIT ERROR]:", error);
        res.status(500).json({ error: "Database error during submission." });
    }
});

/** ⬅️ Эндпоинт для выдачи самого старого ID с TTL=1 час. 🔥 ИСПОЛЬЗУЕТ ТРАНЗАКЦИИ. */
app.get('/api/get_job_id', async (req, res) => {
    // 1. Получаем клиента из пула для транзакции
    const client = await pool.connect(); 
    try {
        await client.query('BEGIN'); // 🚀 НАЧАЛО АТОМАРНОЙ ТРАНЗАКЦИИ

        const expiryDate = new Date(Date.now() - JOB_ID_TTL_HOURS * 3600 * 1000).toISOString();

        // 2. Ищем и БЛОКИРУЕМ самый старый ID
        const queryResult = await client.query(`
            SELECT job_id
            FROM ${TABLE_NAME}
            WHERE timestamp > $1
            ORDER BY timestamp ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED; -- Блокировка и пропуск уже заблокированных
        `, [expiryDate]);

        const item = queryResult.rows[0];

        if (!item) {
            // Нет свежих ID. Удаляем устаревшие и сообщаем об ошибке.
            await client.query(`DELETE FROM ${TABLE_NAME} WHERE timestamp <= $1`, [expiryDate]);
            await client.query('COMMIT'); // Завершаем транзакцию
            return res.status(404).json({ error: "Queue is empty or all IDs have expired (TTL 1h)." });
        }

        const jobId = item.job_id;

        // 3. ID найден. Удаляем его (внутри транзакции).
        await client.query(`DELETE FROM ${TABLE_NAME} WHERE job_id = $1`, [jobId]);
        
        await client.query('COMMIT'); // 🚀 ФИНАЛИЗАЦИЯ ТРАНЗАКЦИИ: Select и Delete выполнены атомарно!
        
        // 4. Считаем оставшиеся (Вне транзакции, чтобы не держать клиента пула долго)
        const totalResult = await pool.query(`SELECT COUNT(*) FROM ${TABLE_NAME}`);
        const remaining = parseInt(totalResult.rows[0].count, 10);
        
        console.log(`[GET] Issued TTL-valid JobID: ${jobId}. Remaining: ${remaining}`);
        return res.json({ ok: true, job_id: jobId });
        
    } catch (error) {
        await client.query('ROLLBACK'); // Откатываем все изменения в случае ошибки
        console.error("[DB GET ERROR]:", error);
        res.status(500).json({ error: "Database error during retrieval." });
    } finally {
        client.release(); // Обязательно возвращаем клиента в пул
    }
});

// ------------------------------------------------------------
// Запуск
// ------------------------------------------------------------
(async () => {
    await initDb();
    app.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
    });
})();
