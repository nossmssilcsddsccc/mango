const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const { Pool } = require('pg'); 
const app = express();
const PORT = process.env.PORT || 3000;

// ==========================================================
// КОНФИГУРАЦИЯ
// ==========================================================
// URL для подключения к базе данных PostgreSQL.
const DATABASE_URL = process.env.DATABASE_URL; 

const ROBLOX_PLACE_ID = "109983668079237";
const ROBLOX_API_URL = 'https://games.roblox.com/v1/games/multiget-place-instances';
const JOB_ID_TTL_HOURS = 1; // Время жизни Job ID в очереди (1 час)
const TABLE_NAME = 'job_ids';

// 🔥 Инициализация пула PostgreSQL
if (!DATABASE_URL) {
    console.error("FATAL: DATABASE_URL is not set. Cannot connect to PostgreSQL.");
    process.exit(1);
}
const pool = new Pool({
    connectionString: DATABASE_URL,
    // На Render/Railway обычно не требуется SSL-конфиг,
    // но если возникнут проблемы, добавьте: ssl: { rejectUnauthorized: false }
});

app.use(bodyParser.json());

// ------------------------------------------------------------
// Инициализация базы данных (С ИСПРАВЛЕНИЕМ ОШИБКИ 42703)
// ------------------------------------------------------------
async function initDb() {
    try {
        // 1. Убеждаемся, что таблица существует (CREATE TABLE IF NOT EXISTS)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
                job_id VARCHAR(50) PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                checked_at TIMESTAMP WITH TIME ZONE
            );
        `);
        
        // 2. 🔥 БЕЗОПАСНОЕ ИСПРАВЛЕНИЕ: Добавляем колонку 'timestamp', если она отсутствует.
        // Это устраняет ошибку 'column "timestamp" does not exist' для старых таблиц.
        try {
             await pool.query(`
                ALTER TABLE ${TABLE_NAME} ADD COLUMN timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
             `);
             console.log(`[DB PATCH] Successfully added column 'timestamp' to existing table.`);
        } catch (e) {
            // Игнорируем ошибку, если колонка уже существует (code 42701)
            if (e.code !== '42701') {
                 console.warn(`[DB PATCH] Column 'timestamp' already existed or failed with non-fatal code: ${e.code}`);
            }
        }
        
        // 3. Создаем индекс
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
// 🔥 Проверка статуса Job ID в Roblox (Axios)
// ------------------------------------------------------------
async function checkRobloxServerStatus(jobId) {
    try {
        const response = await axios.post(ROBLOX_API_URL, {
            placeId: ROBLOX_PLACE_ID,
            jobIds: [jobId]
        }, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 5000
        });

        const instance = response.data.data ? response.data.data[0] : null;

        if (instance && instance.status === 'Alive' && instance.currentPlayers < instance.maxPlayers) {
            return true; // Сервер жив и не полон
        } else {
            return false; // Сервер не найден, мертв, или полон
        }
    } catch (error) {
        // В случае ошибки сети или таймаута, считаем сервер временно недоступным
        console.error(`[ROBLOX API ERROR] Failed to check JobID ${jobId}: ${error.message}`);
        return false; 
    }
}

// ------------------------------------------------------------
// API Эндпоинты
// ------------------------------------------------------------

/** Эндпоинт для приема Job ID от коллектора. */
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
        return res.json({ ok: true, added: 0, total: 0 });
    }

    try {
        // Используем ON CONFLICT DO NOTHING для предотвращения дублирования
        const query = `
            INSERT INTO ${TABLE_NAME} (job_id, timestamp) 
            VALUES ${values}
            ON CONFLICT (job_id) DO NOTHING;
        `;
        const result = await pool.query(query);
        const addedCount = result.rowCount;

        const totalResult = await pool.query(`SELECT COUNT(*) FROM ${TABLE_NAME}`);
        const totalCount = parseInt(totalResult.rows[0].count, 10);
        
        console.log(`[SUBMIT] Added ${addedCount} new IDs. Total: ${totalCount}`);
        res.json({ ok: true, added: addedCount, total: totalCount });

    } catch (error) {
        console.error("[DB SUBMIT ERROR]:", error);
        res.status(500).json({ error: "Database error during submission." });
    }
});

/** 🔥 Эндпоинт для выдачи живого Job ID клиенту. */
app.get('/api/get_job_id', async (req, res) => {
    // Внутренняя функция для обработки рекурсивного поиска
    const findLiveJobId = async () => {
        // 1. Ищем самый старый ID, который не истек по TTL
        const expiryDate = new Date(Date.now() - JOB_ID_TTL_HOURS * 3600 * 1000).toISOString();

        const queryResult = await pool.query(`
            SELECT job_id
            FROM ${TABLE_NAME}
            WHERE timestamp > $1
            ORDER BY timestamp ASC
            LIMIT 1
        `, [expiryDate]);

        const item = queryResult.rows[0];

        if (!item) {
            // Нет свежих ID. Очищаем устаревшие, если таковые есть, и завершаем
            await pool.query(`DELETE FROM ${TABLE_NAME} WHERE timestamp <= $1`, [expiryDate]);
            return res.status(404).json({ error: "No available fresh Job IDs found in queue." });
        }

        const jobId = item.job_id;

        // 2. Проверяем статус в Roblox API
        const isAlive = await checkRobloxServerStatus(jobId);
        
        if (isAlive) {
            // 🔥 ID ЖИВ! Удаляем его из базы и возвращаем.
            await pool.query(`DELETE FROM ${TABLE_NAME} WHERE job_id = $1`, [jobId]);
            
            const totalResult = await pool.query(`SELECT COUNT(*) FROM ${TABLE_NAME}`);
            const remaining = parseInt(totalResult.rows[0].count, 10);
            
            console.log(`[GET] Issued LIVE JobID: ${jobId}. Remaining: ${remaining}`);
            return res.json({ ok: true, job_id: jobId });
        } else {
            // 🔥 ID МЕРТВ. Удаляем его и пытаемся найти следующий.
            await pool.query(`DELETE FROM ${TABLE_NAME} WHERE job_id = $1`, [jobId]);
            console.log(`[GET] Deleted DEAD JobID: ${jobId}. Retrying...`);
            
            // 🔥 РЕКУРСИЯ: Ищем следующий ID
            return findLiveJobId();
        }
    };
    
    try {
        await findLiveJobId();
    } catch (error) {
        console.error("[DB GET ERROR]:", error);
        // Защита от бесконечной рекурсии в случае ошибки базы данных
        res.status(500).json({ error: "Database error during retrieval." });
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
