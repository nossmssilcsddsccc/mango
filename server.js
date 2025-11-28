// server.js (API Server Logic)

import express from 'express';
import Redis from 'ioredis';
import bodyParser from 'body-parser';

// --- CONFIGURATION ---
const PORT = process.env.PORT || 3000;
// Используем URL из переменной окружения Render или локальный
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379'; 

// Названия списков в Redis
const AVAILABLE_JOBS_KEY = 'jobs:available';
const ACTIVE_JOBS_KEY = 'jobs:active';
const SUBMITTED_JOBS_KEY = 'jobs:submitted'; // Для хранения всех принятых ID

// --- INIT ---
const app = express();
const redis = new Redis(REDIS_URL);

app.use(bodyParser.json());

// Проверка соединения с Redis
redis.on('connect', () => {
    console.log(`[REDIS] Connected to Redis at ${REDIS_URL}`);
});
redis.on('error', (err) => {
    console.error('[REDIS ERROR]', err);
    // В случае критической ошибки Redis, можно завершить работу
    // process.exit(1);
});

// ------------------------------------------------------------------
// 1. АТОМАРНАЯ ЛОГИКА ВЫДАЧИ JOB ID
// ------------------------------------------------------------------

/**
 * Атомарно выдает один Job ID, перемещая его из доступных в активные.
 * @returns {string | null} Выданный Job ID или null, если доступных нет.
 */
async function issueJobId() {
    try {
        // RPOPLPUSH гарантирует, что Job ID будет взят только один раз
        const jobId = await redis.rpoplpush(AVAILABLE_JOBS_KEY, ACTIVE_JOBS_KEY);
        
        if (jobId) {
            const remaining = await redis.llen(AVAILABLE_JOBS_KEY);
            console.log(`[GET] Issued TTL-valid jobId: ${jobId}. Remaining: ${remaining}`);
            return jobId;
        } else {
            return null;
        }
    } catch (error) {
        console.error('[CRITICAL] Failed to issue job ID:', error);
        return null;
    }
}

// ------------------------------------------------------------------
// 2. ЛОГИКА ПРИЕМА JOB ID ОТ СКАНЕРА (обновлено)
// ------------------------------------------------------------------

/**
 * Обрабатывает список Job ID, полученных от сканера.
 */
async function submitJobIds(jobIds) {
    if (!jobIds || jobIds.length === 0) return { affected: 0 };
    
    let addedCount = 0;
    
    // Используем транзакцию MULTI/EXEC для атомарного добавления
    const multi = redis.multi();
    
    // 🔥 Pipelining (пакетирование) для высокой скорости записи
    jobIds.forEach(id => {
        // SADD (Set Add) гарантирует, что ID будет добавлен только один раз (уникальность)
        multi.sadd(SUBMITTED_JOBS_KEY, id); 
    });
    
    try {
        const results = await multi.exec();
        
        // Подсчитываем, сколько новых ID было добавлено (SADD возвращает 1, если новый)
        results.forEach(result => {
            if (result[1] === 1) { // result[1] - это результат команды SADD
                addedCount++;
            }
        });
        
        return { affected: jobIds.length, added: addedCount };
    } catch (error) {
        console.error('[SUBMIT ERROR] Failed to execute transaction:', error);
        return { affected: 0 };
    }
}


// ------------------------------------------------------------------
// 3. МАРШРУТЫ API
// ------------------------------------------------------------------

// Маршрут для выдачи Job ID
app.get('/api/get_job_id', async (req, res) => {
    const jobId = await issueJobId();
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
// 4. ИНИЦИАЛИЗАЦИЯ (ОПЦИОНАЛЬНО)
// ------------------------------------------------------------------

// Запуск сервера
app.listen(PORT, () => {
    console.log(`\n--- Server running on port ${PORT} ---`);
    console.log(`API URL: http://localhost:${PORT}/api/get_job_id`);
});

// --- ВНИМАНИЕ ---
// Вам нужно создать начальный пул Job ID в Redis вручную или через отдельный скрипт.
// Пример: redis.lpush(AVAILABLE_JOBS_KEY, 'jobId1', 'jobId2', 'jobId3', ...);
// ИЛИ
// redis.sadd(AVAILABLE_JOBS_KEY, 'jobId1', 'jobId2', ...);
