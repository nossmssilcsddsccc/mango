const express = require('express');
<<<<<<< HEAD
const bodyParser = require('body-parser');
const axios = require('axios'); // 🔥 НУЖЕН AXIOS ДЛЯ ЗАПРОСОВ К ROBLOX
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;
const DATABASE_FILE = 'job_ids.json';

// Конфигурация
const ROBLOX_PLACE_ID = "109983668079237"; // PLACE_ID вашей игры
const ROBLOX_API_URL = 'https://games.roblox.com/v1/games/multiget-place-instances';
const JOB_ID_TTL_HOURS = 1; // Время жизни Job ID в очереди (1 час)
const MAX_QUEUE_SIZE = 50000; // Ограничение очереди

// Глобальное хранилище данных:
// {
//   jobId: {
//     timestamp: <время добавления в мс>,
//     checked_at: <время последней проверки существования>
//   }
// }
let jobIds = {};

app.use(bodyParser.json());

// ------------------------------------------------------------
// Вспомогательные функции
// ------------------------------------------------------------

/** Загружает данные из файла. */
function loadData() {
    if (fs.existsSync(DATABASE_FILE)) {
        try {
            const data = fs.readFileSync(DATABASE_FILE, 'utf8');
            jobIds = JSON.parse(data);
            console.log(`[INIT] Loaded ${Object.keys(jobIds).length} IDs from file.`);
        } catch (e) {
            console.error("[ERROR] Failed to load data:", e.message);
            jobIds = {};
        }
=======
const { Pool } = require('pg'); 
const app = express();

const PORT = process.env.PORT || 3000; 

// ===================================================================
// 1. Инициализация Базы Данных (PostgreSQL)
// ===================================================================

// Pool использует переменную окружения process.env.DATABASE_URL
//, которую Render предоставляет автоматически (если вы связали сервисы)
const pool = new Pool({
    connectionString: process.env.DATABASE_URL, 
    ssl: { 
        // Требуется для внешних хостингов, таких как Render, для безопасного соединения
        rejectUnauthorized: false 
    } 
});

// Проверка соединения при старте (опционально, но полезно)
pool.on('error', (err) => {
    console.error('Unexpected error on idle client', err);
    process.exit(-1);
});

// Middleware для приема JSON и настройки CORS
app.use(express.json());

// Middleware для CORS (Разрешить запросы от любых источников)
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS'); // Добавляем OPTIONS
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    // Обработка предварительного запроса CORS (Preflight)
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});


// ===================================================================
// 2. ЭНДПОИНТ: ПРИЕМ (POST) от Коллектора
// ===================================================================
app.post('/api/submit_job_ids', async (req, res) => {
    const { job_ids } = req.body;
    if (!Array.isArray(job_ids) || job_ids.length === 0) {
        return res.status(400).json({ error: 'Invalid or empty job_ids array.' });
    }

    let client;
    try {
        client = await pool.connect(); // Получаем соединение из пула
        
        // 1. Подготовка параметризованного запроса для пакетной вставки
        const placeholders = [];
        const values = [];

        for (let i = 0; i < job_ids.length; i++) {
            // Создаем плейсхолдеры ($1), ($2), ...
            placeholders.push(`($${i + 1})`); 
            // Добавляем сами значения в массив
            values.push(job_ids[i]);
        }
        
        // 2. Формируем SQL-запрос с безопасными плейсхолдерами
        const query = `
            INSERT INTO job_ids (job_id) 
            VALUES ${placeholders.join(', ')}
            ON CONFLICT (job_id) DO NOTHING;
        `;
        
        // 3. Выполняем запрос, передавая значения отдельно
        const result = await client.query(query, values);

        res.json({ message: `Successfully processed IDs. Inserted: ${result.rowCount}.` });

    } catch (err) {
        // Логирование ошибок соединения или SQL-синтаксиса
        console.error('Database Error (Submit):', err);
        res.status(500).json({ error: 'Failed to process IDs.' });
    } finally {
        if (client) client.release(); // Важно: возвращаем соединение в пул
>>>>>>> 3376461bc962cfe45fdbeb8ecba691c35c1ab8c3
    }
}

/** Сохраняет данные в файл. */
function saveData() {
    try {
        fs.writeFileSync(DATABASE_FILE, JSON.stringify(jobIds, null, 2), 'utf8');
    } catch (e) {
        console.error("[ERROR] Failed to save data:", e.message);
    }
}

/** Очищает устаревшие ID и удаляет 'checked_at' для принудительной перепроверки. */
function cleanAndPrepareQueue() {
    const now = Date.now();
    const expiryTime = now - (JOB_ID_TTL_HOURS * 3600 * 1000);
    const keysToDelete = [];
    let readyCount = 0;

    for (const jobId in jobIds) {
        if (jobIds[jobId].timestamp < expiryTime) {
            keysToDelete.push(jobId);
        } else {
            // Удаляем checked_at, чтобы принудительно проверить статус при следующем запросе
            delete jobIds[jobId].checked_at;
            readyCount++;
        }
    }

    keysToDelete.forEach(key => delete jobIds[key]);
    
    // Ограничение размера очереди
    const currentKeys = Object.keys(jobIds);
    if (currentKeys.length > MAX_QUEUE_SIZE) {
        // Сортируем по времени добавления (самые старые в начале)
        currentKeys.sort((a, b) => jobIds[a].timestamp - jobIds[b].timestamp);
        const excessCount = currentKeys.length - MAX_QUEUE_SIZE;
        for (let i = 0; i < excessCount; i++) {
            delete jobIds[currentKeys[i]];
        }
        console.log(`[CLEAN] Trimmed ${excessCount} old IDs due to max queue size.`);
    }

    console.log(`[CLEAN] Cleaned ${keysToDelete.length} expired IDs. Queue size: ${Object.keys(jobIds).length} (Ready: ${readyCount})`);
    saveData();
    return readyCount;
}

/** 🔥 Проверяет статус существования Job ID в Roblox. */
async function checkRobloxServerStatus(jobId) {
    try {
        const response = await axios.post(ROBLOX_API_URL, {
            placeId: ROBLOX_PLACE_ID,
            jobIds: [jobId]
        }, {
            headers: {
                'Content-Type': 'application/json'
            },
            timeout: 5000
        });

        // Ожидаемый ответ: [{ jobID: "...", maxPlayers: 1, currentPlayers: 1, status: "Alive" }]
        const instance = response.data.data ? response.data.data[0] : null;

        if (instance && instance.status === 'Alive' && instance.currentPlayers < instance.maxPlayers) {
            // Сервер существует и не полон
            return true;
        } else {
            // Сервер не найден, мертв, или полон
            return false;
        }
    } catch (error) {
        console.error(`[ROBLOX API ERROR] Failed to check JobID ${jobId}: ${error.message}`);
        // В случае ошибки сети или таймаута, считаем сервер временно недоступным (false)
        return false; 
    }
}

// ------------------------------------------------------------
// API Эндпоинты
// ------------------------------------------------------------

/** Эндпоинт для приема Job ID от коллектора. */
app.post('/api/submit_job_ids', (req, res) => {
    const newJobIds = req.body.job_ids;
    if (!Array.isArray(newJobIds) || newJobIds.length === 0) {
        return res.status(400).json({ error: "job_ids array is required" });
    }

    const now = Date.now();
    let newCount = 0;

    newJobIds.forEach(jobId => {
        if (typeof jobId === 'string' && jobId.length > 5 && !jobIds[jobId]) {
            jobIds[jobId] = {
                timestamp: now
            };
            newCount++;
        }
    });

    saveData();
    console.log(`[SUBMIT] Added ${newCount} new IDs. Total: ${Object.keys(jobIds).length}`);
    res.json({ ok: true, added: newCount, total: Object.keys(jobIds).length });
});

<<<<<<< HEAD
/** 🔥 Эндпоинт для выдачи Job ID клиенту. */
app.get('/api/get_job_id', async (req, res) => {
    const readyCount = cleanAndPrepareQueue();
    const jobIdsList = Object.keys(jobIds);

    if (jobIdsList.length === 0) {
        return res.status(404).json({ error: "Queue is empty." });
    }

    // 🔥 Ищем следующий Job ID, который не был проверен
    let nextJobId = null;
    let index = 0;
    while (index < jobIdsList.length) {
        const currentId = jobIdsList[index];
        const item = jobIds[currentId];
        
        // 1. Проверяем, был ли ID уже проверен и является ли он живым
        if (item.checked_at) {
            // Пропускаем ID, которые мы уже проверили и пометили как живые, чтобы равномерно распределить нагрузку
            index++;
            continue; 
=======

// ===================================================================
// 3. ЭНДПОИНТ: ВЫДАЧА (GET) для Roblox (Случайный ID и Удаление)
// ===================================================================
app.get('/api/get_job_id', async (req, res) => {
    let client;
    try {
        client = await pool.connect(); 
        await client.query('BEGIN'); // Начинаем транзакцию для атомарной операции

        // 1. Находим случайную запись
        const fetchQuery = `
            SELECT job_id, id FROM job_ids
            ORDER BY RANDOM()
            LIMIT 1
            FOR UPDATE SKIP LOCKED; -- Блокируем и пропускаем, если уже заблокировано
        `;
        const result = await client.query(fetchQuery);

        if (result.rows.length === 0) {
            await client.query('ROLLBACK'); // Откатываем, если ничего не нашли
            return res.status(503).json({ error: 'No job IDs available.' });
>>>>>>> 3376461bc962cfe45fdbeb8ecba691c35c1ab8c3
        }
        
        const record = result.rows[0];
        
        // 2. Удаляем выданный ID
        const deleteQuery = `
            DELETE FROM job_ids WHERE id = $1;
        `;
        await client.query(deleteQuery, [record.id]);

<<<<<<< HEAD
        // 2. Если ID не проверялся, проверяем его статус в Roblox API
        const isAlive = await checkRobloxServerStatus(currentId);
        
        if (isAlive) {
            // 🔥 ID ЖИВ! Выбираем его и помечаем как проверенный.
            nextJobId = currentId;
            item.checked_at = Date.now();
            saveData();
            console.log(`[GET] Issued LIVE JobID: ${nextJobId}. Remaining: ${jobIdsList.length - 1}`);
            
            // Удаляем выданный ID из очереди (чтобы другой клиент не взял его сразу)
            delete jobIds[nextJobId];
            saveData();
            
            return res.json({ ok: true, job_id: nextJobId });
        } else {
            // 🔥 ID МЕРТВ. Удаляем его сразу из базы.
            delete jobIds[currentId];
            saveData();
            console.log(`[GET] Deleted DEAD JobID: ${currentId}`);
            // Продолжаем поиск со следующего элемента
            index++;
        }
=======
        await client.query('COMMIT'); // Завершаем транзакцию
        
        res.json({ job_id: record.job_id });

    } catch (err) {
        // Если что-то пошло не так, откатываем все изменения
        if (client) await client.query('ROLLBACK'); 
        console.error('Database Error (Get):', err);
        res.status(500).json({ error: 'Failed to retrieve Job ID.' });
    } finally {
        if (client) client.release(); 
>>>>>>> 3376461bc962cfe45fdbeb8ecba691c35c1ab8c3
    }
    
    // Если мы прошли всю очередь и не нашли живого ID, возвращаем ошибку.
    res.status(404).json({ error: "No available live Job IDs found in queue." });
});

// ------------------------------------------------------------
// Инициализация и запуск
// ------------------------------------------------------------

loadData();

// Запускаем очистку каждые 15 минут
setInterval(cleanAndPrepareQueue, 15 * 60 * 1000); 

// ===================================================================
// 4. Запуск сервера
// ===================================================================
app.listen(PORT, () => {
<<<<<<< HEAD
    console.log(`Server is running on port ${PORT}`);
    cleanAndPrepareQueue(); // Первая очистка при запуске
});
=======
    console.log(`Server running on port ${PORT}`);
});
>>>>>>> 3376461bc962cfe45fdbeb8ecba691c35c1ab8c3
