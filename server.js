const express = require('express');
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
    }
});


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
        }
        
        const record = result.rows[0];
        
        // 2. Удаляем выданный ID
        const deleteQuery = `
            DELETE FROM job_ids WHERE id = $1;
        `;
        await client.query(deleteQuery, [record.id]);

        await client.query('COMMIT'); // Завершаем транзакцию
        
        res.json({ job_id: record.job_id });

    } catch (err) {
        // Если что-то пошло не так, откатываем все изменения
        if (client) await client.query('ROLLBACK'); 
        console.error('Database Error (Get):', err);
        res.status(500).json({ error: 'Failed to retrieve Job ID.' });
    } finally {
        if (client) client.release(); 
    }
});


// ===================================================================
// 4. Запуск сервера
// ===================================================================
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
