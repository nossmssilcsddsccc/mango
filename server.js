const express = require('express');
const { Pool } = require('pg'); // <-- Правильный импорт для PostgreSQL
const app = express();

const PORT = process.env.PORT || 3000; 
// Имя БД и Коллекции/Таблицы больше не нужно, так как Pool использует полную строку.
// const DB_NAME = "roblox_db"; 
// const COLLECTION_NAME = "JobIds"; 

// 1. Инициализация пула соединений с PostgreSQL, используя DATABASE_URL от Render
const pool = new Pool({
    connectionString: process.env.DATABASE_URL, 
    ssl: { rejectUnauthorized: false } // Требуется для Render
});

app.use(express.json());

// Middleware для CORS (Разрешить запросы от Roblox)
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
});

// ------------------------------------------------------------------
// ЭНДПОИНТ 1: ПРИЕМ (POST) от Коллектора (Логика PostgreSQL)
// ------------------------------------------------------------------
app.post('/api/submit_job_ids', async (req, res) => {
    const { job_ids } = req.body;
    if (!Array.isArray(job_ids) || job_ids.length === 0) {
        return res.status(400).json({ error: 'Invalid or empty job_ids array.' });
    }

    let client;
    try {
        client = await pool.connect(); // Получаем соединение из пула
        
        // Преобразуем массив ID в формат для SQL-запроса: ('id1'), ('id2'), ...
        const values = job_ids.map(id => `('${id}')`).join(', ');
        
        // Запрос для вставки, используем ON CONFLICT DO NOTHING, чтобы игнорировать дубликаты
        const query = `
            INSERT INTO job_ids (job_id) 
            VALUES ${values}
            ON CONFLICT (job_id) DO NOTHING;
        `;
        
        const result = await client.query(query);

        // result.rowCount покажет, сколько строк было реально вставлено
        res.json({ message: `Successfully processed IDs. Inserted: ${result.rowCount}.` });
    } catch (err) {
        console.error('Database Error (Submit):', err);
        res.status(500).json({ error: 'Failed to process IDs.' });
    } finally {
        if (client) client.release(); // Возвращаем соединение в пул
    }
});

// ------------------------------------------------------------------
// ЭНДПОИНТ 2: ВЫДАЧА (GET) для Roblox (Логика PostgreSQL)
// ------------------------------------------------------------------
app.get('/api/get_job_id', async (req, res) => {
    let client;
    try {
        client = await pool.connect(); // Получаем соединение из пула

        // 1. Находим случайную запись
        const fetchQuery = `
            SELECT job_id, id FROM job_ids
            ORDER BY RANDOM()
            LIMIT 1
            FOR UPDATE SKIP LOCKED; -- Блокируем и пропускаем, если уже заблокировано
        `;
        const result = await client.query(fetchQuery);

        if (result.rows.length === 0) {
            return res.status(503).json({ error: 'No job IDs available.' });
        }
        
        const record = result.rows[0];
        
        // 2. Удаляем выданный ID
        const deleteQuery = `
            DELETE FROM job_ids WHERE id = $1;
        `;
        await client.query(deleteQuery, [record.id]);
        
        res.json({ job_id: record.job_id });

    } catch (err) {
        console.error('Database Error (Get):', err);
        res.status(500).json({ error: 'Failed to retrieve Job ID.' });
    } finally {
        if (client) client.release(); // Возвращаем соединение в пул
    }
});


app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
