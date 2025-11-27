// server.js (Для PostgreSQL)
const express = require('express');
const { Pool } = require('pg'); // <-- Используем pg
const app = express();

const PORT = process.env.PORT || 3000; 

// Инициализация пула соединений с PostgreSQL, используя DATABASE_URL от Render
const pool = new Pool({
    connectionString: process.env.DATABASE_URL, 
    ssl: { rejectUnauthorized: false } // Требуется для внешних хостингов, таких как Render
});

// ... (остальной код API, который вы уже писали, используя pool.connect() и client.query())

// Подключение к БД и коллекции
const DB_NAME = "roblox_db";
const COLLECTION_NAME = "JobIds";

app.use(express.json());

// Middleware для CORS (Разрешить запросы от Roblox)
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    next();
});

// ------------------------------------------------------------------
// ЭНДПОИНТ 1: ПРИЕМ (POST) от Коллектора
// ------------------------------------------------------------------
app.post('/api/submit_job_ids', async (req, res) => {
    const { job_ids } = req.body;
    if (!Array.isArray(job_ids) || job_ids.length === 0) {
        return res.status(400).json({ error: 'Invalid or empty job_ids array.' });
    }

    try {
        await client.connect();
        const collection = client.db(DB_NAME).collection(COLLECTION_NAME);

        // Преобразуем массив ID в формат MongoDB
        const inserts = job_ids.map(id => ({ job_id: id, createdAt: new Date() }));

        // Вставляем все, игнорируя дубликаты (для надежности нужно создать UNIQUE-индекс вручную в MongoDB)
        const result = await collection.insertMany(inserts, { ordered: false });

        res.json({ message: `Successfully inserted ${result.insertedCount} IDs.` });
    } catch (err) {
        if (err.code === 11000) { // Код ошибки MongoDB для дубликатов
             return res.json({ message: 'Processed, some duplicates ignored.' });
        }
        console.error('Database Error (Submit):', err);
        res.status(500).json({ error: 'Failed to process IDs.' });
    } finally {
        // В Railway лучше не закрывать соединение после каждого запроса, но для Serverless-подобного стиля
        // можно оставить, или убрать client.close() для лучшей производительности.
        // await client.close(); 
    }
});

// ------------------------------------------------------------------
// ЭНДПОИНТ 2: ВЫДАЧА (GET) для Roblox
// ------------------------------------------------------------------
app.get('/api/get_job_id', async (req, res) => {
    try {
        await client.connect();
        const collection = client.db(DB_NAME).collection(COLLECTION_NAME);

        // 1. Находим случайную запись и удаляем ее (Atomicity)
        const result = await collection.findOneAndDelete({}); 

        if (!result.value) {
            return res.status(503).json({ error: 'No job IDs available.' });
        }

        const job_id = result.value.job_id;

        res.json({ job_id: job_id });

    } catch (err) {
        console.error('Database Error (Get):', err);
        res.status(500).json({ error: 'Failed to retrieve Job ID.' });
    } finally {
        // await client.close();
    }
});


app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
