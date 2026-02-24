const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: 'xxx.xxx.xxx.xxx',
  user: 'xxxxxxxx',
  password: '##########',
  database: 'perf_tif',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

(async () => {
  try {
    const conn = await pool.getConnection();
    console.log('✅ Terkoneksi ke database MySQL (pool).');
    conn.release();
  } catch (err) {
    console.error('❌ Gagal terkoneksi ke database:', err.message);
  }
})();
module.exports = pool;
