const pool = require('./connection'); // Menggunakan pool dari mysql2/promise
const { insertDate } = require('./currentDate');

// 1. Fungsi hapus data lama (Async)
async function deleteExistingData() {
  const tableForDelete = ['unspec_datin'];
  const currentDate = insertDate;

  for (const table of tableForDelete) {
    try {
      const sql = `DELETE FROM ${table} WHERE tgl = ?`;
      await pool.execute(sql, [currentDate]);
      console.log(`🗑️ Data lama dihapus dari tabel ${table}`);
    } catch (err) {
      console.error(`❌ Error deleting data from ${table}:`, err.message);
      throw err;
    }
  }
}

// 2. Fungsi proses kalkulasi unspec_datin (Async)
async function prosesUnspecDatin(jenis, area) {
  const query = `
    SELECT
      sc_lokasi.witel AS witel,
      list_datin.comply AS list_datin,
      tiket_open_datin.comply AS tiket_open_datin
    FROM sc_lokasi
    LEFT JOIN list_datin 
      ON sc_lokasi.witel = list_datin.regional 
      AND list_datin.tgl = ? 
      AND list_datin.jenis = ?
    LEFT JOIN tiket_open_datin
      ON sc_lokasi.witel = tiket_open_datin.regional 
      AND tiket_open_datin.tgl = ?
      AND tiket_open_datin.jenis = ?
    WHERE sc_lokasi.reg = ?
  `;

  try {
    // Jalankan query SELECT menggunakan parameter array untuk keamanan
    const [results] = await pool.query(query, [insertDate, jenis, insertDate, jenis, area]);

    const insertValues = [];
    results.forEach((row) => {
      const val_list_datin = parseFloat(row.list_datin) || 0;
      const val_list_open = parseFloat(row.tiket_open_datin) || 0;

      // Rumus unspec_datin
      const unspec_datin = val_list_datin > 0 ? ((val_list_datin - val_list_open) / val_list_datin) * 100 : 0;

      insertValues.push([insertDate, jenis, row.witel, val_list_datin, val_list_open, unspec_datin.toFixed(2)]);
    });

    if (insertValues.length === 0) {
      console.log(`⚠️ [${jenis} - ${area}] Tidak ada data ditemukan.`);
      return;
    }

    // Bulk Insert
    const insertQuery = `
      INSERT INTO unspec_datin (tgl, jenis, regional, list_datin, list_open, comply)
      VALUES ?
    `;
    await pool.query(insertQuery, [insertValues]);
    console.log(`✅ [${jenis} - ${area}] Berhasil insert ${insertValues.length} baris.`);
  } catch (err) {
    console.error(`❌ Gagal memproses ${jenis} - ${area}:`, err.message);
    throw err;
  }
}

// 3. Eksekusi utama
async function run() {
  try {
    console.log('🔄 Memulai proses perhitungan unspec_datin...');

    // Hapus data lama dulu
    await deleteExistingData();

    // Jalankan satu per satu secara berurutan
    await prosesUnspecDatin('tif', 'tif');
    await prosesUnspecDatin('tif', 'district');
    await prosesUnspecDatin('reg', 'nas');
    await prosesUnspecDatin('reg', 'witel');

    console.log('🚀 Semua proses unspec_datin selesai!');
  } catch (err) {
    console.error('💥 Terjadi kesalahan fatal:', err.message);
  } finally {
    // Jika script ini dijalankan sebagai task mandiri:
    await pool.end();
  }
}

run();
