const pool = require('./connection'); // Menggunakan pool dari mysql2/promise
const { insertDate } = require('./currentDate');

// 1. Hapus data lama berdasarkan tanggal (Versi Async)
async function deleteExistingData() {
  const tableForDelete = ['unspec_hsi'];
  const currentDate = insertDate;

  for (const table of tableForDelete) {
    try {
      const sql = `DELETE FROM ${table} WHERE tgl = ?`;
      await pool.execute(sql, [currentDate]);
      console.log(`🗑️ Data lama dihapus dari tabel ${table}`);
    } catch (err) {
      console.error(`❌ Error deleting data from ${table}:`, err.message);
      throw err; // Lempar error agar proses berhenti jika gagal hapus
    }
  }
}

// 2. Fungsi utama proses perhitungan (Versi Async)
async function prosesUnspecDatin(jenis, area) {
  const query = `
    SELECT
      sc_lokasi.witel AS witel,
      list_hsi.comply AS list_hsi,
      tiket_open_hsi.comply AS tiket_open_hsi
    FROM sc_lokasi
    LEFT JOIN list_hsi 
      ON sc_lokasi.witel = list_hsi.regional 
      AND list_hsi.tgl = ? 
      AND list_hsi.jenis = ?
    LEFT JOIN tiket_open_hsi
      ON sc_lokasi.witel = tiket_open_hsi.regional 
      AND tiket_open_hsi.tgl = ?
      AND tiket_open_hsi.jenis = ?
    WHERE sc_lokasi.reg = ?
  `;

  try {
    // Jalankan query SELECT
    const [results] = await pool.query(query, [insertDate, jenis, insertDate, jenis, area]);

    const insertValues = [];
    results.forEach((row) => {
      const list_datin = parseFloat(row.list_hsi) || 0;
      const list_open = parseFloat(row.tiket_open_hsi) || 0;

      // Kalkulasi unspec
      const unspec_datin = list_datin > 0 ? ((list_datin - list_open) / list_datin) * 100 : 0;

      insertValues.push([insertDate, jenis, row.witel, list_datin, list_open, unspec_datin.toFixed(2)]);
    });

    if (insertValues.length === 0) {
      console.log(`⚠️ [${jenis} - ${area}] Tidak ada data untuk diinsert.`);
      return;
    }

    // Jalankan query INSERT (Bulk)
    const insertQuery = `
      INSERT INTO unspec_hsi (tgl, jenis, regional, list_datin, list_open, comply)
      VALUES ?
    `;
    await pool.query(insertQuery, [insertValues]);
    console.log(`✅ [${jenis} - ${area}] Berhasil insert ${insertValues.length} baris.`);
  } catch (err) {
    console.error(`❌ Gagal memproses unspec ${jenis} - ${area}:`, err.message);
    throw err;
  }
}

// 3. Jalankan secara berurutan dengan Clean Code
async function run() {
  try {
    console.log('🔄 Memulai proses perhitungan unspec_hsi...');

    // Hapus dulu
    await deleteExistingData();

    // Jalankan proses satu per satu secara sinkronus (await)
    await prosesUnspecDatin('tif', 'tif');
    await prosesUnspecDatin('tif', 'district');
    await prosesUnspecDatin('reg', 'nas');
    await prosesUnspecDatin('reg', 'witel');

    console.log('🔒 Semua proses selesai!');
  } catch (err) {
    console.error('💥 Terjadi kegagalan sistem:', err.message);
  } finally {
    // Tutup pool hanya jika ini adalah script cron job mandiri
    await pool.end();
  }
}

run();
