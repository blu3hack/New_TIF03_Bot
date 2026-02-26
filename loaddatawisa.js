const fs = require('fs');
const pool = require('./connection');
const { insertDate } = require('./currentDate.js');
const path = require('path');

// Hapus data lama
async function deleteOldData(table) {
  const deleteQuery = `
    DELETE FROM ${table}
    WHERE REPLACE(uic, CHAR(13), '') = 'operasional'
      AND insert_at = ?
  `;

  const [results] = await pool.query(deleteQuery, [insertDate]);

  console.log(`🗑️ Data lama berhasil dihapus (${results.affectedRows} baris) untuk insert_at = ${insertDate}`);
}

// Import CSV
async function loadCSV(filename, table) {
  const csvPath = path.join(__dirname, 'loaded_file/msa_upload', `${filename}.csv`);

  if (!fs.existsSync(csvPath)) {
    throw new Error(`File tidak ditemukan: ${csvPath}`);
  }

  const loadQuery = `
    LOAD DATA LOCAL INFILE ?
    INTO TABLE ${table}
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS
    (kpi, lokasi, Area, Realisasi, insert_at, bulan, uic)
  `;

  const [results] = await pool.query({
    sql: loadQuery,
    values: [csvPath],
    infileStreamFactory: () => fs.createReadStream(csvPath),
  });

  console.log(`✅ Impor CSV ${filename} berhasil: ${results.affectedRows} baris`);
}

// Main
async function run() {
  try {
    await deleteOldData('wisa_upload_operational');

    await loadCSV('tif.csv', 'wisa_upload_operational');
    await loadCSV('ccm.csv', 'wisa_upload_operational');

    console.log('🎉 Semua proses selesai.');
  } catch (err) {
    console.error('❌ Terjadi kesalahan:', err.message);
  } finally {
    await pool.end();
  }
}

run();
