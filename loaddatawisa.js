const fs = require('fs');
const mysql = require('mysql2');
const pool = require('./connection');
const { insertDate } = require('./currentDate.js');

// Fungsi untuk menghapus data lama berdasarkan tanggal dan uic
function deleteOldData(table) {
  return new Promise((resolve, reject) => {
    const deleteQuery = `
      DELETE FROM ${table} 
      WHERE REPLACE(uic, CHAR(13), '') = 'operasional' 
        AND insert_at = ?
    `;

    pool.query(deleteQuery, [insertDate], (err, results) => {
      if (err) {
        console.error('⚠️ Gagal menghapus data lama:', err);
        return reject(err);
      }

      console.log(`🗑️ Data lama berhasil dihapus (${results.affectedRows} baris) untuk insert_at = ${insertDate}`);
      resolve();
    });
  });
}

// Fungsi untuk mengimpor CSV ke database
function loadCSV(filename, table) {
  return new Promise((resolve, reject) => {
    const csvPath = `D:/SCRAPPERS/Scrapper/loaded_file/msa_upload/${filename}`;

    const loadQuery = `
      LOAD DATA LOCAL INFILE ?
      INTO TABLE ${table}
      FIELDS TERMINATED BY ',' 
      ENCLOSED BY '"'
      LINES TERMINATED BY '\n'
      IGNORE 1 ROWS
      (kpi, lokasi, Area, Realisasi, insert_at, bulan, uic)
    `;

    pool.query(
      {
        sql: loadQuery,
        values: [csvPath],
        infileStreamFactory: () => fs.createReadStream(csvPath),
      },
      (err, results) => {
        if (err) {
          console.error(`⚠️ Gagal mengimpor ${filename}:`, err);
          return reject(err);
        }
        console.log(`✅ Impor CSV ${filename} berhasil:`, results.affectedRows);
        resolve();
      },
    );
  });
}

// Fungsi utama
async function run() {
  try {
    await deleteOldData('wisa_upload_operational'); // Hapus data lama dulu
    await loadCSV('tif.csv', 'wisa_upload_operational');
    // await loadCSV('district.csv', 'wisa_upload_operational');
    await loadCSV('ccm.csv', 'wisa_upload_operational');
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}

run();
