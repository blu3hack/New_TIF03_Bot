const fs = require('fs');
const mysql = require('mysql2/promise');
const fastcsv = require('fast-csv');

const connectionConfig = {
  host: 'xxx.xxx.xxx.xxx',
  user: 'xxxxxxxxx',
  password: 'xxxxxxxxxx',
  database: 'xxxxxxxxx',
};

const table_for_delete = ['helper_sugar_datin', 'helper_sugar_hsi'];

async function deleteData(table) {
  let connection;
  try {
    connection = await mysql.createConnection(connectionConfig);
    const sql = `DELETE FROM ${table}`;
    await connection.execute(sql);
    console.log(`Data di tabel ${table} berhasil dihapus`);
  } catch (error) {
    console.error(`Terjadi kesalahan saat delete tabel ${table}:`, error.message);
  } finally {
    if (connection) await connection.end();
  }
}

async function deleteAllTables() {
  for (const table of table_for_delete) {
    await deleteData(table);
  }
}

async function importCSV(table, fileName, kat) {
  const filePath = `D:/SCRAPPERS/Scrapper/loaded_file/download_fulfillment/${fileName}.csv`;
  const selectedCols = ['INCIDENT', 'WORKZONE', 'WITEL'];
  let connection;

  try {
    connection = await mysql.createConnection(connectionConfig);

    const rows = [];
    await new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(fastcsv.parse({ headers: true, quote: '"', trim: true }))
        .on('error', reject)
        .on('data', (row) => {
          const filtered = selectedCols.map((col) => row[col] || null);
          filtered.push(kat); // tambah KAT di akhir
          rows.push(filtered);
        })
        .on('end', resolve);
    });

    if (rows.length === 0) return console.log('CSV kosong');

    const insertCols = [...selectedCols, 'KAT'];
    const batchSize = 1000;

    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      const placeholders = batch.map(() => `(${insertCols.map(() => '?').join(',')})`).join(',');
      const values = batch.flat();

      const sql = `INSERT INTO ${table} (${insertCols.join(',')}) VALUES ${placeholders}`;
      await connection.execute(sql, values);
    }

    console.log(`File : ${fileName} berhasil diimport ke ${table}`);
  } catch (err) {
    console.error(`Error importCSV for ${table}:`, err);
  } finally {
    if (connection) await connection.end();
  }
}

// Fungsi utama untuk menjalankan semua proses berurutan
async function main() {
  // Hapus tabel dulu, berurutan
  await deleteAllTables();
  // await importCSV('helper_sugar_datin', "REPORT_SUMMARY_GAUL_'_'REG-4'_''_'DATIN24_-", 'non_gaul');
  // await importCSV('helper_sugar_datin', "REPORT_SUMMARY_GAUL_'_'REG-4'_''_'DATIN24_- (1)", 'total');
  // await importCSV('helper_sugar_datin', "REPORT_SUMMARY_GAUL_'_'REG-5'_''_'DATIN24_-", 'non_gaul');
  // await importCSV('helper_sugar_datin', "REPORT_SUMMARY_GAUL_'_'REG-5'_''_'DATIN24_- (1)", 'total');

  await importCSV('helper_sugar_hsi', "REPORT_SUMMARY_GAUL_'_'REG-4'_''_'HSI24_-", 'non_gaul');
  await importCSV('helper_sugar_hsi', "REPORT_SUMMARY_GAUL_'_'REG-4'_''_'HSI24_- (1)", 'total');
  await importCSV('helper_sugar_hsi', "REPORT_SUMMARY_GAUL_'_'REG-5'_''_'HSI24_-", 'non_gaul');
  await importCSV('helper_sugar_hsi', "REPORT_SUMMARY_GAUL_'_'REG-5'_''_'HSI24_- (1)", 'total');
}

// Jalankan
main();
