const fs = require('fs');
const mysql = require('mysql2/promise');
const fastcsv = require('fast-csv');

const connectionConfig = {
  host: 'xxx.xxx.xxx.xxx',
  user: 'xxxxxxxxx',
  password: 'xxxxxxxxxx',
  database: 'xxxxxxxxx',
};

async function deleteData(table) {
  let connection;
  try {
    connection = await mysql.createConnection(connectionConfig);
    const sql = `DELETE FROM ${table}`;
    const [result] = await connection.execute(sql);
  } catch (error) {
    console.error('Terjadi kesalahan saat delete:', error.message);
  } finally {
    if (connection) await connection.end();
  }
}

async function importCSV(table, fileName) {
  const filePath = `D:/SCRAPPERS/Scrapper/loaded_file/download_fulfillment/${fileName}.csv`;
  const selectedCols = ['org_1', 'org_2', 'first_cust_assign', 'f_ttr'];
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
          rows.push(filtered);
        })
        .on('end', resolve);
    });

    if (rows.length === 0) return console.log('CSV kosong');

    const batchSize = 1000;
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      const placeholders = batch.map(() => `(${selectedCols.map(() => '?').join(',')})`).join(',');
      const values = batch.flat();
      const sql = `INSERT INTO ${table} (${selectedCols.join(',')}) VALUES ${placeholders}`;
      const [result] = await connection.execute(sql, values);
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
  await deleteData('download_fulfillment_tif');
  await deleteData('download_fulfillment_regional');

  // Import CSV ke tabel, berurutan
  await importCSV('download_fulfillment_regional', 'data_detail_202511');
  await importCSV('download_fulfillment_regional', 'data_detail_202511 (1)');
  await importCSV('download_fulfillment_regional', 'data_detail_202511 (2)');
  await importCSV('download_fulfillment_regional', 'data_detail_202511 (3)');
  await importCSV('download_fulfillment_regional', 'data_detail_202511 (4)');
  await importCSV('download_fulfillment_regional', 'data_detail_202511 (5)');
  await importCSV('download_fulfillment_regional', 'data_detail_202511 (6)');

  await importCSV('download_fulfillment_tif', 'data_detail_202511 (7)');
  await importCSV('download_fulfillment_tif', 'data_detail_202511 (8)');
  await importCSV('download_fulfillment_tif', 'data_detail_202511 (9)');
  await importCSV('download_fulfillment_tif', 'data_detail_202511 (10)');
}

// Jalankan
main();
