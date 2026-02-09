const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const connection = require('./connection');
const { insertDate } = require('../currentDate');

// Fungsi untuk menghapus data pada tanggal saat ini
function deleteExistingData() {
  return new Promise((resolve, reject) => {
    const tableForDelete = ['helper_service_list_berbil', 'helper_service_list_gangguan', 'helper_sugar_list_berbil', 'helper_sugar_list_gangguan'];

    let count = 0;

    tableForDelete.forEach((table) => {
      const sql = `DELETE FROM ${table}`;
      connection.query(sql, [], (err) => {
        if (err) {
          console.error(`Error deleting data from ${table}:`, err);
          reject(err);
        } else {
          count++;
          if (count === tableForDelete.length) {
            resolve(); // Selesai menghapus
          }
        }
      });
    });
  });
}

// Fungsi untuk memasukkan data dari CSV ke database
function inputDataToDatabase(file, jenis, insertToTable) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'service_sugar', `${file}.csv`);
    const tgl = insertDate;

    const rows = [];

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => {
        const witel = data[Object.keys(data)[0]] || '';
        const m01 = data[Object.keys(data)[1]] || '';
        const m02 = data[Object.keys(data)[2]] || '';
        const m03 = data[Object.keys(data)[3]] || '';
        const m04 = data[Object.keys(data)[4]] || '';
        const m05 = data[Object.keys(data)[5]] || '';
        const m06 = data[Object.keys(data)[6]] || '';
        const m07 = data[Object.keys(data)[7]] || '';
        const m08 = data[Object.keys(data)[8]] || '';
        const m09 = data[Object.keys(data)[9]] || '';
        const m10 = data[Object.keys(data)[10]] || '';
        const m11 = data[Object.keys(data)[11]] || '';
        const m12 = data[Object.keys(data)[12]] || '';

        rows.push([tgl, witel, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12]);
      })
      .on('end', () => {
        const query = `INSERT INTO ${insertToTable} 
          (tgl, witel, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12) 
          VALUES ?`;

        connection.query(query, [rows], (err) => {
          if (err) {
            console.error('Error inserting data:', err);
            reject(err);
          } else {
            console.log(`${file} berhasil diinput ke tabel ${insertToTable}`);
            deleteUnwantedRows(insertToTable).then(resolve).catch(reject);
          }
        });
      })
      .on('error', (err) => {
        console.error('Gagal membuka file CSV:', err);
        reject(err);
      });
  });
}

// Fungsi untuk menghapus data yang tidak diperlukan
function deleteUnwantedRows(table) {
  return new Promise((resolve, reject) => {
    const deleteWitelValues = ['TOTAL', 'TARGET'];
    let count = 0;

    deleteWitelValues.forEach((value) => {
      const sql = `DELETE FROM ${table} WHERE witel = ?`;
      connection.query(sql, [value], (err) => {
        if (err) {
          console.error(`Error deleting ${value} from ${table}:`, err);
          reject(err);
        } else {
          count++;
          if (count === deleteWitelValues.length) {
            resolve();
          }
        }
      });
    });
  });
}

// Fungsi untuk memasukkan data dari CSV ke database
async function run() {
  try {
    await deleteExistingData();
    const files = [
      // Service
      ['service_list_bill_malang', 'tif', 'helper_service_list_berbil'],
      ['service_list_bill_semarang', 'tif', 'helper_service_list_berbil'],
      ['service_list_bill_sidoarjo', 'tif', 'helper_service_list_berbil'],
      ['service_list_bill_surabaya', 'tif', 'helper_service_list_berbil'],

      ['service_list_gangguan_malang', 'tif', 'helper_service_list_gangguan'],
      ['service_list_gangguan_semarang', 'tif', 'helper_service_list_gangguan'],
      ['service_list_gangguan_sidoarjo', 'tif', 'helper_service_list_gangguan'],
      ['service_list_gangguan_surabaya', 'tif', 'helper_service_list_gangguan'],

      ['sugar_list_bill_malang', 'tif', 'helper_sugar_list_berbil'],
      ['sugar_list_bill_semarang', 'tif', 'helper_sugar_list_berbil'],
      ['sugar_list_bill_sidoarjo', 'tif', 'helper_sugar_list_berbil'],
      ['sugar_list_bill_surabaya', 'tif', 'helper_sugar_list_berbil'],

      ['sugar_list_gangguan_malang', 'tif', 'helper_sugar_list_gangguan'],
      ['sugar_list_gangguan_semarang', 'tif', 'helper_sugar_list_gangguan'],
      ['sugar_list_gangguan_sidoarjo', 'tif', 'helper_sugar_list_gangguan'],
      ['sugar_list_gangguan_surabaya', 'tif', 'helper_sugar_list_gangguan'],
    ];

    for (const [file, jenis, insertToTable] of files) {
      await inputDataToDatabase(file, jenis, insertToTable);
    }
  } catch (err) {
    console.error('Error during data processing:', err);
  } finally {
    connection.end();
  }
}

run();
