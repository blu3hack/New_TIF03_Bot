const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const connection = require('./connection');
const { insertDate } = require('../currentDate');

// Fungsi untuk menghapus data pada tanggal saat ini
function deleteExistingData() {
  return new Promise((resolve, reject) => {
    const tableForDelete = ['wsa_sugar_nop', 'wsa_service_nop', 'wsa_ttr3_nop', 'wsa_ttr6_nop', 'wsa_ttr36_nop', 'wsa_ttrmanja_nop', 'wsa_sugar', 'wsa_service', 'wsa_ttr3', 'wsa_ttr6', 'wsa_ttr36', 'wsa_ttrmanja'];
    const currentDate = insertDate;

    let count = 0;

    tableForDelete.forEach((table) => {
      const sql = `DELETE FROM ${table} where tgl = ?`;
      connection.query(sql, [currentDate], (err) => {
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
    const filePath = path.join(__dirname, 'wsa_gamas', `${file}.csv`);
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

        const newWitel = witel.replace('TERITORY', 'TERRITORY');

        rows.push([tgl, jenis, newWitel, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12]);
      })
      .on('end', () => {
        const query = `INSERT INTO ${insertToTable} 
          (tgl, lokasi, witel, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12) 
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
      ['service_tif', 'tif', 'wsa_service'],
      ['service_district_tif3', 'tif', 'wsa_service'],
      ['service_nas', 'reg', 'wsa_service'],
      ['service_tr4', 'reg', 'wsa_service'],
      ['service_tr5', 'reg', 'wsa_service'],
      ['service_area', 'area', 'wsa_service_nop'],
      ['service_balnus', 'balnus', 'wsa_service_nop'],
      ['service_jateng', 'jateng', 'wsa_service_nop'],
      ['service_jatim', 'jatim', 'wsa_service_nop'],
      // sugar
      ['sugar_tif', 'tif', 'wsa_sugar'],
      ['sugar_district_tif3', 'tif', 'wsa_sugar'],
      ['sugar_nas', 'reg', 'wsa_sugar'],
      ['sugar_tr4', 'reg', 'wsa_sugar'],
      ['sugar_tr5', 'reg', 'wsa_sugar'],
      ['sugar_area', 'area', 'wsa_sugar_nop'],
      ['sugar_balnus', 'balnus', 'wsa_sugar_nop'],
      ['sugar_jateng', 'jateng', 'wsa_sugar_nop'],
      ['sugar_jatim', 'jatim', 'wsa_sugar_nop'],
      // TT3
      ['ttr3_tif', 'tif', 'wsa_ttr3'],
      ['ttr3_district_tif3', 'tif', 'wsa_ttr3'],
      ['ttr3_nas', 'reg', 'wsa_ttr3'],
      ['ttr3_tr4', 'reg', 'wsa_ttr3'],
      ['ttr3_tr5', 'reg', 'wsa_ttr3'],
      ['ttr3_area', 'area', 'wsa_ttr3_nop'],
      ['ttr3_balnus', 'balnus', 'wsa_ttr3_nop'],
      ['ttr3_jateng', 'jateng', 'wsa_ttr3_nop'],
      ['ttr3_jatim', 'jatim', 'wsa_ttr3_nop'],
      // TTR6
      ['ttr6_tif', 'tif', 'wsa_ttr6'],
      ['ttr6_district_tif3', 'tif', 'wsa_ttr6'],
      ['ttr6_nas', 'reg', 'wsa_ttr6'],
      ['ttr6_tr4', 'reg', 'wsa_ttr6'],
      ['ttr6_tr5', 'reg', 'wsa_ttr6'],
      ['ttr6_area', 'area', 'wsa_ttr6_nop'],
      ['ttr6_balnus', 'balnus', 'wsa_ttr6_nop'],
      ['ttr6_jateng', 'jateng', 'wsa_ttr6_nop'],
      ['ttr6_jatim', 'jatim', 'wsa_ttr6_nop'],
      // TTR66
      ['ttr36_tif', 'tif', 'wsa_ttr36'],
      ['ttr36_district_tif3', 'tif', 'wsa_ttr36'],
      ['ttr36_nas', 'reg', 'wsa_ttr36'],
      ['ttr36_tr4', 'reg', 'wsa_ttr36'],
      ['ttr36_tr5', 'reg', 'wsa_ttr36'],
      ['ttr36_area', 'area', 'wsa_ttr36_nop'],
      ['ttr36_balnus', 'balnus', 'wsa_ttr36_nop'],
      ['ttr36_jateng', 'jateng', 'wsa_ttr36_nop'],
      ['ttr36_jatim', 'jatim', 'wsa_ttr36_nop'],
      // TTRmanja
      ['ttrmanja_tif', 'tif', 'wsa_ttrmanja'],
      ['ttrmanja_district_tif3', 'tif', 'wsa_ttrmanja'],
      ['ttrmanja_nas', 'reg', 'wsa_ttrmanja'],
      ['ttrmanja_tr4', 'reg', 'wsa_ttrmanja'],
      ['ttrmanja_tr5', 'reg', 'wsa_ttrmanja'],
      ['ttrmanja_area', 'area', 'wsa_ttrmanja_nop'],
      ['ttrmanja_balnus', 'balnus', 'wsa_ttrmanja_nop'],
      ['ttrmanja_jateng', 'jateng', 'wsa_ttrmanja_nop'],
      ['ttrmanja_jatim', 'jatim', 'wsa_ttrmanja_nop'],
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
