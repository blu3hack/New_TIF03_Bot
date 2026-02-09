const mysql = require('mysql2');
const fs = require('fs');
const path = require('path');
const connection = require('./connection');
const csv = require('csv-parser');
const { insertDate } = require('../currentDate');

const tableForDelete = ['wsa_gamas'];
const currentDate = insertDate;

// Fungsi untuk menghapus data berdasarkan tanggal hari ini
function deleteExistingData() {
  return Promise.all(
    tableForDelete.map((table) => {
      return new Promise((resolve, reject) => {
        connection.query(`DELETE FROM ${table} WHERE tgl = ?`, [currentDate], (err, results) => {
          if (err) {
            console.error(`Error deleting data: ${err.message}`);
            reject(err);
          } else {
            console.log(`Deleted existing data from ${table}`);
            resolve(results);
          }
        });
      });
    }),
  );
}

// Fungsi untuk memasukkan data dari CSV ke database
function inputDataToDatabase(file, jenis, insertToTable) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, `wsa_gamas/${file}.csv`);
    let currentLabel = '';
    let queries = [];

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        const witel = (row[Object.keys(row)[0]] || '').replace(/[^\x20-\x7E]/g, '');
        const jml_tiket = row[Object.keys(row)[1]] || '0';
        const mttr = row[Object.keys(row)[2]] || '0';
        const comply = row[Object.keys(row)[3]] || '0';
        const persen_comply = row[Object.keys(row)[4]] || '0';

        jenis = jenis.replace('tr4', 'witel').replace('tr5', 'witel');
        lokasi = witel.replace('TERITORY', 'TERRITORY');

        if (/DISTRIBUSI/i.test(lokasi)) {
          currentLabel = 'DISTRIBUSI';
          return;
        } else if (/FEEDER/i.test(lokasi)) {
          currentLabel = 'FEEDER';
          return;
        } else if (/ODC/i.test(lokasi)) {
          currentLabel = 'ODC';
          return;
        } else if (/ODP/i.test(lokasi)) {
          currentLabel = 'ODP';
          return;
        }

        const query = `INSERT INTO ${insertToTable} (jenis, kat, teritori, jml_tiket, mttr, comply, persen_comply, tgl) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
        const values = [jenis, currentLabel, lokasi, jml_tiket, mttr, comply, persen_comply, currentDate];

        queries.push(
          new Promise((resolve, reject) => {
            connection.query(query, values, (err) => {
              if (err) {
                console.error(`Error inserting data: ${err.message}`);
                reject(err);
              } else {
                resolve();
              }
            });
          }),
        );
      })
      .on('end', () => {
        Promise.all(queries)
          .then(() => {
            console.log(`${file} berhasil diinput ke table ${insertToTable}`);
            resolve();
          })
          .catch(reject);
      })
      .on('error', reject);
  });
}

// Panggil fungsi

// Eksekusi semua fungsi dan hentikan proses setelah selesai
async function main() {
  try {
    await deleteExistingData(); // Hapus data lama sebelum memasukkan data baru

    // Jalankan satu per satu secara berurutan
    await inputDataToDatabase('segmen_tif', 'tif', 'wsa_gamas');
    await inputDataToDatabase('segmen_district', 'tif', 'wsa_gamas');
    await inputDataToDatabase('segmen_reg', 'reg', 'wsa_gamas');
    await inputDataToDatabase('segmen_tr4', 'reg', 'wsa_gamas');
    await inputDataToDatabase('segmen_tr5', 'reg', 'wsa_gamas');
    console.log('WSA GAMAS selesai diProses');
    connection.end();
    process.exit(0);
  } catch (err) {
    console.error('Terjadi kesalahan:', err);
    process.exit(1);
  }
}
main();
