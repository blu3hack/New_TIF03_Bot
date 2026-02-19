const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const connection = require('./connection');
const { insertDate } = require('../currentDate');

// 🔹 Fungsi hapus data berdasarkan tanggal saat ini
function deleteExistingData() {
  const tableForDelete = ['list_datin', 'list_hsi', 'tiket_open_datin', 'tiket_open_hsi'];
  const currentDate = insertDate;

  const deletePromises = tableForDelete.map((table) => {
    return new Promise((resolve, reject) => {
      const sql = `DELETE FROM ${table} WHERE tgl = ?`;
      connection.query(sql, [currentDate], (err) => {
        if (err) {
          console.error(`❌ Error deleting data from ${table}:`, err);
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });

  return Promise.all(deletePromises);
}

// 🔹 Fungsi hapus data tidak diperlukan
function deleteUnwantedRows(table) {
  const deleteWitelValues = ['TOTAL', 'REGIONAL', 'TERITORY', 'SUMBAR', 'AREA', ''];
  const deletePromises = deleteWitelValues.map((value) => {
    return new Promise((resolve, reject) => {
      const sql = `DELETE FROM ${table} WHERE regional = ?`;
      connection.query(sql, [value], (err) => {
        if (err) {
          console.error(`❌ Error deleting "${value}" from ${table}:`, err);
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });

  return Promise.all(deletePromises);
}

// 🔹 Fungsi untuk memasukkan data CSV ke database
function inputDataToDatabase(file, insertToTable, jenis) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/unspec_datin', `${file}.csv`);

    const insertPromises = [];

    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',', headers: false }))
      .on('data', (data) => {
        const tgl = insertDate; // misalnya '2025-11-07'
        const bulan = new Date(tgl).getMonth() + 1; // +1 karena getMonth() hasilnya 0–11
        const witel = data[Object.keys(data)[0]] || '';
        let real = '';

        // Ambil kolom real berdasarkan tipe file
        if (file.includes('list_')) {
          real = data[Object.keys(data)[bulan]] || '';
        } else {
          real = data[Object.keys(data)[10]] || '';
        }

        let newWitel = '';
        if (jenis === 'tif') {
          newWitel = witel.replace('TERRITORY ', 'TERRITORY 0');
        } else {
          newWitel = witel.replace('REGIONAL ', 'REGIONAL 0');
        }

        const lokasi = newWitel.replace('Telkom ', '');

        const query = `INSERT INTO ${insertToTable} (tgl, jenis, regional, comply) VALUES (?, ?, ?, ?)`;

        // Simpan semua query ke dalam array Promise
        insertPromises.push(
          new Promise((resolveInsert, rejectInsert) => {
            connection.query(query, [tgl, jenis, lokasi, real], (err) => {
              if (err) {
                console.error(`❌ Error inserting data ke ${insertToTable}:`, err);
                rejectInsert(err);
              } else {
                resolveInsert();
              }
            });
          }),
        );
      })
      .on('end', async () => {
        try {
          await Promise.all(insertPromises);
          await deleteUnwantedRows(insertToTable);
          resolve();
        } catch (err) {
          reject(err);
        }
      })
      .on('error', (err) => {
        console.error(`❌ Gagal membuka file CSV ${file}:`, err);
        reject(err);
      });
  });
}

// 🔹 Jalankan proses utama
async function run() {
  try {
    console.log('🔄 Menghapus data lama...');
    await deleteExistingData();

    const files = [
      ['list_datin_tif', 'list_datin', 'tif'],
      ['list_datin_district', 'list_datin', 'tif'],
      ['list_datin_reg', 'list_datin', 'reg'],
      ['list_datin_reg4', 'list_datin', 'reg'],
      ['list_datin_reg5', 'list_datin', 'reg'],

      ['unspec_datin_tif', 'tiket_open_datin', 'tif'],
      ['unspec_datin_district', 'tiket_open_datin', 'tif'],
      ['unspec_datin_reg', 'tiket_open_datin', 'reg'],
      ['unspec_datin_reg4', 'tiket_open_datin', 'reg'],
      ['unspec_datin_reg5', 'tiket_open_datin', 'reg'],

      ['list_hsi_tif', 'list_hsi', 'tif'],
      ['list_hsi_district', 'list_hsi', 'tif'],
      ['list_hsi_reg', 'list_hsi', 'reg'],
      ['list_hsi_reg4', 'list_hsi', 'reg'],
      ['list_hsi_reg5', 'list_hsi', 'reg'],

      ['unspec_hsi_tif', 'tiket_open_hsi', 'tif'],
      ['unspec_hsi_district', 'tiket_open_hsi', 'tif'],
      ['unspec_hsi_reg', 'tiket_open_hsi', 'reg'],
      ['unspec_hsi_reg4', 'tiket_open_hsi', 'reg'],
      ['unspec_hsi_reg5', 'tiket_open_hsi', 'reg'],
    ];

    for (const [file, insertToTable, jenis] of files) {
      console.log(`📥 Memproses file: ${file}.csv`);
      await inputDataToDatabase(file, insertToTable, jenis);
    }
  } catch (err) {
    console.error('❌ Terjadi kesalahan saat memproses data:', err);
  } finally {
    connection.end();
  }
}

// Jalankan
run();
