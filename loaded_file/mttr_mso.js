const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const connection = require('./connection');
const { insertDate } = require('../currentDate');

// 🔹 Fungsi hapus data berdasarkan tanggal saat ini
function deleteExistingData() {
  const tableForDelete = ['mttr_mso'];
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
function inputDataToDatabase(file, insertToTable) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'mttr_mso', `${file}.csv`);

    const insertPromises = [];

    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',', headers: false }))
      .on('data', (data) => {
        const tgl = insertDate; // misalnya '2025-11-07'
        // const bulan = new Date(tgl).getMonth() + 1; // +1 karena getMonth() hasilnya 0–11
        const witel = data[Object.keys(data)[0]] || '';
        const critical = data[Object.keys(data)[1]] || '';
        const low = data[Object.keys(data)[2]] || '';
        const major = data[Object.keys(data)[3]] || '';
        const minor = data[Object.keys(data)[4]] || '';
        const premium = data[Object.keys(data)[5]] || '';
        const jenis = data[Object.keys(data)[6]] || '';

        const query = `INSERT INTO ${insertToTable} (tgl, jenis, regional, critical, low, major, minor, premium) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;

        // Simpan semua query ke dalam array Promise
        insertPromises.push(
          new Promise((resolveInsert, rejectInsert) => {
            connection.query(query, [tgl, jenis, witel, critical, low, major, minor, premium], (err) => {
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

    const files = [['mttr_mso', 'mttr_mso']];

    for (const [file, insertToTable] of files) {
      console.log(`📥 Memproses file: ${file}.csv`);
      await inputDataToDatabase(file, insertToTable);
    }
  } catch (err) {
    console.error('❌ Terjadi kesalahan saat memproses data:', err);
  } finally {
    connection.end();
  }
}

// Jalankan
run();
