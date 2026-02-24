const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pool = require('./connection'); // Menggunakan pool dari mysql2/promise
const { insertDate } = require('./currentDate');

// 🔹 Fungsi hapus data berdasarkan tanggal saat ini
async function deleteExistingData() {
  const tableForDelete = ['ttr_non_numbering', 'q_datin', 'q_hsi'];
  const currentDate = insertDate;

  console.log('🗑️ Menghapus data lama untuk tanggal:', currentDate);
  for (const table of tableForDelete) {
    try {
      const sql = `DELETE FROM ${table} WHERE tgl = ?`;
      await pool.execute(sql, [currentDate]);
    } catch (err) {
      console.error(`❌ Error deleting data from ${table}:`, err.message);
      throw err;
    }
  }
}

// 🔹 Fungsi hapus data tidak diperlukan (Clean up)
async function deleteUnwantedRows(table) {
  const deleteWitelValues = ['TOTAL', 'REGIONAL', 'TERITORY', 'SUMBAR', 'AREA', ''];
  try {
    const sql = `DELETE FROM ${table} WHERE regional IN (?)`;
    await pool.query(sql, [deleteWitelValues]);
  } catch (err) {
    console.error(`❌ Error cleaning table ${table}:`, err.message);
    throw err;
  }
}

// 🔹 Fungsi untuk memasukkan data CSV ke database
function inputDataToDatabase(file, insertToTable, jenis) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/unspec_datin', `${file}.csv`);
    const rows = [];
    const tgl = insertDate;
    const bulan = new Date(tgl).getMonth() + 1;

    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️ File tidak ditemukan: ${file}.csv`);
      return resolve();
    }

    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',', headers: false }))
      .on('data', (data) => {
        const witel = data[Object.keys(data)[0]] || '';
        const real = data[Object.keys(data)[bulan]] || '';

        let newWitel = jenis === 'tif' ? witel.replace('TERRITORY ', 'TERRITORY 0') : witel.replace('REGIONAL ', 'REGIONAL 0');

        const lokasi = newWitel.replace('Telkom ', '');

        // Masukkan ke array rows untuk Bulk Insert
        rows.push([tgl, jenis, lokasi, real]);
      })
      .on('end', async () => {
        if (rows.length === 0) return resolve();

        try {
          const query = `INSERT INTO ${insertToTable} (tgl, jenis, regional, comply) VALUES ?`;
          await pool.query(query, [rows]);

          console.log(`✅ ${file}.csv -> ${insertToTable} (${rows.length} baris)`);
          await deleteUnwantedRows(insertToTable);
          resolve();
        } catch (err) {
          console.error(`❌ Gagal insert ke ${insertToTable}:`, err.message);
          reject(err);
        }
      })
      .on('error', (err) => {
        reject(err);
      });
  });
}

// 🔹 Jalankan proses utama
async function run() {
  try {
    await deleteExistingData();

    const files = [
      ['q_datin_tif', 'q_datin', 'tif'],
      ['q_datin_district', 'q_datin', 'tif'],
      ['q_datin_reg', 'q_datin', 'reg'],
      ['q_datin_reg4', 'q_datin', 'reg'],
      ['q_datin_reg5', 'q_datin', 'reg'],

      ['q_hsi_tif', 'q_hsi', 'tif'],
      ['q_hsi_district', 'q_hsi', 'tif'],
      ['q_hsi_reg', 'q_hsi', 'reg'],
      ['q_hsi_reg4', 'q_hsi', 'reg'],
      ['q_hsi_reg5', 'q_hsi', 'reg'],

      ['ttr_datin_tif', 'ttr_non_numbering', 'tif'],
      ['ttr_datin_district', 'ttr_non_numbering', 'tif'],
      ['ttr_datin_reg', 'ttr_non_numbering', 'reg'],
      ['ttr_datin_reg4', 'ttr_non_numbering', 'reg'],
      ['ttr_datin_reg5', 'ttr_non_numbering', 'reg'],
    ];

    for (const [file, insertToTable, jenis] of files) {
      await inputDataToDatabase(file, insertToTable, jenis);
    }

    console.log('🚀 Semua data TTR dan Quality berhasil diperbarui!');
  } catch (err) {
    console.error('💥 Error proses utama:', err.message);
  } finally {
    // Jika script mandiri (cron), aktifkan baris di bawah:
    await pool.end();
  }
}

run();
