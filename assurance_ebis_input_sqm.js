const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pool = require('./connection'); // Menggunakan pool dari mysql2/promise
const { insertDate } = require('./currentDate');

// 🔹 1. Fungsi Hapus Data Lama (Async)
async function deleteExistingData() {
  const tableForDelete = ['sqm_datin', 'sqm_hsi'];
  const currentDate = insertDate;

  console.log('🗑️ Menghapus data lama SQM untuk tanggal:', currentDate);
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

// 🔹 2. Fungsi Hapus Baris Tidak Diperlukan
async function deleteUnwantedRows(table) {
  const deleteWitelValues = ['TOTAL', 'REGIONAL', 'TERITORY', 'SUMBAR', 'AREA', ''];
  try {
    // Menggunakan pool.query agar array deleteWitelValues dipetakan ke operator IN
    const sql = `DELETE FROM ${table} WHERE regional IN (?)`;
    await pool.query(sql, [deleteWitelValues]);
  } catch (err) {
    console.error(`❌ Error cleaning table ${table}:`, err.message);
    throw err;
  }
}

// 🔹 3. Fungsi Input CSV ke Database (Bulk Insert)
function inputDataToDatabase(file, insertToTable, jenis) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/unspec_datin', `${file}.csv`);
    const rows = [];
    const tgl = insertDate;

    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️ File tidak ditemukan: ${file}.csv`);
      return resolve();
    }

    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',', headers: false }))
      .on('data', (data) => {
        // Mengambil kolom pertama untuk Witel
        const witel = data[Object.keys(data)[0]] || '';
        // Mengambil kolom ke-16 (indeks 16) untuk nilai realisasi/comply
        const real = data[Object.keys(data)[16]] || '';

        // Transformasi penamaan Regional/Territory
        let newWitel = jenis === 'tif' ? witel.replace('TERRITORY ', 'TERRITORY 0') : witel.replace('REGIONAL ', 'REGIONAL 0');

        const lokasi = newWitel.replace('Telkom ', '');

        // Tambahkan ke penampung baris
        rows.push([tgl, jenis, lokasi, real]);
      })
      .on('end', async () => {
        if (rows.length === 0) return resolve();

        try {
          // Eksekusi Bulk Insert
          const query = `INSERT INTO ${insertToTable} (tgl, jenis, regional, comply) VALUES ?`;
          await pool.query(query, [rows]);

          console.log(`✅ ${file}.csv -> ${insertToTable} (${rows.length} baris)`);

          // Bersihkan data sampah setelah insert selesai
          await deleteUnwantedRows(insertToTable);
          resolve();
        } catch (err) {
          console.error(`❌ Gagal bulk insert ke ${insertToTable}:`, err.message);
          reject(err);
        }
      })
      .on('error', (err) => {
        reject(err);
      });
  });
}

// 🔹 4. Jalankan Proses Utama
async function run() {
  try {
    console.log('🚀 Memulai update data SQM...');

    // Hapus data lama dulu
    await deleteExistingData();

    // Daftar file yang akan diproses
    const files = [
      ['sqm_datin_tif', 'sqm_datin', 'tif'],
      ['sqm_datin_district', 'sqm_datin', 'tif'],
      ['sqm_datin_reg', 'sqm_datin', 'reg'],
      ['sqm_datin_reg4', 'sqm_datin', 'reg'],
      ['sqm_datin_reg5', 'sqm_datin', 'reg'],

      ['sqm_hsi_tif', 'sqm_hsi', 'tif'],
      ['sqm_hsi_district', 'sqm_hsi', 'tif'],
      ['sqm_hsi_reg', 'sqm_hsi', 'reg'],
      ['sqm_hsi_reg4', 'sqm_hsi', 'reg'],
      ['sqm_hsi_reg5', 'sqm_hsi', 'reg'],
    ];

    // Proses file satu per satu secara sekuensial
    for (const [file, insertToTable, jenis] of files) {
      console.log(`📥 Memproses: ${file}.csv`);
      await inputDataToDatabase(file, insertToTable, jenis);
    }

    console.log('🏁 Semua data SQM berhasil diperbarui!');
  } catch (err) {
    console.error('💥 Error pada proses utama SQM:', err.message);
  } finally {
    // Menutup pool koneksi
    await pool.end();
    console.log('🔒 Koneksi database ditutup.');
  }
}

// Eksekusi
run();
