const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pool = require('./connection'); // Pastikan ini mengarah ke file pool Anda
const { insertDate } = require('./currentDate');

// Fungsi untuk menghapus data pada tabel tujuan
async function deleteExistingData() {
  const tableForDelete = ['helper_service_list_berbil', 'helper_service_list_gangguan', 'helper_sugar_list_berbil', 'helper_sugar_list_gangguan'];

  console.log('⏳ Membersihkan tabel lama...');
  for (const table of tableForDelete) {
    const sql = `DELETE FROM ${table}`;
    await pool.query(sql); // Menggunakan promise
  }
}

// Fungsi untuk memasukkan data dari CSV ke database
function inputDataToDatabase(file, jenis, insertToTable) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/service_sugar', `${file}.csv`);
    const tgl = insertDate;
    const rows = [];

    // Cek apakah file ada sebelum dibaca
    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️ File tidak ditemukan: ${filePath}`);
      return resolve();
    }

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => {
        // Ambil value secara dinamis berdasarkan urutan kolom
        const values = Object.values(data);
        rows.push([
          tgl,
          values[0] || '', // witel
          values[1] || '', // m01
          values[2] || '', // m02
          values[3] || '', // m03
          values[4] || '', // m04
          values[5] || '', // m05
          values[6] || '', // m06
          values[7] || '', // m07
          values[8] || '', // m08
          values[9] || '', // m09
          values[10] || '', // m10
          values[11] || '', // m11
          values[12] || '', // m12
        ]);
      })
      .on('end', async () => {
        if (rows.length === 0) {
          return resolve();
        }

        const query = `INSERT INTO ${insertToTable} 
          (tgl, witel, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12) 
          VALUES ?`;

        try {
          await pool.query(query, [rows]);
          console.log(`✅ ${file} -> ${insertToTable} (${rows.length} baris)`);
          await deleteUnwantedRows(insertToTable);
          resolve();
        } catch (err) {
          console.error(`❌ Error insert ke ${insertToTable}:`, err.message);
          reject(err);
        }
      })
      .on('error', (err) => {
        reject(err);
      });
  });
}

// Fungsi untuk menghapus baris TOTAL/TARGET
async function deleteUnwantedRows(table) {
  const deleteWitelValues = ['TOTAL', 'TARGET'];
  for (const value of deleteWitelValues) {
    const sql = `DELETE FROM ${table} WHERE witel = ?`;
    await pool.execute(sql, [value]);
  }
}

// Fungsi Utama
async function run() {
  try {
    await deleteExistingData();

    const files = [
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

    console.log('🚀 Semua proses selesai!');
  } catch (err) {
    console.error('💥 Terjadi kesalahan fatal:', err);
  } finally {
    // Opsional: Tutup pool jika script hanya dijalankan sekali (misal cron)
    await pool.end();
  }
}

run();
