const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pool = require('./connection'); // Menggunakan pool dari mysql2/promise
const { insertDate } = require('./currentDate');

// 🔹 1. Fungsi Hapus Data Lama (Async)
async function deleteExistingData() {
  const tableForDelete = ['sugar_wifi', 'ttr_wifi'];
  const currentDate = insertDate;

  for (const table of tableForDelete) {
    try {
      const sql = `DELETE FROM ${table} WHERE tgl = ?`;
      await pool.execute(sql, [currentDate]);
      console.log(`🗑️ Data lama di ${table} untuk tanggal ${currentDate} telah dibersihkan.`);
    } catch (err) {
      console.error(`❌ Error deleting data from ${table}:`, err.message);
    }
  }
}

// 🔹 2. Fungsi Hapus Data Sampah (Async)
async function deleteUnwantedRows(table) {
  const deleteWitelValues = ['Regional', 'SUGAR (%)', 'Gaul', 'Nasional', 'Comply', 'Territory'];
  try {
    const sql = `DELETE FROM ${table} WHERE regional IN (?)`;
    await pool.query(sql, [deleteWitelValues]);
  } catch (err) {
    console.error(`❌ Error cleaning table ${table}:`, err.message);
  }
}

// 🔹 3. Fungsi Input CSV ke Database (Bulk Insert)
function inputDataToDatabase(file, jenis, insertToTable) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/asr_wifi', `${file}.csv`);
    const rows = [];
    const tgl = insertDate;

    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️ File tidak ditemukan: ${file}.csv`);
      return resolve();
    }

    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => {
        const witel = data[Object.keys(data)[0]] || '';

        // Penentuan kolom Comply berdasarkan file dan jenis
        let comp;
        if (file.includes('ttr') && jenis === 'tif') {
          comp = data[Object.keys(data)[5]] || '';
        } else {
          comp = data[Object.keys(data)[5]] || '';
        }

        // Transformasi Nama Lokasi
        let lokasi = witel
          .replace('REG-', 'REGIONAL 0')
          .replace('TERRITORY ', 'TERRITORY 0')
          .replace('JATIM BARAT', 'MALANG')
          .replace('JATIM TIMUR', 'SIDOARJO')
          .replace('SEMARANG JATENG UTARA', 'SEMARANG')
          .replace('SOLO JATENG TIMUR', 'SOLO')
          .replace('YOGJA JATENG SELATAN', 'YOGYAKARTA');

        rows.push([tgl, jenis, lokasi, comp]);
      })
      .on('end', async () => {
        if (rows.length === 0) return resolve();

        try {
          const query = `INSERT INTO ${insertToTable} (tgl, jenis, regional, comply) VALUES ?`;
          await pool.query(query, [rows]);

          console.log(`✅ ${file}.csv berhasil diinput ke ${insertToTable} (${rows.length} baris)`);
          await deleteUnwantedRows(insertToTable);
          resolve();
        } catch (err) {
          console.error(`❌ Gagal bulk insert ke ${insertToTable}:`, err.message);
          reject(err);
        }
      })
      .on('error', (err) => {
        console.error(`❌ Gagal membaca file ${file}:`, err.message);
        reject(err);
      });
  });
}

async function sugar_wifi() {
  const currentDate = insertDate;

  // SQL untuk agregasi Area CCM pada tabel sugar_wifi
  const sql = `
    INSERT INTO sugar_wifi (tgl, jenis, regional, comply)
    SELECT
      tgl,
      'area_ccm' AS jenis,
      'BALI NUSRA' AS regional,
      ROUND(AVG(NULLIF(comply, '-')), 2) AS comply
    FROM sugar_wifi
    WHERE tgl = ?
      AND jenis = 'reg'
      AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT')
    GROUP BY tgl

    UNION ALL

    SELECT
      tgl,
      'area_ccm' AS jenis,
      'JAWA TIMUR' AS regional,
      ROUND(AVG(NULLIF(comply, '-')), 2) AS comply
    FROM sugar_wifi
    WHERE tgl = ?
      AND jenis = 'reg'
      AND regional IN (
        'MADIUN','MALANG','JEMBER','SIDOARJO',
        'SURABAYA SELATAN','SURABAYA UTARA',
        'MADURA','PASURUAN'
      )
    GROUP BY tgl

    UNION ALL

    SELECT
      tgl,
      'area_ccm' AS jenis,
      'JATENG DIY' AS regional,
      ROUND(AVG(NULLIF(comply, '-')), 2) AS comply
    FROM sugar_wifi
    WHERE tgl = ?
      AND jenis = 'reg'
      AND regional IN (
        'KUDUS','MAGELANG','PEKALONGAN',
        'PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA'
      )
    GROUP BY tgl
  `;

  try {
    // Menjalankan query menggunakan pool.query (support await secara native)
    await pool.query(sql, [currentDate, currentDate, currentDate]);
    console.log('✅ Agregasi Area CCM ke table sugar_wifi berhasil.');
  } catch (err) {
    console.error('❌ Error insert sugar_wifi:', err.message);
  }
}

async function ttr_wifi(table) {
  const currentDate = insertDate;
  const sql = `
    INSERT INTO ${table} (tgl, jenis, regional, comply)
    SELECT tgl, 'area_ccm', 'BALI NUSRA', ROUND(AVG(NULLIF(comply, '-')), 2) FROM ${table}
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('DENPASAR', 'SINGARAJA', 'NTB', 'NTT') GROUP BY tgl
    UNION ALL
    SELECT tgl, 'area_ccm', 'JAWA TIMUR', ROUND(AVG(NULLIF(comply, '-')), 2) FROM ${table}
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('MADIUN','MALANG','JEMBER','SIDOARJO','SURABAYA SELATAN','SURABAYA UTARA','MADURA','PASURUAN') GROUP BY tgl
    UNION ALL
    SELECT tgl, 'area_ccm', 'JATENG DIY', ROUND(AVG(NULLIF(comply, '-')), 2) FROM ${table}
    WHERE tgl = ? AND jenis = 'reg' AND regional IN ('KUDUS','MAGELANG','PEKALONGAN','PURWOKERTO','SEMARANG','SOLO','YOGYAKARTA') GROUP BY tgl
  `;

  try {
    await pool.query(sql, [currentDate, currentDate, currentDate]);
    console.log(`✅ Insert ke table ${table} berhasil`);
  } catch (err) {
    console.error(`❌ Error insert ${table}:`, err.message);
  }
}

// 🔹 4. Jalankan Semua Proses (Sequential)
async function run() {
  try {
    console.log('🔄 Memulai proses update Sugar & TTR Wifi...');

    // Pastikan data lama dihapus dulu
    await deleteExistingData();

    // Jalankan satu per satu agar tidak terjadi race condition
    const tasks = [
      { file: 'sugar_regional', jenis: 'reg', table: 'sugar_wifi' },
      { file: 'sugar_teritory', jenis: 'tif', table: 'sugar_wifi' },
      { file: 'ttr_regional', jenis: 'reg', table: 'ttr_wifi' },
      { file: 'ttr_teritory', jenis: 'tif', table: 'ttr_wifi' },
    ];

    for (const task of tasks) {
      await inputDataToDatabase(task.file, task.jenis, task.table);
    }
    await sugar_wifi();
    await ttr_wifi('ttr_wifi');
    console.log('🏁 Seluruh proses Wifi ASR selesai!');
  } catch (err) {
    console.error('💥 Terjadi kesalahan fatal:', err.message);
  } finally {
    await pool.end();
    console.log('🔒 Koneksi database ditutup.');
  }
}

run();
