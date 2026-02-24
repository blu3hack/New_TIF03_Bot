const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pool = require('./connection'); // Menggunakan pool dari mysql2/promise
const { insertDate } = require('./currentDate');

// 🔹 Fungsi hapus data berdasarkan tanggal saat ini (Async/Await)
async function deleteExistingData() {
  const tableForDelete = ['list_datin', 'list_hsi', 'tiket_open_datin', 'tiket_open_hsi'];
  const currentDate = insertDate;

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

// 🔹 Fungsi hapus data tidak diperlukan
async function deleteUnwantedRows(table) {
  const deleteWitelValues = ['TOTAL', 'REGIONAL', 'TERITORY', 'SUMBAR', 'AREA', ''];
  const sql = `DELETE FROM ${table} WHERE regional IN (?)`;

  try {
    // Menggunakan IN (?) lebih efisien daripada looping kueri DELETE
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
      console.warn(`⚠️ File skipped (not found): ${file}.csv`);
      return resolve();
    }

    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',', headers: false }))
      .on('data', (data) => {
        const witel = data[Object.keys(data)[0]] || '';
        let real = '';

        // Logika pengambilan kolom real
        if (file.includes('list_')) {
          real = data[Object.keys(data)[bulan]] || '';
        } else {
          real = data[Object.keys(data)[10]] || '';
        }

        // Transformasi Nama Witel/Regional
        let newWitel = jenis === 'tif' ? witel.replace('TERRITORY ', 'TERRITORY 0') : witel.replace('REGIONAL ', 'REGIONAL 0');

        const lokasi = newWitel.replace('Telkom ', '');

        // Simpan ke array untuk bulk insert
        rows.push([tgl, jenis, lokasi, real]);
      })
      .on('end', async () => {
        if (rows.length === 0) return resolve();

        try {
          // 🚀 BULK INSERT: Jauh lebih cepat daripada satu-satu
          const query = `INSERT INTO ${insertToTable} (tgl, jenis, regional, comply) VALUES ?`;
          await pool.query(query, [rows]);

          console.log(`✅ Berhasil insert ${rows.length} baris ke ${insertToTable}`);
          await deleteUnwantedRows(insertToTable);
          resolve();
        } catch (err) {
          console.error(`❌ Error bulk insert ke ${insertToTable}:`, err.message);
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
    console.log('🔄 Memulai sinkronisasi data...');
    await deleteExistingData();
    console.log('🧹 Data lama pada tanggal tersebut telah dibersihkan.');

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
      console.log(`📥 Memproses: ${file}.csv`);
      await inputDataToDatabase(file, insertToTable, jenis);
    }

    console.log('🚀 Semua proses selesai dengan sukses!');
  } catch (err) {
    console.error('💥 Terjadi kesalahan fatal:', err.message);
  } finally {
    // Jangan panggil pool.end() jika ini dijalankan oleh scheduler yang sama dengan server
    // Namun jika ini script mandiri (cron job), aktifkan baris di bawah:
    await pool.end();
  }
}

run();
