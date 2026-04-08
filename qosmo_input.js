const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const pool = require('./connection'); // Menggunakan pool dari mysql2/promise
const { insertDate } = require('./currentDate');

// 🔹 1. Fungsi Hapus Data Lama (Async)
async function deleteExistingData() {
  const tableForDelete = ['qosmo_packetloss_kurang_lima', 'qosmo_packetloss_satu_sampai_lima'];
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
async function deleteUnwantedRows() {
  const tableForDelete = ['qosmo_packetloss_kurang_lima', 'qosmo_packetloss_satu_sampai_lima'];
  const deleteWitelValues = ['JAWA TENGAH', 'JAWA TIMUR', 'BALI NUSRA'];

  for (const table of tableForDelete) {
    try {
      const sql = `DELETE FROM ${table} WHERE regional NOT IN (?)`;
      await pool.query(sql, [deleteWitelValues]);
    } catch (err) {
      console.error(`❌ Error deleting data from ${table}:`, err.message);
      throw err;
    }
  }
}

function inputDataToDatabase(file, insertToTable, jenis) {
  return new Promise((resolve, reject) => {
    const filePath = path.join(__dirname, 'loaded_file/qosmo', `${file}.csv`);
    const rows = [];
    const tgl = insertDate;
    let avgIndex = -1;

    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️ File tidak ditemukan: ${file}.csv`);
      return resolve();
    }

    fs.createReadStream(filePath)
      .pipe(csv({ separator: ',', headers: false }))
      .on('data', (data) => {
        // Gunakan trim() untuk menghapus spasi liar di awal/akhir string
        const firstCol = (data[1] || '').trim();
        // 2. FILTER Baris Sampah

        // 3. AMBIL DATA
        const rawTerritory = firstCol;
        const real = data[15] || '0';

        // 4. TRANSFORMASI (Gunakan 'let' agar bisa diubah)
        let lokasi = rawTerritory;

        if (rawTerritory === 'SUMATERA') {
          lokasi = 'TERRITORY 01';
        } else if (rawTerritory === 'JABODETABEK JABAR') {
          lokasi = 'TERRITORY 02';
        } else if (rawTerritory === 'JAWA BALI') {
          lokasi = 'TERRITORY 03';
        } else if (rawTerritory === 'PAMASUKA') {
          lokasi = 'TERRITORY 04';
        }

        if (rawTerritory === 'JATIM') lokasi = 'JAWA TIMUR';
        if (rawTerritory === 'BALINUSRA') lokasi = 'BALI NUSRA';

        // 5. Simpan ke penampung
        rows.push([tgl, jenis, lokasi, real]);
      })
      .on('end', async () => {
        if (rows.length === 0) {
          console.log(`ℹ️ Tidak ada data untuk diproses di ${file}.csv`);
          return resolve();
        }

        try {
          // Pastikan kolom di database sesuai: tgl, jenis, regional, comply
          const query = `INSERT INTO ${insertToTable} (tgl, jenis, regional, comply) VALUES ?`;
          await pool.query(query, [rows]);
          console.log(`✅ ${file}.csv -> ${insertToTable} (${rows.length} baris)`);
          resolve();
        } catch (err) {
          console.error(`❌ Gagal bulk insert:`, err.message);
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
    await deleteExistingData();
    const files = [
      ['packetloss_kurang_lima', 'qosmo_packetloss_kurang_lima', 'tif'],
      ['packetloss_satu_sampai_lima', 'qosmo_packetloss_satu_sampai_lima', 'tif'],
    ];
    for (const [file, insertToTable, jenis] of files) {
      console.log(`📥 Memproses: ${file}.csv`);
      await inputDataToDatabase(file, insertToTable, jenis);
    }
    await deleteUnwantedRows('qosmo_packetloss_kurang_lima');
    await deleteUnwantedRows('qosmo_packetloss_satu_sampai_lima');
    console.log('🏁 Semua data SQM berhasil diperbarui!');
  } catch (err) {
    console.error('💥 Error pada proses utama SQM:', err.message);
  } finally {
    await pool.end();
    console.log('🔒 Koneksi database ditutup.');
  }
}
run();
