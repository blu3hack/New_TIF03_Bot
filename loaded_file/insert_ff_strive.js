const mysql = require('mysql2');
const connection = require('./connection');
const { MaxDate } = require('../currentDate');

// Tabel yang akan dihapus terlebih dahulu
const tableForDelete = ['ff_strive'];
const currentDate = MaxDate;

async function deleteData() {
  return new Promise((resolve, reject) => {
    tableForDelete.forEach((table, index) => {
      const sql = `DELETE FROM ${table} WHERE tgl = ?`;
      connection.query(sql, [currentDate], (err, results) => {
        if (err) {
          console.error(`Error deleting data from ${table}:`, err);
          return reject(err);
        }
        console.log(`Deleted ${results.affectedRows} rows from ${table}`);
        if (index === tableForDelete.length - 1) resolve();
      });
    });
  });
}

function insert_ff(jenis, fromTable, finalTable) {
  return new Promise((resolve, reject) => {
    const today = currentDate;
    console.log(today);

    connection.query(`SELECT lokasi, tti_real, ffg_real, ttr_ffg_real FROM ${fromTable} GROUP BY lokasi`, (error, rows) => {
      if (error) {
        console.error('Gagal mengambil data:', error);
        return reject(error);
      }

      if (rows.length === 0) {
        console.log(`Tidak ada data di ${fromTable} untuk dipindahkan.`);
        return resolve();
      }

      let completed = 0;
      rows.forEach((row) => {
        let lokasi = row.lokasi;
        // let witel; // deklarasi di sini

        // Perbaiki lokasi berdasarkan jenis
        if (jenis == 'reg') {
          lokasi = lokasi.replace('TREG ', 'REGIONAL 0');
        } else {
          lokasi = lokasi.replace('TERRITORY ', 'TERRITORY 0');
        }

        connection.query(`INSERT INTO ${finalTable} (tgl, jenis, lokasi, ttic, ffg, ttr_ffg) VALUES (?, ?, ?, ?, ?, ?)`, [today, jenis, lokasi, row.tti_real, row.ffg_real, row.ttr_ffg_real], (error) => {
          if (error) {
            console.error('Gagal memasukkan data:', error);
          }
          completed++;
          if (completed === rows.length) {
            console.log(`Data berhasil dipindahkan dari ${fromTable} ke ${finalTable}.`);
            resolve();
          }
        });
      });
    });
  });
}

async function main() {
  try {
    await deleteData(); // Hapus data terlebih dahulu
    await insert_ff('reg', 'kpi_endstate_monthly_reg', 'ff_strive');
    await insert_ff('tif', 'kpi_endstate_monthly_tif', 'ff_strive');
    console.log('Semua proses selesai, siap melanjutkan program berikutnya.');
  } catch (error) {
    console.error('Terjadi kesalahan:', error);
  } finally {
    connection.end(); // Tutup koneksi setelah semua proses selesai
  }
}
main();
