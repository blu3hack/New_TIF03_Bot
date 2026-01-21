const connection = require('./connection');
const fs = require('fs');
const path = require('path');

const table = ['kpi_endstate_monthly_reg', 'kpi_endstate_monthly_tif', 'kpi_msa_2025_reg', 'kpi_msa_2025_tif', 'wifi_revi_reg'];
async function deleteTables() {
  try {
    const deletePromises = table.map((tbl) => {
      return new Promise((resolve, reject) => {
        const query = `DELETE FROM ${tbl}`;
        connection.query(query, (error, results) => {
          if (error) {
            console.error(`Gagal menghapus data di tabel ${tbl}:`, error);
            reject(error);
          } else {
            console.log(`Berhasil menghapus table ${tbl}`);
            resolve(results);
          }
        });
      });
    });
    await Promise.all(deletePromises);
  } catch (error) {
    console.error('Terjadi kesalahan:', error);
  } finally {
    connection.end();
  }
}

function clearFolder(folderPath) {
  fs.readdir(folderPath, (err, files) => {
    if (err) {
      console.error(`Gagal membaca folder: ${err.message}`);
      return;
    }

    files.forEach((file) => {
      const filePath = path.join(folderPath, file);
      fs.stat(filePath, (err, stats) => {
        if (err) {
          console.error(`Gagal membaca file: ${err.message}`);
          return;
        }

        if (stats.isDirectory()) {
          fs.rm(filePath, { recursive: true, force: true }, (err) => {
            if (err) {
              console.error(`Gagal menghapus folder: ${err.message}`);
            }
          });
        } else {
          fs.unlink(filePath, (err) => {
            if (err) {
              console.error(`Gagal menghapus file: ${err.message}`);
            }
          });
        }
      });
    });
  });
}

clearFolder('./asr_wifi');
clearFolder('./asr_datin');
clearFolder('./wsa');
clearFolder('./wsa_gamas');
clearFolder('./download_fulfillment');
clearFolder('./ff_non_hsi');
clearFolder('./wifi_revi');
clearFolder('./cnop');
clearFolder('./ps_re');
clearFolder('./msa_upload');
clearFolder('./unspec_datin');
deleteTables();
