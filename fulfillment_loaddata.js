const fs = require('fs');
const path = require('path');
const connection = require('./connection');
const { insertDate } = require('./currentDate');

async function delete_existing() {
  try {
    const table_for_delete = ['helper_fulfillment', 'helper_fulfillment_hsi'];

    for (const table of table_for_delete) {
      const query = `delete from ${table}`;
      await connection.query({
        sql: query,
      });
      console.log(`table ${table} Berhasil di hapus`);
    }
  } catch (err) {
    console.error('Terjadi kesalahan:', err);
  }
}

async function fulfillment() {
  try {
    const filePath = path.join(__dirname, 'loaded_file/wsa', 'wsa_fulfillment_tif.csv').replace(/\\/g, '/');

    const query = `
      LOAD DATA LOCAL INFILE ?
      INTO TABLE helper_fulfillment
      FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
      LINES TERMINATED BY '\n'
      (blank, lokasi, tti_comply, tti_not_comply, tti_ps_ih, tti_real, tti_ach, ffg_comply, ffg_not_comply, ffg_jml_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_ggn_wsa, ttr_ffg_real, ttr_ffg_ach, ttr_ffg_wilsus, ttr_ffg_ggn)
      SET insert_at = CURRENT_TIMESTAMP;
    `;

    await connection.query({
      sql: query,
      values: [filePath],
      infileStreamFactory: (filePath) => fs.createReadStream(filePath),
    });

    console.log(`wsa_fulfillment_tif.csv Berhasil Di input ke Database`);
  } catch (err) {
    console.error('Terjadi kesalahan:', err);
  }
}

async function fulfillment_hsi() {
  try {
    const filePath = path.join(__dirname, 'loaded_file/wsa', 'wsa_fulfillment_hsi_tif.csv').replace(/\\/g, '/');

    const query = `
      LOAD DATA LOCAL INFILE ?
      INTO TABLE helper_fulfillment_hsi
      FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
      LINES TERMINATED BY '\n'
      (blank, lokasi, tti_comply, ttic_not_comply, tti_jml_ps, tti_real, tti_ach,
       ffg_not_comply, ffg_avg_ps, ffg_real, ffg_ach,
       ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_jml_ggn, ttr_ffg_real, ttr_ffg_ach,
       pspi_jml_ps, pspi_jml_pi, pspi_saldo, pspi_real, pspi_ach,
       unspec, unspec_jml, unspec_saldo, unspec_real, unspec_ach)
      SET insert_at = CURRENT_TIMESTAMP;
    `;

    await connection.query({
      sql: query,
      values: [filePath],
      infileStreamFactory: (filePath) => fs.createReadStream(filePath),
    });

    console.log(`wsa_fulfillment_hsi_tif.csv Berhasil Di input ke Database`);
  } catch (err) {
    console.error('Terjadi kesalahan:', err);
  }
}

async function main() {
  await delete_existing();
  await fulfillment();
  await fulfillment_hsi();
  console.log('Program selesai.');
  await connection.end(); // tutup setelah query benar-benar selesai
}
main();
