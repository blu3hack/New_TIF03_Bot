const fs = require('fs');
const path = require('path');
const connection = require('./connection');
const csv = require('csv-parser');
const { insertDate } = require('../currentDate');

function fulfillment_ih_tif() {
  const filePath = path.join(__dirname, 'wsa', 'wsa_fulfillment_tif.csv').replace(/\\/g, '/');
  const query = `
  LOAD DATA LOCAL INFILE ?
  INTO TABLE helper_fulfillment
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  (blank, lokasi, tti_comply, tti_not_comply, tti_ps_ih, tti_real, tti_ach, ffg_comply, ffg_not_comply, ffg_jml_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_ggn_wsa, ttr_ffg_real, ttr_ffg_ach, ttr_ffg_wilsus, ttr_ffg_ggn)`;

  connection.query({
    sql: query,
    values: [filePath],
    infileStreamFactory: (path) => fs.createReadStream(path), // StreamFactory untuk membaca file CSV
  });
  console.log(`wsa_fulfillment_tif.csv Berhasil Di input ke Database`);
}

fulfillment_ih_tif();
console.log('Program selesai.');
connection.end();
