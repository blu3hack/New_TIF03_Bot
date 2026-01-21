const fs = require('fs');
const path = require('path');
const connection = require('./connection');
const csv = require('csv-parser');
const { insertDate } = require('../currentDate');

function fulfillment_hsi_reg() {
  const filePath = path.join(__dirname, 'wsa', 'wsa_fulfillment_hsi_reg.csv').replace(/\\/g, '/');
  const query = `
  LOAD DATA LOCAL INFILE ?
  INTO TABLE kpi_msa_2025_reg
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  (blank, lokasi, tti_comply, ttic_not_comply, tti_jml_ps, tti_real, tti_ach, ffg_not_comply, ffg_avg_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_jml_ggn, ttr_ffg_real, ttr_ffg_ach, pspi_jml_ps, pspi_jml_pi, pspi_saldo, pspi_real, pspi_ach, unspec, unspec_jml, unspec_saldo, unspec_real, unspec_ach)`;

  connection.query({
    sql: query,
    values: [filePath],
    infileStreamFactory: (path) => fs.createReadStream(path), // StreamFactory untuk membaca file CSV
  });
  console.log(`wsa_fulfillment_tif.csv Berhasil Di input ke Database`);

  // HAPUS DATA
  const searchStrings = [
    'TREG 1',
    'TREG 2',
    'TREG 3',
    'TREG 4',
    'TREG 5',
    'TREG 6',
    'TREG 7',
    'KUDUS',
    'MAGELANG',
    'PEKALONGAN',
    'PURWOKERTO',
    'SEMARANG',
    'SOLO',
    'YOGYAKARTA',
    'DENPASAR',
    'JEMBER',
    'KEDIRI',
    'MADIUN',
    'MADURA',
    'MALANG',
    'NTB',
    'NTT',
    'PASURUAN',
    'SIDOARJO',
    'SINGARAJA',
    'SURABAYA SELATAN',
    'SURABAYA UTARA',
  ];

  // Buat bagian WHERE dengan NOT LIKE untuk setiap kata
  // const whereClause = searchStrings.map(() => 'lokasi NOT LIKE ?').join(' AND ');
  // const sql = `DELETE FROM kpi_msa_2025_reg WHERE ${whereClause}`;

  // connection.query(sql, searchStrings, (err, results) => {
  //   if (err) throw err;
  //   console.log(`Deleted ${results.affectedRows} rows`);
  // });
  // connection.end();
}

function fulfillment_hsi_tif() {
  const filePath = path.join(__dirname, 'wsa', 'wsa_fulfillment_hsi_tif.csv').replace(/\\/g, '/');
  const query = `
  LOAD DATA LOCAL INFILE ?
  INTO TABLE kpi_msa_2025_tif
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  (blank, lokasi, tti_comply, ttic_not_comply, tti_jml_ps, tti_real, tti_ach, ffg_not_comply, ffg_avg_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_jml_ggn, ttr_ffg_real, ttr_ffg_ach, pspi_jml_ps, pspi_jml_pi, pspi_saldo, pspi_real, pspi_ach, unspec, unspec_jml, unspec_saldo, unspec_real, unspec_ach) SET insert_at = CURRENT_TIMESTAMP;`;

  connection.query({
    sql: query,
    values: [filePath],
    infileStreamFactory: (path) => fs.createReadStream(path), // StreamFactory untuk membaca file CSV
  });
  console.log(`wsa_fulfillment_tif.csv Berhasil Di input ke Database`);

  // HAPUS DATA
  const searchStrings = ['TERRITORY 1', 'TERRITORY 2', 'TERRITORY 3', 'BALI', 'MALANG', 'NUSA TENGGARA', 'SEMARANG', 'SIDOARJO', 'SOLO', 'SURAMADU', 'YOGYAKARTA', 'TERRITORY 4'];

  // Buat bagian WHERE dengan NOT LIKE untuk setiap kata
  // const whereClause = searchStrings.map(() => 'lokasi NOT LIKE ?').join(' AND ');
  // const sql = `DELETE FROM kpi_msa_2025_tif WHERE ${whereClause}`;

  // connection.query(sql, searchStrings, (err, results) => {
  //   if (err) throw err;
  //   console.log(`Deleted ${results.affectedRows} rows`);
  // });
  // connection.end();
}

function fulfillment_ih_reg() {
  const filePath = path.join(__dirname, 'wsa', 'wsa_fulfillment_reg.csv').replace(/\\/g, '/');
  const query = `
  LOAD DATA LOCAL INFILE ?
  INTO TABLE kpi_endstate_monthly_reg
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  (blank, lokasi, tti_comply, tti_not_comply, tti_ps_ih, tti_real, tti_ach, ffg_comply, ffg_not_comply, ffg_jml_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_ggn_wsa, ttr_ffg_real, ttr_ffg_ach, ttr_ffg_wilsus, ttr_ffg_ggn)  SET insert_at = CURRENT_TIMESTAMP;`;

  connection.query({
    sql: query,
    values: [filePath],
    infileStreamFactory: (path) => fs.createReadStream(path), // StreamFactory untuk membaca file CSV
  });
  console.log(`wsa_fulfillment.csv Berhasil Di input ke Database`);

  // HAPUS DATA
  const searchStrings = [
    'TREG 1',
    'TREG 2',
    'TREG 3',
    'TREG 4',
    'TREG 5',
    'TREG 6',
    'TREG 7',
    'KUDUS',
    'MAGELANG',
    'PEKALONGAN',
    'PURWOKERTO',
    'SEMARANG',
    'SOLO',
    'YOGYAKARTA',
    'DENPASAR',
    'JEMBER',
    'KEDIRI',
    'MADIUN',
    'MADURA',
    'MALANG',
    'NTB',
    'NTT',
    'PASURUAN',
    'SIDOARJO',
    'SINGARAJA',
    'SURABAYA SELATAN',
    'SURABAYA UTARA',
  ];

  // Buat bagian WHERE dengan NOT LIKE untuk setiap kata
  const whereClause = searchStrings.map(() => 'lokasi NOT LIKE ?').join(' AND ');
  const sql = `DELETE FROM kpi_endstate_monthly_reg WHERE ${whereClause}`;

  connection.query(sql, searchStrings, (err, results) => {
    if (err) throw err;
    console.log(`Deleted ${results.affectedRows} rows`);
  });
  // connection.end();
}

function fulfillment_ih_tif() {
  const filePath = path.join(__dirname, 'wsa', 'wsa_fulfillment_tif.csv').replace(/\\/g, '/');
  const query = `
  LOAD DATA LOCAL INFILE ?
  INTO TABLE kpi_endstate_monthly_tif
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  (blank, lokasi, tti_comply, tti_not_comply, tti_ps_ih, tti_real, tti_ach, ffg_comply, ffg_not_comply, ffg_jml_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_ggn_wsa, ttr_ffg_real, ttr_ffg_ach, ttr_ffg_wilsus, ttr_ffg_ggn)`;

  connection.query({
    sql: query,
    values: [filePath],
    infileStreamFactory: (path) => fs.createReadStream(path), // StreamFactory untuk membaca file CSV
  });
  console.log(`wsa_fulfillment_tif.csv Berhasil Di input ke Database`);

  // HAPUS DATA
  const searchStrings = ['TREG 1', 'TREG 2', 'TREG 3', 'TREG 4', 'BALI', 'JATIM BARAT', 'JATIM TIMUR', 'NUSA TENGGARA', 'SEMARANG JATENG UTARA', 'SOLO JATENG TIMUR', 'SURAMADU', 'YOGYA JATENG SELATAN'];

  // Buat bagian WHERE dengan NOT LIKE untuk setiap kata
  const whereClause = searchStrings.map(() => 'lokasi NOT LIKE ?').join(' AND ');
  const sql = `DELETE FROM kpi_endstate_monthly_tif WHERE ${whereClause}`;

  connection.query(sql, searchStrings, (err, results) => {
    if (err) throw err;
    console.log(`Deleted ${results.affectedRows} rows`);
  });
  // connection.end();
}

function fulfillment_ih_ccm() {
  const filePath = path.join(__dirname, 'wsa', 'wsa_fulfillment_ccm.csv').replace(/\\/g, '/');
  const query = `
  LOAD DATA LOCAL INFILE ?
  INTO TABLE kpi_endstate_monthly_ccm
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  (blank, lokasi, tti_comply, tti_not_comply, tti_ps_ih, tti_real, tti_ach, ffg_comply, ffg_not_comply, ffg_jml_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_ggn_wsa, ttr_ffg_real, ttr_ffg_ach, ttr_ffg_wilsus, ttr_ffg_ggn)  SET insert_at = CURRENT_TIMESTAMP;`;

  connection.query({
    sql: query,
    values: [filePath],
    infileStreamFactory: (path) => fs.createReadStream(path), // StreamFactory untuk membaca file CSV
  });
  console.log(`wsa_fulfillment.csv Berhasil Di input ke Database`);

  // HAPUS DATA
  const searchStrings = [
    'AREA 1',
    'AREA 2',
    'AREA 3',
    'AREA 4',
    'BALI NUSRA',
    'DENPASAR',
    'FLORES',
    'KUPANG',
    'MATARAM',
    'JATENG-DIY',
    'MAGELANG',
    'PEKALONGAN',
    'PURWOKERTO',
    'SEMARANG',
    'SURAKARTA',
    'YOGYAKARTA',
    'JATIM',
    'JEMBER',
    'LAMONGAN',
    'MADIUN',
    'MALANG',
    'SIDOARJO',
    'SURABAYA',
  ];

  // Buat bagian WHERE dengan NOT LIKE untuk setiap kata
  const whereClause = searchStrings.map(() => 'lokasi NOT LIKE ?').join(' AND ');
  const sql = `DELETE FROM kpi_endstate_monthly_ccm WHERE ${whereClause}`;

  connection.query(sql, searchStrings, (err, results) => {
    if (err) throw err;
    console.log(`Deleted ${results.affectedRows} rows`);
  });
  // connection.end();
}

function fulfillment_ih_reg_history() {
  const filePath = path.join(__dirname, 'wsa', 'wsa_fulfillment_reg.csv').replace(/\\/g, '/');
  const query = `
  LOAD DATA LOCAL INFILE ?
  INTO TABLE kpi_endstate_monthly_reg_history
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  (blank, lokasi, tti_comply, tti_not_comply, tti_ps_ih, tti_real, tti_ach, ffg_comply, ffg_not_comply, ffg_jml_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_ggn_wsa, ttr_ffg_real, ttr_ffg_ach, ttr_ffg_wilsus, ttr_ffg_ggn) SET created_at = CURRENT_TIMESTAMP;`;

  connection.query({
    sql: query,
    values: [filePath],
    infileStreamFactory: (path) => fs.createReadStream(path), // StreamFactory untuk membaca file CSV
  });
  console.log(`wsa_fulfillment.csv Berhasil Di input ke Database`);

  // HAPUS DATA
  const searchStrings = [
    'TREG 1',
    'TREG 2',
    'TREG 3',
    'TREG 4',
    'TREG 5',
    'TREG 6',
    'TREG 7',
    'KUDUS',
    'MAGELANG',
    'PEKALONGAN',
    'PURWOKERTO',
    'SEMARANG',
    'SOLO',
    'YOGYAKARTA',
    'DENPASAR',
    'JEMBER',
    'KEDIRI',
    'MADIUN',
    'MADURA',
    'MALANG',
    'NTB',
    'NTT',
    'PASURUAN',
    'SIDOARJO',
    'SINGARAJA',
    'SURABAYA SELATAN',
    'SURABAYA UTARA',
  ];

  // Buat bagian WHERE dengan NOT LIKE untuk setiap kata
  const whereClause = searchStrings.map(() => 'lokasi NOT LIKE ?').join(' AND ');
  const sql = `DELETE FROM kpi_endstate_monthly_reg_history WHERE ${whereClause}`;

  connection.query(sql, searchStrings, (err, results) => {
    if (err) throw err;
    console.log(`Deleted ${results.affectedRows} rows`);
  });
  // connection.end();
}

function fulfillment_ih_tif_history() {
  const filePath = path.join(__dirname, 'wsa', 'wsa_fulfillment_tif.csv').replace(/\\/g, '/');
  const query = `
  LOAD DATA LOCAL INFILE ?
  INTO TABLE kpi_endstate_monthly_tif_history
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  (blank, lokasi, tti_comply, tti_not_comply, tti_ps_ih, tti_real, tti_ach, ffg_comply, ffg_not_comply, ffg_jml_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_ggn_wsa, ttr_ffg_real, ttr_ffg_ach, ttr_ffg_wilsus, ttr_ffg_ggn) SET created_at = CURRENT_TIMESTAMP;`;

  connection.query({
    sql: query,
    values: [filePath],
    infileStreamFactory: (path) => fs.createReadStream(path), // StreamFactory untuk membaca file CSV
  });
  console.log(`wsa_fulfillment.csv Berhasil Di input ke Database`);

  // HAPUS DATA
  const searchStrings = ['TERRITORY 1', 'TERRITORY 2', 'TERRITORY 3', 'TERRITORY 4', 'BALI', 'MALANG', 'SIDOARJO', 'NUSA TENGGARA', 'SEMARANG', 'SOLO', 'SURAMADU', 'YOGYAKARTA'];

  // Buat bagian WHERE dengan NOT LIKE untuk setiap kata
  const whereClause = searchStrings.map(() => 'lokasi NOT LIKE ?').join(' AND ');
  const sql = `DELETE FROM kpi_endstate_monthly_tif_history WHERE ${whereClause}`;

  connection.query(sql, searchStrings, (err, results) => {
    if (err) throw err;
    console.log(`Deleted ${results.affectedRows} rows`);
  });
  // connection.end();
}

function insert_file_download() {
  const filePath = path.join(__dirname, 'wsa', 'wsa_fulfillment_tif.csv').replace(/\\/g, '/');
  const query = `
  LOAD DATA LOCAL INFILE ?
  INTO TABLE kpi_endstate_monthly_tif_history
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  (blank, lokasi, tti_comply, tti_not_comply, tti_ps_ih, tti_real, tti_ach, ffg_comply, ffg_not_comply, ffg_jml_ps, ffg_real, ffg_ach, ttr_ffg_comply, ttr_ffg_not_comply, ttr_ffg_ggn_wsa, ttr_ffg_real, ttr_ffg_ach, ttr_ffg_wilsus, ttr_ffg_ggn) SET created_at = CURRENT_TIMESTAMP;`;

  connection.query({
    sql: query,
    values: [filePath],
    infileStreamFactory: (path) => fs.createReadStream(path), // StreamFactory untuk membaca file CSV
  });

  connection.query(sql, searchStrings, (err, results) => {
    if (err) throw err;
    console.log(`Deleted ${results.affectedRows} rows`);
  });
  // connection.end();
}

fulfillment_hsi_reg();
fulfillment_hsi_tif();
// fulfillment_ih_reg();
// fulfillment_ih_tif();
// fulfillment_ih_ccm();

// fulfillment_ih_reg_history();
// fulfillment_ih_tif_history();
console.log('Program selesai.');
connection.end();
