const fs = require('fs');
const { exec } = require('child_process');
const path = require('path');
const { extractFull } = require('node-7z');
const connection = require('./db_connection'); // Pastikan db_connection sudah ada dateStrings: true
const mysql = require('mysql2');

(async () => {
  const tableForDelete = ['Detil_WSA_OPEN', 'Detil_WSA_CLOSE'];

  // Fungsi ambil tanggal terbaru dari insert_at

  function getMaxInsertAt(table) {
    return new Promise((resolve, reject) => {
      connection.query(`SELECT MAX(tgl) AS max_date FROM ${table}`, (err, results) => {
        if (err) return reject(err);
        if (!results[0].max_date) return reject(new Error(`Tidak ada data insert_at di ${table}`));
        resolve(results[0].max_date); // langsung ambil mentah dari DB
      });
    });
  }

  // Fungsi untuk menghapus data
  function deleteFromTable(table, currentDate) {
    return new Promise((resolve, reject) => {
      const query = `DELETE FROM nonatero_download.${table} WHERE tgl = ? `;
      const pattern = `${currentDate}`;
      console.log(`Running query on table ${table} with pattern: ${pattern}`);

      connection.query(query, [pattern], (err, results) => {
        if (err) {
          console.error(`Error deleting data from ${table}: ${err.message}`);
          reject(err);
        } else {
          console.log(`Deleted ${results.affectedRows} rows from ${table}`);
          resolve(results);
        }
      });
    });
  }

  // Fungsi untuk load file WSA Open
  async function LoadFileWSAopen() {
    const treg = ['reg4', 'reg5'];
    for (let i = 0; i < treg.length; i++) {
      const folder = 'extracted-files/';
      const files = fs.readdirSync(folder);
      const fileName = files.find((file) => file.includes('open') && file.includes(treg[i])) || 'No valid files found';
      const filePath = path.join(__dirname, folder, fileName).replace(/\\/g, '/');
      const query = `
        LOAD DATA LOCAL INFILE ?
        INTO TABLE nonatero_download.Detil_WSA_OPEN
        FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (NPER,DPER,TROUBLE_NO,TROUBLE_OPENTIME,FIRST_ASSIGN_BY,FIRST_CUST_ASSIGN,DATE_CLOSE,TROUBLE_CLOSETIME,STATUS,PLBLCL,UNIT,CHANNEL,CHANNEL_NAME,CHANNEL_GROUP,ALPRO,JENIS_TIKET1,JENIS_TIKET2,SUBSEGMENTASI_ID,SUBSEGMENTASI,ACTUAL_SOLUTION_CODE,ACTUAL_SOLUTION,KAT_PLG,IS_GAMAS,TROUBLENO_PARENT,INCIDENT_DOMAIN,FLAG_HVC,FLAG_FCR,ND_INET,ND_POTS,ND_GROUP,NCLI,TKASSETTYPE,REG,WITEL,DATEL,STO,DP,ODC,IS_KPI_TTR,LAST_TCLOSE_LOKER_ID,SPEED_INET,UPDATED_DATE,ONT_MANUFACTURE,ONT_SN,ONT_TYPE,IS_GAUL,GAUL_CAT_NAME,TROUBLENO_B4,IS_GAMAS_B4,FLAG_FCR_B4,JENIS_TIKET2_B4,ACTUAL_SOLUTION_B4,AMCREW_B4,LABORCODE_B4,JML_TICKET_B4,FIRST_CLOSETIME_B4,LAST_CLOSETIME_B4,WSA_EXCLUDE,KET_EXCLUDE,IS_WILSUS,IS_FFG,BRANCH_TSEL,REGION_TSEL,AREA_TSEL,SQA_TSEL,NOP_TSEL,SQM_TROUBLENO_B4,SQM_TROUBLE_OPENTIME_B4,SQM_TROUBLE_CLOSETIME_B4,DURASI_JAM_SQM_REG,C_EXTERNAL_TICKETID,AMCREW,LABORCODE,WSA_EXCLUDE_B4,SERVICE_AREA,IBOOSTER_ONT_RX,MANAGE_BY) 
        SET insert_at = CURRENT_TIMESTAMP, tgl = insertDate;
      `;
      connection.query({
        sql: query,
        values: [filePath],
        infileStreamFactory: (path) => fs.createReadStream(path),
      });
      console.log(`${fileName} berhasil diinput ke Database`);
    }
  }

  // Fungsi untuk load file WSA Close
  async function LoadFileWSAclose() {
    const treg = ['reg4', 'reg5'];
    for (let i = 0; i < treg.length; i++) {
      const folder = 'extracted-files/';
      const files = fs.readdirSync(folder);
      const fileName = files.find((file) => file.includes('close') && file.includes(treg[i])) || 'No valid files found';
      const filePath = path.join(__dirname, folder, fileName).replace(/\\/g, '/');
      const query = `
        LOAD DATA LOCAL INFILE ?
        INTO TABLE nonatero_download.Detil_WSA_CLOSE
        FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (DPER,TROUBLE_NO,TROUBLE_OPENTIME,DATE_CLOSE,TROUBLE_CLOSETIME,STATUS,FIRST_ASSIGN_BY,FIRST_CUST_ASSIGN,HOUR_ADJ,PLBLCL,UNIT,KAT_PLG,FLAG_HVC,CHANNEL,CHANNEL_NAME,CHANNEL_GROUP,JENIS_TIKET1,JENIS_TIKET2,SUBSEGMENTASI_ID,SUBSEGMENTASI,ACTUAL_SOLUTION_CODE,ACTUAL_SOLUTION,IS_GAMAS,TROUBLENO_PARENT,FLAG_FCR,ND_INET,ND_POTS,ND_GROUP,NCLI,TKASSETTYPE,REG,WITEL,DATEL,STO,DP,ODC,TK_WORKZONE,SPEED_INET,IS_KPI_TTR,LAST_TCLOSE_LOKER_ID,TTR_OPEN_TCLOSE,TTR_FIRST_ASSIGN_FREEZE,COMPLETED_BY,TROUBLE_CLOSED_GROUP_ID,TROUBLE_CLOSED_GROUP,COMPLY3,COMPLY6,COMPLY12,COMPLY24,COMPLY36,COMPLY48,COMPLY72,COMPLY3_MANJA,ONT_MANUFACTURE,ONT_SN,ONT_TYPE,ACTUAL_SOLUTION_GROUP,INCIDENT_DOMAIN,INCIDENT_DOMAIN_GROUP,WSA_EXCLUDE,KET_EXCLUDE,IS_WILSUS,IS_FFG,BRANCH_TSEL,REGION_TSEL,AREA_TSEL,SQA_TSEL,NOP_TSEL,C_EXTERNAL_TICKETID,AMCREW,LABORCODE,WSA_EXCLUDE_B4,SERVICE_AREA,IBOOSTER_ONT_RX,MANAGE_BY) 
        SET insert_at = CURRENT_TIMESTAMP, tgl = CURDATE();
      `;
      connection.query({
        sql: query,
        values: [filePath],
        infileStreamFactory: (path) => fs.createReadStream(path),
      });
      console.log(`${fileName} berhasil diinput ke Database`);
    }
  }

  try {
    // Ambil tanggal terbaru
    // for (const table of tableForDelete) {
    //   const maxDate = await getMaxInsertAt(table);
    //   console.log(`Max insert_at di ${table}: ${maxDate}`);
    //   const result_short = maxDate.replace(/-/g, '').substring(0, 6);
    //   console.log(result_short); // Output: "202509"
    //   await deleteFromTable(table, maxDate);
    // }
    // console.log('Semua penghapusan selesai');
    // Lanjutkan proses load file
    await LoadFileWSAopen();
    await LoadFileWSAclose();
  } catch (err) {
    console.error('Error:', err.message);
  } finally {
    connection.end();
  }
})();
