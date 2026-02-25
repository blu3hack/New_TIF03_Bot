const fs = require('fs');
const path = require('path');
const pool = require('./connection'); // Pastikan menggunakan mysql2/promise
const { insertDate } = require('./currentDate');

(async () => {
  const tableForDelete = ['Detil_WSA_OPEN', 'Detil_WSA_CLOSE'];

  // 1. Fungsi Ambil Tanggal Maksimal
  async function getMaxInsertAt(table) {
    try {
      const [results] = await pool.query(`SELECT MAX(tgl) AS max_date FROM nonatero_download.${table}`);
      if (!results[0].max_date) {
        console.warn(`⚠️ Tidak ada data tgl di ${table}`);
        return null;
      }
      return results[0].max_date;
    } catch (err) {
      console.error(`❌ Gagal ambil max date dari ${table}:`, err.message);
      return null;
    }
  }

  // 2. Fungsi Hapus Data Berdasarkan Tanggal
  async function deleteFromTable(table, targetDate) {
    try {
      const query = `DELETE FROM nonatero_download.${table} WHERE tgl = ?`;
      const [result] = await pool.execute(query, [targetDate]);
      console.log(`🗑️ Deleted ${result.affectedRows} rows from ${table} for date: ${targetDate}`);
    } catch (err) {
      console.error(`❌ Error deleting from ${table}:`, err.message);
    }
  }

  // 3. Fungsi untuk Load File (General)
  async function processLoadData(type) {
    const tregs = ['reg4', 'reg5'];
    const folder = './extracted-files/';
    const table = type === 'open' ? 'Detil_WSA_OPEN' : 'Detil_WSA_CLOSE';

    // Kolom mapping sesuai kode Anda
    const columnsOpen = `(NPER,DPER,TROUBLE_NO,TROUBLE_OPENTIME,FIRST_ASSIGN_BY,FIRST_CUST_ASSIGN,DATE_CLOSE,TROUBLE_CLOSETIME,STATUS,PLBLCL,UNIT,CHANNEL,CHANNEL_NAME,CHANNEL_GROUP,ALPRO,JENIS_TIKET1,JENIS_TIKET2,SUBSEGMENTASI_ID,SUBSEGMENTASI,ACTUAL_SOLUTION_CODE,ACTUAL_SOLUTION,KAT_PLG,IS_GAMAS,TROUBLENO_PARENT,INCIDENT_DOMAIN,FLAG_HVC,FLAG_FCR,ND_INET,ND_POTS,ND_GROUP,NCLI,TKASSETTYPE,REG,WITEL,DATEL,STO,DP,ODC,IS_KPI_TTR,LAST_TCLOSE_LOKER_ID,SPEED_INET,UPDATED_DATE,ONT_MANUFACTURE,ONT_SN,ONT_TYPE,IS_GAUL,GAUL_CAT_NAME,TROUBLENO_B4,IS_GAMAS_B4,FLAG_FCR_B4,JENIS_TIKET2_B4,ACTUAL_SOLUTION_B4,AMCREW_B4,LABORCODE_B4,JML_TICKET_B4,FIRST_CLOSETIME_B4,LAST_CLOSETIME_B4,WSA_EXCLUDE,KET_EXCLUDE,IS_WILSUS,IS_FFG,BRANCH_TSEL,REGION_TSEL,AREA_TSEL,SQA_TSEL,NOP_TSEL,SQM_TROUBLENO_B4,SQM_TROUBLE_OPENTIME_B4,SQM_TROUBLE_CLOSETIME_B4,DURASI_JAM_SQM_REG,C_EXTERNAL_TICKETID,AMCREW,LABORCODE,WSA_EXCLUDE_B4,SERVICE_AREA,IBOOSTER_ONT_RX,MANAGE_BY)`;

    const columnsClose = `(DPER,TROUBLE_NO,TROUBLE_OPENTIME,DATE_CLOSE,TROUBLE_CLOSETIME,STATUS,FIRST_ASSIGN_BY,FIRST_CUST_ASSIGN,HOUR_ADJ,PLBLCL,UNIT,KAT_PLG,FLAG_HVC,CHANNEL,CHANNEL_NAME,CHANNEL_GROUP,JENIS_TIKET1,JENIS_TIKET2,SUBSEGMENTASI_ID,SUBSEGMENTASI,ACTUAL_SOLUTION_CODE,ACTUAL_SOLUTION,IS_GAMAS,TROUBLENO_PARENT,FLAG_FCR,ND_INET,ND_POTS,ND_GROUP,NCLI,TKASSETTYPE,REG,WITEL,DATEL,STO,DP,ODC,TK_WORKZONE,SPEED_INET,IS_KPI_TTR,LAST_TCLOSE_LOKER_ID,TTR_OPEN_TCLOSE,TTR_FIRST_ASSIGN_FREEZE,COMPLETED_BY,TROUBLE_CLOSED_GROUP_ID,TROUBLE_CLOSED_GROUP,COMPLY3,COMPLY6,COMPLY12,COMPLY24,COMPLY36,COMPLY48,COMPLY72,COMPLY3_MANJA,ONT_MANUFACTURE,ONT_SN,ONT_TYPE,ACTUAL_SOLUTION_GROUP,INCIDENT_DOMAIN,INCIDENT_DOMAIN_GROUP,WSA_EXCLUDE,KET_EXCLUDE,IS_WILSUS,IS_FFG,BRANCH_TSEL,REGION_TSEL,AREA_TSEL,SQA_TSEL,NOP_TSEL,C_EXTERNAL_TICKETID,AMCREW,LABORCODE,WSA_EXCLUDE_B4,SERVICE_AREA,IBOOSTER_ONT_RX,MANAGE_BY)`;

    const selectedColumns = type === 'open' ? columnsOpen : columnsClose;

    for (const treg of tregs) {
      const files = fs.readdirSync(folder);
      const fileName = files.find((file) => file.toLowerCase().includes(type) && file.toLowerCase().includes(treg));

      if (!fileName) {
        console.warn(`⚠️ File untuk ${type} ${treg} tidak ditemukan.`);
        continue;
      }

      const filePath = path.join(__dirname, folder, fileName).replace(/\\/g, '/');

      const query = `
        LOAD DATA LOCAL INFILE ?
        INTO TABLE nonatero_download.${table}
        FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        ${selectedColumns}
        SET insert_at = CURRENT_TIMESTAMP, tgl = ?;
      `;

      try {
        await pool.query({
          sql: query,
          values: [filePath, insertDate], // Menggunakan insertDate dari module luar
          infileStreamFactory: (p) => fs.createReadStream(p),
        });
        console.log(`✅ ${fileName} berhasil diinput ke ${table}`);
      } catch (err) {
        console.error(`❌ Gagal load ${fileName}:`, err.message);
      }
    }
  }

  // --- MAIN EXECUTION ---
  try {
    console.log('🔄 Memulai proses ETL WSA...');

    // 1. Pembersihan Data Lama (Opsional - Aktifkan jika diperlukan)
    for (const table of tableForDelete) {
      const maxDate = await getMaxInsertAt(table);
      console.log(maxDate);
      // if (maxDate) {
      //   await deleteFromTable(table, maxDate);
      // }
    }

    // 2. Load Data
    // await processLoadData('open');
    // await processLoadData('close');

    console.log('🏁 Seluruh proses WSA selesai!');
  } catch (err) {
    console.error('💥 Kesalahan sistem:', err.message);
  } finally {
    await pool.end();
    console.log('🔒 Koneksi database ditutup.');
  }
})();
