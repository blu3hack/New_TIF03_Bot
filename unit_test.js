const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');
const { user_aribi, pass_aribi } = require('./login');
const { exec } = require('child_process');

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    protocolTimeout: 300000, // 5 menit untuk proses ekspansi yang lama
  });
  const page = await browser.newPage();

  // --- HELPER: GET OTP ---
  async function getOTP() {
    return new Promise((resolve, reject) => {
      exec('python otp_qosmo.py', (error, stdout, stderr) => {
        if (error) return reject(error);
        resolve(stdout.trim());
      });
    });
  }

  // --- FUNGSI UTAMA: SCRAPE DATA ---
  async function getDataQosmo(filtersArray, namefile) {
    try {
      console.log(`\n🚀 Memproses Group: ${namefile} [${filtersArray.join(', ')}]`);

      // Masuk ke halaman target (MSA)
      await page.goto('https://qosmo.telkom.co.id/msa', { waitUntil: 'networkidle0' });
      await page.waitForSelector('table');

      const qosmo_data = await page.evaluate(async (targetStrings) => {
        const delay = (ms) => new Promise((res) => setTimeout(res, ms));
        const table1 = document.querySelectorAll('table')[0];
        if (!table1) return 'Tabel tidak ditemukan';

        // 1. PROSES EKSPANSI (Berdasarkan Array Filter)
        let rowCount = table1.querySelectorAll('tr.ant-table-row').length;
        for (let i = 0; i < rowCount; i++) {
          const rows = table1.querySelectorAll('tr.ant-table-row');
          const currentRow = rows[i];
          if (!currentRow) continue;

          const content = currentRow.textContent || '';
          // Cek apakah baris mengandung salah satu kata kunci di array
          const isTarget = targetStrings.some((str) => content.includes(str));
          const isAlreadyExpanded = currentRow.classList.contains('ant-table-row-expanded');

          if (isTarget && !isAlreadyExpanded) {
            const expandBtn = currentRow.querySelector('div[role="button"]');
            if (expandBtn) {
              expandBtn.scrollIntoView({ behavior: 'auto', block: 'center' });
              await delay(500);
              expandBtn.click();
              await delay(2500); // Tunggu API/XHR narik data wilayah
              rowCount = table1.querySelectorAll('tr.ant-table-row').length;
            }
          }
        }

        await delay(2000);

        // 2. AMBIL DATA (Hanya Baris Detail + Header)
        const allRows = Array.from(table1.querySelectorAll('tr'));
        return allRows
          .filter((row) => {
            // Ambil Header (th) ATAU Baris Detail hasil ekspansi (ant-table-expanded-row)
            return row.querySelector('th') || row.classList.contains('ant-table-expanded-row');
          })
          .map((row) => {
            const columns = Array.from(row.querySelectorAll('td, th'));
            return columns
              .map((column) => {
                let cellValue = column.textContent.trim();
                return cellValue.replace(/,/g, '').replace(/\n/g, ' ').replace(/\s+/g, ' ');
              })
              .join(',');
          })
          .filter((rowStr) => rowStr.replace(/,/g, '').trim().length > 0)
          .join('\n');
      }, filtersArray);

      // Simpan ke Folder
      if (qosmo_data && qosmo_data !== 'Tabel tidak ditemukan') {
        const dir = 'loaded_file/qosmo';
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(`${dir}/${namefile}.csv`, qosmo_data);
        console.log(`✅ File ${namefile}.csv berhasil disimpan.`);
      }
    } catch (err) {
      console.error(`❌ Error pada ${namefile}:`, err.message);
      if (err.message.includes('destroyed')) {
        console.log('🔄 Re-navigating due to context loss...');
        await page.goto('https://qosmo.telkom.co.id/msa', { waitUntil: 'networkidle0' });
      }
    }
  }

  // --- ALUR LOGIN ---
  try {
    await page.goto('https://qosmo.telkom.co.id/login');
    await new Promise((res) => setTimeout(res, 5000));

    await page.type('#email', user_aribi);
    await page.type('#password', pass_aribi);
    await page.click('button[type="submit"]');

    await new Promise((res) => setTimeout(res, 3000));
    const otp = await getOTP();
    const inputOTP = 'input[placeholder="Masukkan Kode OTP"]';
    await page.waitForSelector(inputOTP);
    await page.type(inputOTP, otp);

    const [confirmBtn] = await page.$x("//button[contains(., 'Confirm')]");
    if (confirmBtn) await confirmBtn.click();

    await page.waitForSelector('main', { timeout: 30000 });

    // --- KONFIGURASI FILTER ---
    // Anda bisa menggabungkan beberapa filter dalam satu file CSV di sini
    const jobList = [
      {
        key: ['PACKETLOSS >5% RAN TO CORE', 'PACKETLOSS 1-5% RAN TO CORE'],
        file: 'all_packetloss_data',
      },
      {
        key: ['LATENCY RAN TO CORE', 'JITTER RAN TO CORE'],
        file: 'latency_jitter_ran',
      },
      {
        key: ['MTTRQ RAN TO CORE MAJOR', 'MTTRQ RAN TO CORE MINOR', 'MTTRQ RAN TO CORE CRITICAL'],
        file: 'mttr_complete',
      },
    ];

    // Eksekusi satu per satu
    for (const job of jobList) {
      await getDataQosmo(job.key, job.file);
      await new Promise((res) => setTimeout(res, 5000)); // Jeda antar file
    }

    console.log('\n🏁 SEMUA PROSES SELESAI!');
  } catch (error) {
    console.error('💥 Fatal Error:', error);
  } finally {
    // await browser.close();
  }
})();
