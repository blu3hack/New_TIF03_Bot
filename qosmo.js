const puppeteer = require('puppeteer');
const fs = require('fs');
const { user_aribi, pass_aribi } = require('./login');
const { exec } = require('child_process');

(async () => {
  const browser = await puppeteer.launch({ headless: false }); // Buka browser (non-headless)
  const page = await browser.newPage();

  // ambil capcute
  await page.goto('https://qosmo.telkom.co.id/login');
  await page.waitForTimeout(10000);

  // Isi formulir login
  await page.type('#email', user_aribi);
  await page.type('#password', pass_aribi);
  page.$x("//a[contains(., 'Wallboard')]");

  // Klik tombol login
  await page.click('#root > div > div > form > div.mb-0 > button');

  // ================== INPUT OTP =====================
  async function getOTP() {
    return new Promise((resolve, reject) => {
      exec('python otp_qosmo.py', (error, stdout, stderr) => {
        if (error) {
          console.error(`Terjadi kesalahan: ${error.message}`);
          return reject(error);
        }
        if (stderr) {
          console.error(`Stderr: ${stderr}`);
          return reject(stderr);
        }

        const otp = stdout.trim();
        console.log(`OTP dari Python: ${otp}`); // Pastikan OTP valid di sini
        resolve(otp);
      });
    });
  }
  await page.waitForTimeout(3000);

  async function insertOTP() {
    try {
      const otp = await getOTP();
      if (!otp || otp.length < 4) throw new Error('OTP tidak valid');

      const inputSelector = 'input[placeholder="Masukkan Kode OTP"]';
      await page.waitForSelector(inputSelector, { visible: true });

      // Bersihkan input dulu sebelum mengetik
      await page.click(inputSelector, { clickCount: 3 });
      await page.type(inputSelector, otp, { delay: 100 });

      const [confirmButton] = await page.$x("//button[contains(., 'Confirm')]");

      if (confirmButton) {
        await confirmButton.click();
        console.log('Tombol Confirm berhasil diklik via XPath');
      } else {
        console.error('Tombol Confirm tidak ditemukan!');
      }

      console.log('OTP berhasil disubmit via Evaluate');
    } catch (error) {
      console.error('Gagal memasukkan OTP:', error.message);
    }
  }
  await insertOTP();

  // 1. Tunggu navigasi selesai atau tunggu URL berubah
  console.log('Menunggu pengalihan halaman...');
  try {
    // Lebih aman menunggu elemen dashboard muncul daripada waitForNavigation
    await page.waitForSelector('main', { timeout: 30000 });
  } catch (e) {
    console.log('Navigasi otomatis lambat, mencoba goto manual...');
  }

  // 2. Arahkan ke halaman target
  console.log('Membuka halaman MSA...');

  async function getDataQosmo(filter, namefile) {
    await page.goto('https://qosmo.telkom.co.id/msa', {
      waitUntil: 'networkidle0', // Tunggu sampai tidak ada aktivitas jaringan lagi
    });

    // 3. Tunggu Tabel muncul di halaman MSA
    // Gunakan selector yang lebih simpel, misal: 'table'
    // 3. Tunggu Tabel muncul
    const tableSelector = 'table';
    await page.waitForSelector(tableSelector);

    console.log('Proses ekspansi baris dimulai...');
    // Tambahkan 'filter' sebagai argumen di sini --------------------v
    const qosmo_data = await page.evaluate(async (targetString) => {
      const delay = (ms) => new Promise((res) => setTimeout(res, ms));

      // Sekarang targetString berisi nilai dari 'filter' yang dikirim dari Node.js
      const tables = document.querySelectorAll('table');
      const table1 = tables[0];
      if (!table1) return 'Tabel tidak ditemukan';

      const expandSpecificRows = async () => {
        let rowCount = table1.querySelectorAll('tr.ant-table-row').length;

        for (let i = 0; i < rowCount; i++) {
          const rows = table1.querySelectorAll('tr.ant-table-row');
          const currentRow = rows[i];
          if (!currentRow) continue;

          const content = currentRow.textContent || '';

          const isTarget = content.includes(targetString);
          const isAlreadyExpanded = currentRow.classList.contains('ant-table-row-expanded');
          const isDetailRow = currentRow.classList.contains('ant-table-expanded-row');

          if (isTarget && !isAlreadyExpanded && !isDetailRow) {
            const expandBtn = currentRow.querySelector('div[role="button"]');

            if (expandBtn) {
              console.log(`Mengekspansi baris target: ${targetString}`);
              expandBtn.scrollIntoView({ behavior: 'auto', block: 'center' });
              await delay(500);
              expandBtn.click();
              await delay(2000);
              rowCount = table1.querySelectorAll('tr.ant-table-row').length;
            }
          }
        }
      };

      await expandSpecificRows();
      await delay(10000);

      const allRows = Array.from(table1.querySelectorAll('tr'));
      return allRows
        .map((row) => {
          const columns = Array.from(row.querySelectorAll('td, th'));
          return columns
            .map((column) => {
              let cellValue = column.textContent.trim();
              return cellValue.replace(/,/g, '').replace(/\n/g, ' ').replace(/\s+/g, ' ');
            })
            .join(',');
        })
        .filter((row) => row.trim().length > 0 && row !== ',')
        .join('\n');
    }, filter); // <--- INI PENTING: Mengirim variabel 'filter' ke dalam browser

    if (qosmo_data && qosmo_data !== 'Tabel tidak ditemukan') {
      const dir = 'loaded_file/qosmo';
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

      fs.writeFileSync(`${dir}/${namefile}.csv`, qosmo_data);
      console.log(`${namefile}.csv Berhasil di simpan`);
    } else {
      console.error('❌ Gagal mengambil data tabel.');
    }
  }

  // Bagian pemanggilan fungsi dengan penanganan refresh/stabilitas
  const filters = [
    { key: 'PACKETLOSS', file: 'packetloss_kurang_lima' },
    { key: 'PACKETLOSS 1-5% RAN TO CORE', file: 'packetloss_satu_sampai_lima' },
    { key: 'LATENCY RAN TO CORE', file: 'latency_ran_to_core' },
    { key: 'JITTER RAN TO CORE', file: 'jittter_ran_to_core' },
    { key: 'LATENCY CORE TO INTERNET', file: 'latency_core_to_inet' },
    { key: 'JITTER CORE TO INTERNET', file: 'jitter_core_to_inet' },
    { key: 'MTTRQ RAN TO CORE MAJOR', file: 'mttr_ran_to_core_major' },
    { key: 'MTTRQ RAN TO CORE MINOR', file: 'mttr_ran_to_core_minor' },
    { key: 'MTTRQ RAN TO CORE CRITICAL', file: 'mttr_ran_to_core_critical' },
  ];

  for (const item of filters) {
    try {
      console.log(`Memulai proses untuk: ${item.key}`);
      await getDataQosmo(item.key, item.file);

      // JEDA PENTING: Beri waktu browser untuk tenang sebelum filter berikutnya
      await new Promise((res) => setTimeout(res, 10000));

      // OPTIONAL: Reload jika tabel menjadi terlalu berat karena terlalu banyak baris terbuka
      // await page.reload({ waitUntil: 'networkidle0' });
    } catch (err) {
      console.error(`Gagal memproses ${item.key}:`, err.message);
      // Jika context destroyed, kita coba reload halaman
      await page.goto('https://qosmo.telkom.co.id/msa', { waitUntil: 'networkidle0' });
    }
  }

  console.log('Semua proses selesai.');
  // await browser.close();
})();
