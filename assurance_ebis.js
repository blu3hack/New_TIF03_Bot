const puppeteer = require('puppeteer');
const connection = require('./connection');
const { user_aribi, pass_aribi } = require('./login');
const { exec } = require('child_process');
const fs = require('fs');

(async () => {
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();

  try {
    // Navigasi ke halaman login
    await page.goto('https://assurance.telkom.co.id/ebis/index.php/login/index/');

    // Screenshoot cpt
    const element = await page.$('#mtLogin > div:nth-child(4) > img'); // ganti #main dengan id target
    if (element) {
      await element.screenshot({
        path: 'captcha/cpt.png',
      });
    } else {
      console.log('? Elemen dengan id tersebut tidak ditemukan');
    }

    // // mbil cpacha dari database
    async function getData() {
      const query = `
        SELECT pesan 
        FROM get_otp_for_download 
        WHERE pesan LIKE '%cpt%' 
        ORDER BY id DESC 
        LIMIT 1
      `;
      const [rows] = await connection.query(query);
      return rows;
    }

    // Isi formulir login
    await page.type('[placeholder="Username"]', user_aribi);
    await page.type('[placeholder="Password"]', pass_aribi);

    // input captcha
    await page.waitForTimeout(20000);
    const result = await getData();
    const pesan = result[0].pesan; // contoh: "cpt azp"
    const parts = pesan.split(' ');
    const captcha = parts[1] || null; // ambil kata setelah "cpt"
    console.log(captcha);
    await page.type('[placeholder="Captcha"]', String(captcha));

    await page.waitForTimeout(5000);
    await page.click('#mtLogin > div:nth-child(5) > button');

    async function getOTP() {
      return new Promise((resolve, reject) => {
        exec('python otp_ebis.py', (error, stdout, stderr) => {
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
      const otp = await getOTP();
      console.log('OTP yang didapat:', otp);
      await page.type('[placeholder="Token Google"]', otp);
      await page.click('#mtGoogleAuth > div:nth-child(6) > button');
    }
    await insertOTP();

    // Proses Pengambilan Data setelah login
    async function goToLinkByXPath(xpath) {
      await page.waitForXPath(xpath);
      const links = await page.$x(xpath);
      if (links.length > 0) {
        const linkUrl = await page.evaluate((link) => link.href, links[0]);
        await page.goto(linkUrl, { waitUntil: 'networkidle2', timeout: 5000 }).catch((e) => void 0);
      }
    }

    async function unspec() {
      await goToLinkByXPath("//a[contains(., 'Report Performansi Draft Underspec')]");
    }

    async function list() {
      await goToLinkByXPath("//a[contains(., 'Datin')]");
    }

    async function datin_q() {
      await goToLinkByXPath("//a[contains(., 'Datin')]");
    }

    async function hsi_q() {
      await goToLinkByXPath("//a[contains(., ' HSI, POTS, & IPTV')]");
    }

    async function datin_sqm() {
      await goToLinkByXPath("//a[contains(., 'Report SQM')]");
    }

    async function non_numbering() {
      await goToLinkByXPath("//a[contains(., 'Non-Numbering Infra')]");
    }

    async function unspec_datin() {
      console.log('============== Underspec ===============');
      await page.waitForTimeout(5000);
      await unspec();

      async function datin_unspec(group, regional, keterangan, nama_file) {
        await page.waitForTimeout(5000);
        await page.evaluate(() => {
          [...document.querySelectorAll('a.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
        });

        // Tunggu modal filter muncul
        await page.waitForSelector('#grup_teritori', { visible: true, timeout: 10000 });

        // Pilih Group territory
        await page.select('#grup_teritori', await page.$eval(`#grup_teritori option:nth-child(${group})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Territory
        await page.waitForSelector('#regional', { visible: true });
        await page.select('#regional', await page.$eval(`#regional option:nth-child(${regional})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Product
        await page.waitForSelector('#_____produk', { visible: true });
        if (keterangan === 'hsi') {
          const values = await page.$$eval('#_____produk option', (options) => [options[2]?.value, options[3]?.value, options[5]?.value].filter(Boolean));
          // Pilih semuanya sekaligus
          await page.select('#_____produk', ...values);
          console.log('Option yang dipilih:', values);
        } else {
          await page.select('#_____produk', await page.$eval('#_____produk option:nth-child(1)', (el) => el.value));
        }
        await page.waitForTimeout(2000);

        // Pilih Bulan
        await page.waitForSelector('#periodeValue', { visible: true });
        await page.select('#periodeValue', await page.$eval('#periodeValue > option:nth-child(1)', (el) => el.value));
        await page.waitForTimeout(2000);

        // Klik Terapkan Filter dan tunggu reload
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle0' }),
          page.evaluate(() => {
            [...document.querySelectorAll('button.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
          }),
        ]);

        // Tunggu tabel hasil muncul
        await page.waitForSelector('table tbody', { visible: true, timeout: 20000 });

        // Ambil data tabel
        const unspec_datin = await page.evaluate(() => {
          const table = document.querySelector('table tbody');
          if (!table) return 'Tabel tidak ditemukan';
          return Array.from(table.querySelectorAll('tr'))
            .map((row) =>
              Array.from(row.querySelectorAll('td, th'))
                .map((col) => col.innerText.trim().replace(/,/g, '')) // Hapus koma di dalam isi sel
                .join(','),
            )
            .join('\n');
        });

        console.log(unspec_datin);
        fs.writeFileSync(`loaded_file/unspec_datin/unspec_${keterangan}_${nama_file}.csv`, unspec_datin);
        console.log(`unspec_${keterangan}_${nama_file} berhasil didownload \n`);
      }

      await datin_unspec(3, 1, 'datin', 'tif');
      await datin_unspec(3, 4, 'datin', 'district');
      await datin_unspec(1, 1, 'datin', 'reg');
      await datin_unspec(1, 5, 'datin', 'reg4');
      await datin_unspec(1, 6, 'datin', 'reg5');
      await datin_unspec(3, 1, 'hsi', 'tif');
      await datin_unspec(3, 4, 'hsi', 'district');
      await datin_unspec(1, 1, 'hsi', 'reg');
      await datin_unspec(1, 5, 'hsi', 'reg4');
      await datin_unspec(1, 6, 'hsi', 'reg5');
    }

    async function q_datin() {
      console.log('============== Q DATIN ===============');
      await page.waitForTimeout(5000);
      await datin_q();

      async function data_q(group, regional, nama_file) {
        await page.waitForTimeout(5000);
        await page.evaluate(() => {
          [...document.querySelectorAll('a.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
        });

        // Tunggu modal filter muncul
        await page.waitForSelector('#grup_teritori', { visible: true, timeout: 10000 });

        // Pilih Group territory
        await page.select('#grup_teritori', await page.$eval(`#grup_teritori option:nth-child(${group})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Territory
        await page.waitForSelector('#regional', { visible: true });
        await page.select('#regional', await page.$eval(`#regional option:nth-child(${regional})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Bulan
        await page.waitForSelector('#periodeValue', { visible: true });
        await page.select('#periodeValue', await page.$eval('#periodeValue > option:nth-child(1)', (el) => el.value));
        await page.waitForTimeout(2000);

        // Klik Terapkan Filter dan tunggu reload
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle0' }),
          page.evaluate(() => {
            [...document.querySelectorAll('button.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
          }),
        ]);

        async function get_data(tableSelector, filePrefix) {
          // Tunggu tabel hasil muncul
          await page.waitForSelector(`#tab_ALL > table:nth-child(${tableSelector})`, { visible: true, timeout: 20000 });
          // Ambil data tabel
          const q_datin = await page.evaluate((tableSelector) => {
            const table = document.querySelector(`#tab_ALL > table:nth-child(${tableSelector})`);
            if (!table) return 'Tabel tidak ditemukan';
            return Array.from(table.querySelectorAll('tr'))
              .map((row) =>
                Array.from(row.querySelectorAll('td, th'))
                  .map((col) => col.innerText.trim().replace(/,/g, '')) // Hapus koma di dalam isi sel
                  .join(','),
              )
              .join('\n');
          }, tableSelector);

          console.log(q_datin);
          fs.writeFileSync(`loaded_file/unspec_datin/${filePrefix}_${nama_file}.csv`, q_datin);
          console.log(`${filePrefix}_${nama_file} berhasil didownload \n`);
        }

        await get_data(7, 'q_datin');
        await get_data(14, 'list_datin');
      }

      await data_q(3, 1, 'tif');
      await data_q(3, 4, 'district');
      await data_q(1, 1, 'reg');
      await data_q(1, 5, 'reg4');
      await data_q(1, 6, 'reg5');
    }

    async function q_hsi() {
      console.log('============== Q HSI ===============');
      await page.waitForTimeout(5000);
      await hsi_q();

      async function data_q(group, regional, nama_file) {
        await page.waitForTimeout(5000);
        await page.evaluate(() => {
          [...document.querySelectorAll('a.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
        });

        // Tunggu modal filter muncul
        await page.waitForSelector('#grup_teritori', { visible: true, timeout: 10000 });

        // Pilih Group territory
        await page.select('#grup_teritori', await page.$eval(`#grup_teritori option:nth-child(${group})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Territory
        await page.waitForSelector('#regional', { visible: true });
        await page.select('#regional', await page.$eval(`#regional option:nth-child(${regional})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Bulan
        await page.waitForSelector('#periodeValue', { visible: true });
        await page.select('#periodeValue', await page.$eval('#periodeValue > option:nth-child(1)', (el) => el.value));
        await page.waitForTimeout(2000);

        // Klik Terapkan Filter dan tunggu reload
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle0' }),
          page.evaluate(() => {
            [...document.querySelectorAll('button.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
          }),
        ]);

        async function get_data(tableSelector, filePrefix) {
          // Tunggu tabel hasil muncul
          await page.waitForSelector(`#tab_TEKNIS > table:nth-child(${tableSelector})`, { visible: true, timeout: 20000 });
          // Ambil data tabel #tab_TEKNIS > table:nth-child(6)
          const q_hsi = await page.evaluate((tableSelector) => {
            const table = document.querySelector(`#tab_TEKNIS > table:nth-child(${tableSelector})`);
            if (!table) return 'Tabel tidak ditemukan';
            return Array.from(table.querySelectorAll('tr'))
              .map((row) =>
                Array.from(row.querySelectorAll('td, th'))
                  .map((col) => col.innerText.trim().replace(/,/g, '')) // Hapus koma di dalam isi sel
                  .join(','),
              )
              .join('\n');
          }, tableSelector);

          console.log(q_hsi);
          fs.writeFileSync(`loaded_file/unspec_datin/${filePrefix}_${nama_file}.csv`, q_hsi);
          console.log(`${filePrefix}_${nama_file} berhasil didownload \n`);
        }

        await get_data(6, 'q_hsi');
        await get_data(13, 'list_hsi');
      }

      await data_q(3, 1, 'tif');
      await data_q(3, 4, 'district');
      await data_q(1, 1, 'reg');
      await data_q(1, 5, 'reg4');
      await data_q(1, 6, 'reg5');
    }

    async function sqm_datin() {
      console.log('============== SQM ===============');
      await page.waitForTimeout(5000);
      await datin_sqm();

      async function data_sqm(group, regional, keterangan, nama_file) {
        await page.waitForTimeout(5000);
        await page.evaluate(() => {
          [...document.querySelectorAll('a.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
        });

        // Tunggu modal filter muncul
        await page.waitForSelector('#grup_teritori', { visible: true, timeout: 10000 });

        // Pilih Group territory
        await page.select('#grup_teritori', await page.$eval(`#grup_teritori option:nth-child(${group})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Territory
        await page.waitForSelector('#regional', { visible: true });
        await page.select('#regional', await page.$eval(`#regional option:nth-child(${regional})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Product
        await page.waitForSelector('#_____produk', { visible: true });
        if (keterangan === 'hsi') {
          const values = await page.$$eval('#_____produk option', (options) => [options[2]?.value, options[3]?.value, options[5]?.value].filter(Boolean));
          // Pilih semuanya sekaligus
          await page.select('#_____produk', ...values);
          console.log('Option yang dipilih:', values);
        } else {
          await page.select('#_____produk', await page.$eval('#_____produk option:nth-child(1)', (el) => el.value));
        }
        await page.waitForTimeout(2000);

        // Pilih Bulan
        await page.waitForSelector('#periodeValue', { visible: true });
        await page.select('#periodeValue', await page.$eval('#periodeValue > option:nth-child(1)', (el) => el.value));
        await page.waitForTimeout(2000);

        // Klik Terapkan Filter dan tunggu reload
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle0' }),
          page.evaluate(() => {
            [...document.querySelectorAll('button.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
          }),
        ]);

        // Tunggu tabel hasil muncul
        await page.waitForSelector('table tbody', { visible: true, timeout: 20000 });

        // Ambil data tabel
        const sqm_datin = await page.evaluate(() => {
          const table = document.querySelector('table tbody');
          if (!table) return 'Tabel tidak ditemukan';
          return Array.from(table.querySelectorAll('tr'))
            .map((row) =>
              Array.from(row.querySelectorAll('td, th'))
                .map((col) => col.innerText.trim().replace(/,/g, '')) // Hapus koma di dalam isi sel
                .join(','),
            )
            .join('\n');
        });

        console.log(sqm_datin);
        fs.writeFileSync(`loaded_file/unspec_datin/sqm_${keterangan}_${nama_file}.csv`, sqm_datin);
        console.log(`sqm_${keterangan}_${nama_file} berhasil didownload \n`);
      }

      await data_sqm(3, 1, 'datin', 'tif');
      await data_sqm(3, 4, 'datin', 'district');
      await data_sqm(1, 1, 'datin', 'reg');
      await data_sqm(1, 5, 'datin', 'reg4');
      await data_sqm(1, 6, 'datin', 'reg5');
      await data_sqm(3, 1, 'hsi', 'tif');
      await data_sqm(3, 4, 'hsi', 'district');
      await data_sqm(1, 1, 'hsi', 'reg');
      await data_sqm(1, 5, 'hsi', 'reg4');
      await data_sqm(1, 6, 'hsi', 'reg5');
    }

    async function ttr() {
      console.log('============== TTR NON NUMBERING ===============');
      await page.waitForTimeout(5000);
      await non_numbering();

      async function data_ttr(group, regional, keterangan, nama_file) {
        await page.waitForTimeout(5000);
        await page.evaluate(() => {
          [...document.querySelectorAll('a.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
        });

        // Tunggu modal filter muncul
        await page.waitForSelector('#grup_teritori', { visible: true, timeout: 10000 });

        // Pilih Group territory
        await page.select('#grup_teritori', await page.$eval(`#grup_teritori option:nth-child(${group})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Territory
        await page.waitForSelector('#regional', { visible: true });
        await page.select('#regional', await page.$eval(`#regional option:nth-child(${regional})`, (el) => el.value));
        await page.waitForTimeout(2000);

        // Pilih Bulan
        await page.waitForSelector('#periodeValue', { visible: true });
        await page.select('#periodeValue', await page.$eval('#periodeValue > option:nth-child(1)', (el) => el.value));
        await page.waitForTimeout(2000);

        // Klik Terapkan Filter dan tunggu reload
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'networkidle0' }),
          page.evaluate(() => {
            [...document.querySelectorAll('button.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
          }),
        ]);

        // Tunggu tabel hasil muncul
        await page.waitForSelector('table', { visible: true, timeout: 20000 });

        // Ambil data tabele
        const sqm_datin = await page.evaluate(() => {
          const table = document.querySelector('table');
          if (!table) return 'Tabel tidak ditemukan';
          return Array.from(table.querySelectorAll('tr'))
            .map((row) =>
              Array.from(row.querySelectorAll('td, th'))
                .map((col) => col.innerText.trim().replace(/,/g, '')) // Hapus koma di dalam isi sel
                .join(','),
            )
            .join('\n');
        });

        console.log(sqm_datin);
        fs.writeFileSync(`loaded_file/unspec_datin/ttr_${keterangan}_${nama_file}.csv`, sqm_datin);
        console.log(`ttr_${keterangan}_${nama_file} berhasil didownload \n`);
      }

      await data_ttr(3, 1, 'datin', 'tif');
      await data_ttr(3, 4, 'datin', 'district');
      await data_ttr(1, 1, 'datin', 'reg');
      await data_ttr(1, 5, 'datin', 'reg4');
      await data_ttr(1, 6, 'datin', 'reg5');
    }

    await unspec_datin();
    await q_hsi();
    await sqm_datin();
    await ttr();
    await q_datin();
  } catch (error) {
    console.error('Terjadi kesalahan:', error.message);
  } finally {
    await browser.close();
  }
})();
