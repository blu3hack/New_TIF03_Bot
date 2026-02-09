const puppeteer = require('puppeteer');
const fs = require('fs');
const csv = require('csv-parser');
const { secureHeapUsed } = require('crypto');
const { user_aribi, pass_aribi } = require('./login');
const { exec } = require('child_process');
const axios = require('axios');
const sharp = require('sharp');
const TelegramBot = require('node-telegram-bot-api');
const mysql = require('mysql2/promise');
// const connection = require('./db_connection');
const { periode_long_format } = require('./currentDate');

(async () => {
  const browser = await puppeteer.launch({ headless: false }); // Buka browser (non-headless)
  const page = await browser.newPage();

  // ambil capcute
  await page.goto('https://assurance.telkom.co.id/hmvc/index.php/login/index/');

  // Isi formulir login
  await page.type('[placeholder="Username"]', user_aribi);
  await page.type('[placeholder="Password"]', pass_aribi);
  page.$x("//a[contains(., 'Wallboard')]");

  // klik chec box
  const checkboxSelector = '#remember-me';
  const isChecked = await page.$eval(checkboxSelector, (checkbox) => checkbox.checked);

  // Jika belum dicentang, klik untuk mencentangnya
  if (!isChecked) {
    await page.click(checkboxSelector);
    console.log('Checkbox berhasil diklik!');
  } else {
    console.log('Checkbox sudah dicentang.');
  }
  // Klik tombol login
  await page.click('#mtLogin > div:nth-child(5) > button');

  // ================== INPUT OTP =====================
  async function getCaptchaFromDatabase() {
    return new Promise((resolve, reject) => {
      exec('python otp_assurance.py', (error, stdout, stderr) => {
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
    const code_otp = String(await getCaptchaFromDatabase()).padStart(6, '0');

    console.log('OTP:', code_otp.split('').join(' '));

    // fokus ke input pertama
    await page.waitForSelector('#gg_otp input.sscrt', { visible: true });
    await page.click('#gg_otp input.sscrt');

    for (const digit of code_otp) {
      await page.keyboard.type(digit, { delay: 120 });
    }

    await page.click('#gg_otp > input.btn.btn-primary.mb-3');
    await page.waitForNavigation({ waitUntil: 'networkidle0' });
  }

  await insertOTP();
  await page.waitForTimeout(20000);

  async function goToLinkByXPath(xpath) {
    await page.waitForXPath(xpath);
    const links = await page.$x(xpath);
    if (links.length > 0) {
      const linkUrl = await page.evaluate((link) => link.href, links[0]);
      await page.goto(linkUrl, { waitUntil: 'networkidle2', timeout: 5000 }).catch((e) => void 0);
    }
  }

  async function service() {
    await goToLinkByXPath("//a[contains(., 'Q 30 Hari')]");
  }

  async function sugar() {
    await goToLinkByXPath("//a[contains(., 'Asr Guarantee')]");
  }

  //   ================== proses pengambilan data table =========================
  async function Service_availability() {
    console.log('============== Service Availability Q ===============');
    await page.waitForTimeout(10000);
    await service();

    async function getdataservice(index, fileName) {
      await page.waitForTimeout(3000);
      await page.evaluate(() => {
        [...document.querySelectorAll('a.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
      });

      // Pilih Company -> TIF
      await page.waitForSelector('#company', { visible: true, timeout: 10000 });
      await page.select('#company', await page.$eval(`#company > option:nth-child(3)`, (el) => el.value));
      await page.waitForTimeout(2000);

      // Pilih Regional -> TIF 03
      await page.waitForSelector('#regional', { visible: true });
      await page.select('#regional', await page.$eval(`#regional > option:nth-child(4)`, (el) => el.value));
      await page.waitForTimeout(2000);

      // Pilih Witel -> flexible sesuai kebutuhan
      await page.waitForSelector('#witel', { visible: true });
      await page.select('#witel', await page.$eval(`#witel > option:nth-child(${index})`, (el) => el.value)); // Semarang
      await page.waitForTimeout(2000);

      // Pilih view_by
      await page.waitForSelector('#_view_by_', { visible: true });
      await page.select('#_view_by_', await page.$eval('#_view_by_ > option:nth-child(2)', (el) => el.value));
      await page.waitForTimeout(2000);

      // Klik Terapkan Filter dan tunggu reload
      await Promise.all([
        page.waitForNavigation({ waitUntil: 'networkidle0' }),
        page.evaluate(() => {
          [...document.querySelectorAll('button.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
        }),
      ]);

      async function getdatatables(list_gangguan, keterangan) {
        // Tunggu tabel hasil muncul
        await page.waitForSelector(`#tab_TEKNIS > table:nth-child(${list_gangguan})`, { visible: true, timeout: 20000 });
        const service = await page.evaluate((list_gangguan) => {
          const table = document.querySelector(`#tab_TEKNIS > table:nth-child(${list_gangguan})`);
          if (!table) return 'Tabel tidak ditemukan';
          return Array.from(table.querySelectorAll('tr'))
            .map((row) =>
              Array.from(row.querySelectorAll('td, th'))
                .map((col) => col.innerText.trim().replace(/,/g, '')) // Hapus koma di dalam isi sel
                .join(','),
            )
            .join('\n');
        }, list_gangguan);

        // console.log(service);
        fs.writeFileSync(`loaded_file/service_sugar/service_${keterangan}_${fileName}.csv`, service);
        console.log(`service_${keterangan}_${fileName} berhasil didownload \n`);
      }

      await getdatatables(6, 'list_gangguan');
      await getdatatables(10, 'list_bill');
    }

    await getdataservice(3, 'semarang');
    await getdataservice(5, 'malang');
    await getdataservice(6, 'sidoarjo');
    await getdataservice(7, 'surabaya');
  }

  async function Assurance_guarantee() {
    console.log('============== Service Availability Q ===============');
    await page.waitForTimeout(10000);
    await sugar();

    async function getdatasugar(index, fileName) {
      await page.waitForTimeout(3000);
      await page.evaluate(() => {
        [...document.querySelectorAll('a.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
      });

      // Pilih Company -> TIF
      await page.waitForSelector('#company', { visible: true, timeout: 10000 });
      await page.select('#company', await page.$eval(`#company > option:nth-child(3)`, (el) => el.value));
      await page.waitForTimeout(2000);

      // Pilih Regional -> TIF 03
      await page.waitForSelector('#regional', { visible: true });
      await page.select('#regional', await page.$eval(`#regional > option:nth-child(4)`, (el) => el.value));
      await page.waitForTimeout(2000);

      // Pilih Witel -> flexible sesuai kebutuhan
      await page.waitForSelector('#witel', { visible: true });
      await page.select('#witel', await page.$eval(`#witel > option:nth-child(${index})`, (el) => el.value)); // Semarang
      await page.waitForTimeout(2000);

      // Pilih view_by
      await page.waitForSelector('#_view_by_', { visible: true });
      await page.select('#_view_by_', await page.$eval('#_view_by_ > option:nth-child(2)', (el) => el.value));
      await page.waitForTimeout(2000);

      // Klik Terapkan Filter dan tunggu reload
      await Promise.all([
        page.waitForNavigation({ waitUntil: 'networkidle0' }),
        page.evaluate(() => {
          [...document.querySelectorAll('button.btn.wrn-gradasi')].find((el) => el.textContent.trim() === 'Filter')?.click();
        }),
      ]);

      async function getdatatables(list_gangguan, keterangan) {
        // Tunggu tabel hasil muncul
        await page.waitForSelector(`#tab_TEKNIS > table:nth-child(${list_gangguan})`, { visible: true, timeout: 20000 });
        const sugar = await page.evaluate((list_gangguan) => {
          const table = document.querySelector(`#tab_TEKNIS > table:nth-child(${list_gangguan})`);
          if (!table) return 'Tabel tidak ditemukan';
          return Array.from(table.querySelectorAll('tr'))
            .map((row) =>
              Array.from(row.querySelectorAll('td, th'))
                .map((col) => col.innerText.trim().replace(/,/g, '')) // Hapus koma di dalam isi sel
                .join(','),
            )
            .join('\n');
        }, list_gangguan);

        // console.log(sugar);
        fs.writeFileSync(`loaded_file/service_sugar/sugar_${keterangan}_${fileName}.csv`, sugar);
        console.log(`sugar_${keterangan}_${fileName} berhasil didownload \n`);
      }

      await getdatatables(4, 'list_gangguan');
      await getdatatables(6, 'list_bill');
    }

    await getdatasugar(3, 'semarang');
    await getdatasugar(5, 'malang');
    await getdatasugar(6, 'sidoarjo');
    await getdatasugar(7, 'surabaya');
  }

  // Proses DOwnload Data
  await Service_availability();
  await Assurance_guarantee();

  async function tombol() {
    const [button] = await page.$x("//button[contains(., 'Filter')]");
    if (button) {
      await Promise.all([
        button.click(),
        page
          .waitForNavigation({
            waitUntil: 'networkidle0',
          })
          .catch((e) => void 0),
      ]);
    }
  }
  // await browser.close();
})();
