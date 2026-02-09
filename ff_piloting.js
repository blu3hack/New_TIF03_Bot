const puppeteer = require('puppeteer');
const connection = require('./connection');
// const connection = require('./db');
const { user_care, pass_care } = require('./login');
const { periode_short_format, startdate_short_format, enddate_short_format } = require('./currentDate');
const { get } = require('request-promise-native');

(async () => {
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();

  try {
    // Navigasi ke halaman login
    await page.goto('https://dashboard.telkom.co.id/infrastructure');

    // Screenshoot cpt
    const element = await page.$('#captcha-element > img'); // ganti #main dengan id target
    if (element) {
      await element.screenshot({
        path: 'captcha/cpt.png',
      });
    } else {
      console.log('? Elemen dengan id tersebut tidak ditemukan');
    }

    // // mbil cpacha dari database
    function getData() {
      return new Promise((resolve, reject) => {
        const query = "SELECT pesan FROM get_otp_for_download WHERE pesan LIKE '%cpt%' ORDER BY id DESC LIMIT 1";
        connection.query(query, (err, results) => {
          if (err) return reject(err);
          resolve(results);
        });
      });
    }

    // Isi formulir login
    await page.waitForSelector('#uname');
    await page.type('#uname', user_care);
    await page.type('#passw', pass_care);
    await page.waitForTimeout(10000);
    await page.waitForSelector('#captcha-input');
    const result = await getData();
    const pesan = result[0].pesan; // contoh: "cpt azp"
    const parts = pesan.split(' ');
    const captcha = parts[1] || null; // ambil kata setelah "cpt"
    console.log(captcha);
    await page.type('#captcha-input', String(captcha)); // pastikan string

    // Cek dan klik checkbox jika belum dicentang
    const checkboxSelector = '#agree';
    if (await page.$(checkboxSelector)) {
      const isChecked = await page.$eval(checkboxSelector, (checkbox) => checkbox.checked);
      if (!isChecked) {
        await page.click(checkboxSelector);
        console.log('Checkbox berhasil diklik!');
      } else {
        console.log('Checkbox sudah dicentang.');
      }
    }
    await page.waitForTimeout(3000);

    // Klik tombol login
    await page.click('#submit');

    // Menunggu OTP dimasukkan ke dalam database
    console.log('Mengambil OTP dari database...');
    await page.waitForTimeout(5000);

    const getOtp = () => {
      return new Promise((resolve, reject) => {
        const query = 'SELECT otp FROM bot_message WHERE username_sender = "DashboardVerificationBot" ORDER BY created_at DESC LIMIT 1';
        connection.query(query, (error, results) => {
          if (error) {
            return reject(error);
          }
          if (!results.length || !results[0].otp) {
            return reject(new Error('Tidak ada OTP di database.'));
          }
          resolve(results[0].otp);
        });
      });
    };

    try {
      const otp = await getOtp();
      console.log('OTP yang didapat:', otp);

      await page.waitForSelector('input[name="verifikasi"]');
      await page.type('input[name="verifikasi"]', otp);
      await page.click('body > div.content > form > div.form-actions > button');
    } catch (error) {
      console.error('Gagal mengambil OTP:', error.message);
    } finally {
      connection.end(); // Tutup koneksi database
    }

    // Fungsi untuk mengambil data dari halaman
    console.log('Logged in successfully!');
    const url = 'https://dashboard.telkom.co.id/fulfillment';
    await page.goto(url, {
      waitUntil: ['networkidle0', 'domcontentloaded'],
      timeout: 0,
    });

    async function wsa_fulfillment_ih(page) {
      const fs = require('fs');
      const path = require('path');
      const source = 'C:/Users/L/Downloads/';
      const destination = 'D:/SCRAPPERS/Scrapper/loaded_file/download_fulfillment';
      // Mulai proses lainnya setelah login
      console.log('sudah masuk >> wsa fulfillment Indihome');
      await page.waitForTimeout(3000);

      // Tunggu submenu muncul dan klik pada "KPI Wecare 2024" menggunakan XPath

      async function fulfillment_data(fileName) {
        // const selectSelector = '#periode';
        // const optionValue = periode_short_format;
        // await page.waitForSelector(selectSelector);
        // await page.evaluate(
        //   (selectSelector, optionValue) => {
        //     const selectElement = document.querySelector(selectSelector);
        //     if (selectElement) {
        //       selectElement.value = optionValue;
        //       const event = new Event('change', { bubbles: true });
        //       selectElement.dispatchEvent(event);
        //     }
        //   },
        //   selectSelector,
        //   optionValue,
        // );
        // await page.waitForTimeout(10000);
        // // Tunggu elemen tombol siap
        // await page.waitForSelector('#form > div > div > div.form-group.col-md-1 > button');

        // // Klik tombol
        // await page.click('#form > div > div > div.form-group.col-md-1 > button');

        async function ambil_data_wsa_ff() {
          const selector_reg = ['#table_lokasi'];

          for (i = 0; i < selector_reg.length; i++) {
            await page.waitForSelector(selector_reg[i]);
            await page.click(selector_reg[i]);
          }

          // ambil data dari table
          await page.waitForSelector('#table_lokasi', { timeout: 0 });
          const wsa_ful = await page.evaluate(() => {
            const table = document.querySelector('#table_lokasi > tbody');
            const rows = Array.from(table.querySelectorAll('tr'));
            return rows
              .map((row) => {
                const columns = Array.from(row.querySelectorAll('td, th'));
                return columns
                  .map((column) => {
                    let cellValue = column.innerText.trim();
                    // Mengganti koma dengan string kosong
                    if (cellValue.includes(',')) {
                      cellValue = cellValue.replace(/,/g, '');
                    }
                    return cellValue;
                  })
                  .join(',');
              })
              .join('\n');
          });
          fs.writeFileSync(`loaded_file/wsa/${fileName}.csv`, wsa_ful);
          console.log(`${fileName} berhasil didownload \n`);
        }
        await ambil_data_wsa_ff();
      }

      console.log('Lakukan Selection manual pada KPI TIF Endstate Monthly');
      await page.waitForTimeout(120000);
      await fulfillment_data('wsa_fulfillment_tif');
      console.log('Proses Pemindahan FIle Download ke Folder Project');
      await page.waitForTimeout(20000);
      console.log('Proses Pengambilan WSA Fulfillment Ih Selesai');
    }

    const fs = require('fs');
    await wsa_fulfillment_ih(page);
  } catch (error) {
    console.error('Terjadi kesalahan:', error.message);
  } finally {
    await browser.close();
  }
})();
