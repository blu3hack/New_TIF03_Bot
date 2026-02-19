const puppeteer = require('puppeteer');
const pool = require('./connection');
const { user_care, pass_care } = require('./login');
const fs = require('fs');
const { startdate_long_format, enddate_long_format } = require('./currentDate');
const { exec } = require('child_process');

(async () => {
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();

  try {
    await page.goto('https://kpro.telkom.co.id/kpro/public/login');

    // Screenshoot cpt
    const element = await page.$('#m_login > div > div:nth-child(2) > div > div > div.m-login__signin > form > div:nth-child(5) > img'); // ganti #main dengan id target
    if (element) {
      await element.screenshot({
        path: 'captcha/cpt.png',
      });
    } else {
      console.log('❌ Elemen dengan id tersebut tidak ditemukan');
    }

    // mbil cpacha dari database
    function getData() {
      return new Promise((resolve, reject) => {
        const query = "SELECT pesan FROM get_otp_for_download WHERE pesan LIKE '%cpt%' ORDER BY id DESC LIMIT 1";
        pool.query(query, (err, results) => {
          if (err) return reject(err);
          resolve(results);
        });
      });
    }

    await page.waitForSelector('#uname');
    await page.type('#uname', user_care);
    await page.type('#passw', pass_care);

    await page.waitForTimeout(20000);
    await page.waitForSelector('#captcha-input');
    const result = await getData();
    const pesan = result[0].pesan; // contoh: "cpt azp"
    const parts = pesan.split(' ');
    const captcha = parts[1] || null; // ambil kata setelah "cpt"
    console.log(captcha);
    await page.type('#captcha-input', String(captcha)); // pastikan string

    const checkboxSelector = '#agree';
    if (await page.$(checkboxSelector)) {
      const isChecked = await page.$eval(checkboxSelector, (el) => el.checked);
      if (!isChecked) await page.click(checkboxSelector);
    }

    await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle0' }), page.click('#m_login_signin_submit')]);

    // Menunggu OTP dimasukkan ke dalam database
    console.log('Mengambil OTP dari database...');
    await page.waitForTimeout(5000);

    const getOtp = () => {
      return new Promise((resolve, reject) => {
        const query = 'SELECT otp FROM bot_message WHERE username_sender = "dashkapro_bot" ORDER BY created_at DESC LIMIT 1';
        pool.query(query, (error, results) => {
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
      await page.waitForSelector('#m_login > div > div:nth-child(2) > div > div > div.container > div > form > div.row.mt-1.mb-2 > div > div > center > input');
      await page.type('#m_login > div > div:nth-child(2) > div > div > div.container > div > form > div.row.mt-1.mb-2 > div > div > center > input', otp);
      await page.click('#m_login > div > div:nth-child(2) > div > div > div.container > div > form > div.row.mb-3 > div > div > button');
    } catch (error) {
      console.error('Gagal mengambil OTP:', error.message);
    } finally {
      pool.end(); // Tutup koneksi database
    }

    console.log('Berhasil login ke KPRO');

    // navigasi ke halaman yang di tuju
    const url = 'https://kpro.telkom.co.id/kpro/newteknisi/psre-report';
    await page.goto(url, {
      waitUntil: ['networkidle0', 'domcontentloaded'],
      timeout: 0,
    });

    // form selection function
    // pilih company

    async function ps_re(Territory, file_name) {
      console.log('Pilih Territory');
      await page.waitForSelector('#teritory');
      await page.click('#teritory');

      await page.evaluate((Territory) => {
        const optionElement = document.querySelector(`#teritory > option:nth-child(${Territory})`);
        if (optionElement) {
          optionElement.selected = true;
          const selectElement = optionElement.parentElement;
          selectElement.dispatchEvent(new Event('change'));
        } else {
          console.error('Elemen <option> tidak ditemukan.');
        }
      }, Territory);
      await page.waitForTimeout(3000);
      await tombol();

      //  ==============================

      // pilih tanggal periode
      const startdate = startdate_long_format;
      console.log(startdate);

      const inputSelectorsstartdate = '#start'; // Ganti dengan selector elemen input Anda
      await page.waitForSelector(inputSelectorsstartdate);

      await page.evaluate(
        (selector, value) => {
          const input = document.querySelector(selector);
          if (input) {
            input.value = value; // Ubah nilai
            input.dispatchEvent(new Event('input', { bubbles: true })); // Trigger event input
            input.dispatchEvent(new Event('change', { bubbles: true })); // Trigger event change
          } else {
            console.error(`Elemen dengan selector ${selector} tidak ditemukan.`);
          }
        },
        inputSelectorsstartdate,
        startdate,
      );
      await page.waitForTimeout(3000);

      // ====================

      const enddate = enddate_long_format;
      console.log(enddate);

      const inputSelectorenddate = '#end'; // Ganti dengan selector elemen input Anda
      await page.waitForSelector(inputSelectorenddate);

      await page.evaluate(
        (selector, value) => {
          const input = document.querySelector(selector);
          if (input) {
            input.value = value; // Ubah nilai
            input.dispatchEvent(new Event('input', { bubbles: true })); // Trigger event input
            input.dispatchEvent(new Event('change', { bubbles: true })); // Trigger event change
          } else {
            console.error(`Elemen dengan selector ${selector} tidak ditemukan.`);
          }
        },
        inputSelectorenddate,
        enddate,
      );

      await tombol();

      console.log('Ambil data dari semua table TIF');
      await page.waitForTimeout(30000);

      const ps_re_tif_all = await page.evaluate(() => {
        const tables = Array.from(document.querySelectorAll('#html_table'));
        return tables
          .map((table, index) => {
            const rows = Array.from(table.querySelectorAll('tr'));
            const csv = rows
              .map((row) => {
                const cols = Array.from(row.querySelectorAll('td, th'));
                return cols.map((col) => col.innerText.trim()).join(',');
              })
              .join('\n');
            return `Table ${index + 1}\n${csv}`;
          })
          .join('\n\n'); // Pisahkan antar tabel dengan 2 baris kosong
      });

      console.log(ps_re_tif_all);
      fs.writeFileSync(`loaded_file/ps_re/${file_name}.csv`, ps_re_tif_all);
      console.log(`${file_name} berhasil didownload\n`);
    }

    await ps_re(1, 'ps_re_reg');
    await ps_re(3, 'ps_re_tif');

    // tombol submit
    async function tombol() {
      const [button] = await page.$x("//button[contains(., 'Submit')]");
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
  } catch (err) {
    console.error('Ada kesalahan:', err.message);
  } finally {
    await browser.close();
  }
})();
