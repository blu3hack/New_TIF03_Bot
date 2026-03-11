const puppeteer = require('puppeteer');
const connection = require('./connection');
const { user_care, pass_care } = require('./login');
const fs = require('fs');
const { startdate_long_format, enddate_long_format } = require('./currentDate');
const { exec } = require('child_process');

(async () => {
  const browser = await puppeteer.launch({ headless: false });
  const page = await browser.newPage();

  try {
    await page.goto('https://telkomcare.telkom.co.id/');

    // Screenshoot captcha
    const element = await page.$('#captcha-element > img'); // ganti #main dengan id target
    if (element) {
      await element.screenshot({
        path: 'captcha/cpt.png',
      });
    } else {
      console.log('❌ Elemen dengan id tersebut tidak ditemukan');
    }

    // mbil cpacha dari database
    async function getData() {
      const query = `SELECT pesan FROM get_otp_for_download WHERE pesan LIKE '%cpt%' ORDER BY id DESC LIMIT 1`;
      const [rows] = await connection.query(query);
      return rows;
    }

    await page.waitForSelector('#uname');
    await page.type('#uname', user_care);
    await page.type('#passw', pass_care);

    // ========

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

    await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle0' }), page.click('#submit')]);

    async function getCaptchaFromDatabase() {
      return new Promise((resolve, reject) => {
        exec('python otp_telkomcare.py', (error, stdout, stderr) => {
          if (error || stderr) return reject(error || stderr);
          resolve(stdout.trim());
        });
      });
    }

    async function insertOTP() {
      const otp = await getCaptchaFromDatabase();
      const codes = otp.split('').map((c) => c.toString());
      for (let i = 0; i < 6; i++) {
        const selector = `#otpForm input:nth-child(${i + 1})`;
        await page.type(selector, codes[i]);
        await page.waitForTimeout(50);
      }
    }

    await insertOTP();
    console.log('Berhasil login ke TelkomCare');

    async function datin(url, jenis) {
      try {
        await page.goto(url, {
          waitUntil: 'domcontentloaded',
          timeout: 60000,
        });
      } catch (err) {
        console.log('Gagal load:', err.message);
      }

      async function dataDatin(parameter, fileName) {
        const regional = '#param_teritory';
        const option_regional = `#param_teritory > option:nth-child(${parameter})`;

        await page.waitForSelector(regional);
        await page.evaluate(
          (r, o) => {
            const select = document.querySelector(r);
            const opt = document.querySelector(o);
            if (select && opt) {
              select.value = opt.value;
              select.dispatchEvent(new Event('change', { bubbles: true }));
            }
          },
          regional,
          option_regional,
        );

        if (jenis.includes('sugar')) {
          const endSelector = '#enddate';
          await page.waitForSelector(endSelector);
          await page.evaluate(
            (sel, val) => {
              const el = document.querySelector(sel);
              if (el) {
                el.value = val;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
              }
            },
            endSelector,
            enddate_long_format,
          );
        } else {
          const startSelector = '#startdate';
          const endSelector = '#enddate';
          await page.waitForSelector(startSelector);
          await page.waitForSelector(endSelector);
          await page.evaluate(
            (sel, val) => {
              const el = document.querySelector(sel);
              if (el) {
                el.value = val;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
              }
            },
            startSelector,
            startdate_long_format,
          );
          await page.evaluate(
            (sel, val) => {
              const el = document.querySelector(sel);
              if (el) {
                el.value = val;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
              }
            },
            endSelector,
            enddate_long_format,
          );
        }

        if (['dwdm', 'siptrunk', 'ttr_datin'].includes(jenis)) {
          const tiket = '#tiket';
          const option_tiket = '#tiket > option:nth-child(2)';
          await page.waitForSelector(tiket);
          await page.evaluate(
            (t, o) => {
              const sel = document.querySelector(t);
              const opt = document.querySelector(o);
              if (sel && opt) {
                sel.value = opt.value;
                sel.dispatchEvent(new Event('change', { bubbles: true }));
              }
            },
            tiket,
            option_tiket,
          );
        }

        if (['dwdm', 'siptrunk'].includes(jenis)) {
          const witel = '#reportby';
          const option_witel = '#reportby > option:nth-child(2)';
          await page.waitForSelector(witel);
          await page.evaluate(
            (w, o) => {
              const sel = document.querySelector(w);
              const opt = document.querySelector(o);
              if (sel && opt) {
                sel.value = opt.value;
                sel.dispatchEvent(new Event('change', { bubbles: true }));
              }
            },
            witel,
            option_witel,
          );
        }

        let button = jenis === 'indibiz' || jenis === 'reseller' ? '#content button' : jenis === 'dwdm' || jenis === 'siptrunk' ? '#formx div:nth-child(8) > button' : '#formx div:nth-child(6) > button';

        await page.waitForSelector(button);
        await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle0' }).catch(() => {}), page.click(button)]);

        await page.waitForTimeout(jenis === 'sugar_hsi' ? 60000 : 20000);
        try {
          await page.waitForSelector('#profit');
          const data = await page.evaluate(() => {
            const table = document.querySelector('#profit');
            return Array.from(table?.querySelectorAll('tr') || [])
              .map((row) =>
                Array.from(row.querySelectorAll('td,th'))
                  .map((col) => col.innerText.trim().replace(/,/g, ''))
                  .join(','),
              )
              .join('\n');
          });
          fs.writeFileSync(`loaded_file/asr_datin/${fileName}.csv`, data);
          console.log(`${fileName} berhasil diunduh`);
        } catch (e) {
          console.error(`Gagal ambil data ${fileName}: ${e.message}`);
        }
      }

      // await dataDatin(1, `${jenis}_reg`);
      await page.waitForTimeout(2000);
      await dataDatin(3, `${jenis}_tif`);
    }

    const urls = [
      ['https://telkomcare.telkom.co.id/assurance/lapebis25/wecaresugar25?sumber=DATIN24', 'sugar_datin'],
      ['https://telkomcare.telkom.co.id/assurance/lapebis25/wecaresugar25?sumber=HSI24', 'sugar_hsi'],
      ['https://telkomcare.telkom.co.id/assurance/lapebis25/resumecompliance25', 'ttr_datin'],
      ['https://telkomcare.telkom.co.id/assurance/lapebis25/resttrindibiz25', 'indibiz'],
      ['https://telkomcare.telkom.co.id/assurance/lapebis25/resttrreseller25', 'reseller'],
      ['https://telkomcare.telkom.co.id/assurance/lapebis25/mttr25?sumber=SIPTRUNK', 'siptrunk'],
      ['https://telkomcare.telkom.co.id/assurance/lapebis25/mttr25?sumber=DWDM', 'dwdm'],
    ];

    for (const [url, jenis] of urls) {
      await datin(url, jenis);
      await page.waitForTimeout(5000);
    }
  } catch (err) {
    console.error('Ada kesalahan:', err.message);
  } finally {
    await browser.close();
  }
})();
