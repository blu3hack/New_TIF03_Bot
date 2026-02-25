const puppeteer = require('puppeteer');
const pool = require('./connection');
const { user_care, pass_care } = require('./login');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

(async () => {
  const downloadPath = 'D:\\SCRAPPERS\\Scrapper\\loaded_file\\download_fulfillment';

  const browser = await puppeteer.launch({
    headless: false,
    defaultViewport: null,
  });

  const page = await browser.newPage();

  try {
    const client = await page.target().createCDPSession();
    await client.send('Page.setDownloadBehavior', {
      behavior: 'allow',
      downloadPath,
    });

    await page.goto('https://telkomcare.telkom.co.id/', {
      waitUntil: 'domcontentloaded',
    });

    const captchaImg = await page.$('#captcha-element img');
    if (captchaImg) {
      await captchaImg.screenshot({ path: 'captcha/cpt.png' });
    }

    async function getCaptchaDB() {
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

    await page.waitForSelector('#uname');
    await page.type('#uname', user_care, { delay: 50 });
    await page.type('#passw', pass_care, { delay: 50 });

    await page.waitForTimeout(20000);

    const pesan = await getCaptchaDB();
    const captcha = pesan?.split(' ')[1];
    if (!captcha) throw new Error('Captcha tidak ditemukan');

    await page.type('#captcha-input', captcha);

    if (await page.$('#agree')) {
      const checked = await page.$eval('#agree', (el) => el.checked);
      if (!checked) await page.click('#agree');
    }

    await Promise.all([page.waitForNavigation({ waitUntil: 'domcontentloaded' }), page.click('#submit')]);

    function getOTP() {
      return new Promise((resolve, reject) => {
        exec('python otp_telkomcare.py', (err, stdout, stderr) => {
          if (err || stderr) return reject(err || stderr);
          resolve(stdout.trim());
        });
      });
    }

    const otp = await getOTP();
    const digits = otp.split('');

    for (let i = 0; i < digits.length; i++) {
      await page.type(`#otpForm input:nth-child(${i + 1})`, digits[i]);
      await page.waitForTimeout(80);
    }

    await page.waitForSelector('.page-container', { timeout: 60000 });
    console.log('✅ Login berhasil');
  } catch (err) {
    console.error('❌ Error:', err.message);
  } finally {
    await page.waitForTimeout(15000);
    // await browser.close();
  }
})();
