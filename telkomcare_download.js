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

    function getCaptchaDB() {
      return new Promise((resolve, reject) => {
        pool.query("SELECT pesan FROM get_otp_for_download WHERE pesan LIKE '%cpt%' ORDER BY id DESC LIMIT 1", (err, result) => {
          if (err) return reject(err);
          resolve(result[0]?.pesan || null);
        });
      });
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

    function uniqueName(base, ext = 'xlsx') {
      const ts = new Date().toISOString().replace(/[:.]/g, '-');
      return `${base}.${ext}`;
    }

    async function waitAndRename(dir, newName, timeout = 60000) {
      const start = Date.now();

      return new Promise((resolve, reject) => {
        const interval = setInterval(() => {
          const files = fs
            .readdirSync(dir)
            .filter((f) => !f.endsWith('.crdownload'))
            .map((f) => ({
              name: f,
              time: fs.statSync(path.join(dir, f)).mtime.getTime(),
            }))
            .sort((a, b) => b.time - a.time);

          if (files.length > 0) {
            const oldPath = path.join(dir, files[0].name);
            const newPath = path.join(dir, newName);

            fs.renameSync(oldPath, newPath);
            clearInterval(interval);
            console.log(`✅ File disimpan: ${newName}`);
            resolve();
          }

          if (Date.now() - start > timeout) {
            clearInterval(interval);
            reject(new Error('Download timeout'));
          }
        }, 1000);
      });
    }

    function delay(ms) {
      return new Promise((resolve) => setTimeout(resolve, ms));
    }
    async function download(url, filename) {
      await page.goto(url, {
        waitUntil: 'domcontentloaded',
        timeout: 60000,
      });
      const selector = 'body > div.page-container > div.page-content-wrapper > div.page-content > div:nth-child(4) > div > div > div > a';
      await page.waitForSelector(selector, { visible: true, timeout: 60000 });
      await delay(10000);
      await page.click(selector);
      // await waitAndRename(downloadPath, uniqueName(filename));
    }

    // ttr datin
    // await download(
    //   'https://telkomcare.telkom.co.id/assurance/lapebis25/detailrescomp25?read=all&param_teritory=TIF&tahun=&bulan=&sumber=DATIN24&tiket=TELKOMGAMAS&startdate=2026-02-01&enddate=2026-02-18&custpending=&regional=TERRITORY%203&kategori=&tcomp=',
    // );
    // await delay(20000);

    // sugar Datin
    await download('https://telkomcare.telkom.co.id/assurance/lapebis25/detailsugar25?read=all&param_teritory=TIF&enddate=2026-02-18&tahun=&bulan=&sumber=DATIN24&tiket=&regional=TERRITORY3&kategori=grand_total');
    await delay(20000);

    // // ttr reseller
    // await download(
    //   'https://telkomcare.telkom.co.id/assurance/lapebis25/detailrescomp25?read=all&param_teritory=TIF&tahun=&bulan=&sumber=RESELLER&tiket=TELKOMGAMAS&startdate=2026-02-01&enddate=2026-02-18&custpending=&regional=TERRITORY%203&kategori=&tcomp=',
    // );

    // // ttr indibiz
    // await download(
    //   'https://telkomcare.telkom.co.id/assurance/lapebis25/detailrescomp25?read=all&param_teritory=TIF&tahun=&bulan=&sumber=INDIBIZ&tiket=TELKOMGAMAS&startdate=2026-02-01&enddate=2026-02-18&custpending=&regional=TERRITORY%203&kategori=&tcomp=',
    // );

    // // sugar HSI
    // await download('https://telkomcare.telkom.co.id/assurance/lapebis25/detailsugar25?read=all&param_teritory=TIF&enddate=2026-02-18&tahun=&bulan=&sumber=HSI24&tiket=&regional=TERRITORY3&kategori=grand_total');
    // await delay(20000);

    // // ttr sip trunk
    // await download(
    //   'https://telkomcare.telkom.co.id/assurance/lapebis25/detailreport25?read=all&param_teritory=TIF&startdate=2026-02-01&enddate=2026-02-18&tahun=&bulan=&sumber=SIPTRUNK&tiket=TELKOMGAMAS&reportby=byreg&regional=TERRITORY3&kategori=',
    // );
    // await delay(20000);

    // // ttr sip dwdm
    // await download(
    //   'https://telkomcare.telkom.co.id/assurance/lapebis25/detailreport25?read=all&param_teritory=TIF&startdate=2026-02-01&enddate=2026-02-18&tahun=&bulan=&sumber=DWDM&tiket=TELKOMGAMAS&reportby=byreg&regional=TERRITORY3&kategori=',
    // );
    await delay(20000);
  } catch (err) {
    console.error('❌ Error:', err.message);
  } finally {
    await page.waitForTimeout(15000);
    await browser.close();
  }
})();
