/*
A script for converting html into pdf, using headless Chrome.
This is to replace wkhtmltopdf, since that's not actively maintained anymore.
Requires Chrome 59 or newer, and Puppeteer, which requires Node 6.4.0 or newer.
*/

const puppeteer = require('puppeteer');

const html_name = process.argv[2];
const pdf_name = process.argv[3];

(async () => {
  const browser = await puppeteer.launch({headless: true});
  const page = await browser.newPage();
  await page.goto(html_name, {waitUntil: 'networkidle'});
  await page.pdf({path: pdf_name, format: 'A4', margin: {top: '5cm'}, displayHeaderFooter: false}); //displayHeaderFooter doesn't seem to do anything, but fortunately it defaults to false.

  browser.close();
})();
