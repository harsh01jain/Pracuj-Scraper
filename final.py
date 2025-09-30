#!/usr/bin/env python3
"""
Pracuj.pl 24h Job Scraper API (FastAPI + Playwright)
- Returns JSON (full details + URLs)
- Optionally returns Excel file if excel=true
"""

import asyncio
import logging
import os
from datetime import datetime
from urllib.parse import quote, urljoin

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.pracuj.pl"
RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

app = FastAPI(title="Pracuj.pl 24h Job Scraper API")


# ---------------- Playwright functions ----------------
async def start_browser(headless=True):
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=headless,
        args=["--disable-blink-features=AutomationControlled"]
    )
    context = await browser.new_context(viewport={"width": 1920, "height": 1080})
    page = await context.new_page()
    return playwright, browser, page


async def accept_cookies(page):
    try:
        await page.wait_for_selector("#onetrust-accept-btn-handler", timeout=5000)
        await page.click("#onetrust-accept-btn-handler")
        logger.info("✅ Accepted cookies")
    except PlaywrightTimeout:
        logger.info("ℹ️ No cookie popup found")


async def goto_with_cf(page, url, timeout=60000):
    try:
        await page.goto(url, wait_until="networkidle", timeout=timeout)
        await page.wait_for_selector("body", timeout=timeout)
        logger.info(f"🌐 Loaded {url}")
        return True
    except PlaywrightTimeout:
        logger.error(f"❌ Timeout loading {url}")
        return False


async def get_job_links(page, search_term):
    job_urls = []
    term_enc = quote(search_term)
    page_num = 1

    while True:
        url = f"{BASE_URL}/praca/{term_enc};kw/ostatnich%2024h;p,{page_num}"
        logger.info(f"🔍 Loading {url}")
        ok = await goto_with_cf(page, url)
        if not ok:
            break

        await accept_cookies(page)

        try:
            await page.wait_for_selector("a[data-test='link-offer']", timeout=15000)
            links = await page.query_selector_all("a[data-test='link-offer']")
        except PlaywrightTimeout:
            logger.info(f"⚠️ No more jobs found on page {page_num}")
            break

        if not links:
            break

        new_links = 0
        for link in links:
            href = await link.get_attribute("href")
            if href:
                full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                if full_url not in job_urls:
                    job_urls.append(full_url)
                    new_links += 1

        if new_links == 0:
            break

        page_num += 1

    logger.info(f"✅ Found {len(job_urls)} job links in last 24 hours")
    return job_urls


async def scrape_job(page, url):
    job = {"URL": url, "Scraped At": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    try:
        ok = await goto_with_cf(page, url)
        if not ok:
            job["Error"] = "Timeout"
            return job

        async def safe_text(selector):
            try:
                el = await page.query_selector(selector)
                return (await el.text_content()).strip() if el else "N/A"
            except:
                return "N/A"

        async def extract_bullets(section_selector):
            items = await page.query_selector_all(f"{section_selector} li, {section_selector} p")
            texts = [((await i.text_content()) or "").strip() for i in items]
            return "\n".join([t for t in texts if t]) if texts else "N/A"

        job["Job Title"] = await safe_text("[data-test='text-positionName']")
        job["Employer"] = await safe_text("[data-test='text-employerName']")
        job["Location"] = await safe_text("[data-test='text-region']")
        job["Salary"] = await safe_text("[data-test='text-pay']")
        job["Posted Date"] = await safe_text("[data-test='text-publicationDate']")
        job["Description"] = await extract_bullets("[data-test='section-offerDescription']")
        job["Responsibilities"] = await extract_bullets("[data-test='section-responsibilities']")
        job["Requirements"] = await extract_bullets("[data-test='section-requirements']")
        job["Benefits"] = await extract_bullets("[data-test='section-benefits']")
        job["About Company"] = await extract_bullets("[data-test='section-about-us']")
        job["Contract Type"] = await safe_text("[data-test='sections-benefit-contracts']")
        job["Employment Type"] = await safe_text("[data-test='sections-benefit-employment-type-name']")
        job["Work Schedule"] = await safe_text("[data-test='sections-benefit-work-schedule']")
        job["Work Mode (Office)"] = await safe_text("[data-test='sections-benefit-work-modes-full-office']")
        job["Work Mode (Hybrid)"] = await safe_text("[data-test='sections-benefit-work-modes-hybrid']")
        job["Remote Recruitment"] = await safe_text("[data-test='sections-benefit-remote-recruitment']")
        job["Immediate Employment"] = await safe_text("[data-test='sections-benefit-immediate-employment']")
        job["Languages"] = await safe_text("[data-test='required-languages']")
        job["Eligibility"] = await safe_text("[data-test='eligibilities']")
        job["Address"] = await safe_text("[data-test='text-address']")
        job["Phone"] = await safe_text("[data-test='text-phoneNumber']")
        job["Recruitment Stages"] = await extract_bullets("[data-test='section-recruitment-stages']")
        job["Salary by Contract Type"] = await extract_bullets("[data-test='section-salaryPerContractType']")

    except Exception as e:
        logger.error(f"❌ Error scraping {url}: {e}")
        job["Error"] = str(e)

    return job


def save_to_excel(jobs, filename):
    filepath = os.path.join(RESULTS_DIR, filename)
    all_keys = set()
    for job in jobs:
        all_keys.update(job.keys())
    df = pd.DataFrame(jobs, columns=sorted(all_keys))
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Jobs', index=False)
    return filepath


# ---------------- API ROUTE ----------------
@app.get("/scrape")
async def scrape(
    term: str = Query("Mechanik", description="Search term"),
    excel: bool = Query(False, description="Return Excel file"),
    headless: bool = Query(True, description="Run browser headless")
):
    playwright, browser, page = await start_browser(headless=headless)
    try:
        job_urls = await get_job_links(page, term)
        full_results = []
        for url in job_urls:
            job = await scrape_job(page, url)
            full_results.append(job)

        json_full = {"jobs": full_results}
        json_urls = {"urls": job_urls}

        if excel:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"jobs_{term}_{ts}.xlsx"
            excel_path = save_to_excel(full_results, filename)
            return FileResponse(
                path=excel_path,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=filename
            )

        return JSONResponse(content={
            "full_jobs": json_full,
            "urls_only": json_urls
        })

    finally:
        await browser.close()
        await playwright.stop()
