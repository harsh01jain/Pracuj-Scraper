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
    import os
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "0"

    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=headless,
        args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled"
        ],
    )
    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    )
    # Hide webdriver
    await context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
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


# replace get_job_links with this version
async def get_job_links(page, search_term, limit: int | None = None):
    job_urls = []
    term_enc = quote(search_term)
    page_num = 1

    # normalize limit: treat <=0 as "no results"
    if limit is not None and limit <= 0:
        logger.info("🔍 limit provided <= 0, returning no job links")
        return job_urls

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
                    # stop early if limit reached
                    if limit is not None and len(job_urls) >= limit:
                        logger.info(f"✅ Reached limit of {limit} job links")
                        return job_urls

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
# replace your /scrape route signature and use of get_job_links with this
from typing import Optional

@app.get("/scrape")
async def scrape(
    term: str = Query("Mechanik", description="Search term"),
    excel: bool = Query(False, description="Return Excel file"),
    headless: bool = Query(True, description="Run browser headless"),
    limit: Optional[int] = Query(None, description="Max number of job URLs to scrape (e.g. 10)")
):
    playwright, browser, page = await start_browser(headless=headless)
    try:
        # pass limit down to get_job_links
        job_urls = await get_job_links(page, term, limit=limit)

        full_results = []
        for idx, url in enumerate(job_urls, start=1):
            # optional: stop again in case job_urls was modified elsewhere
            if limit is not None and idx > limit:
                break
            job = await scrape_job(page, url)
            full_results.append(job)

        json_full = {"jobs": full_results}
        json_urls = {"urls": job_urls}

        if excel:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_term = "".join(c if c.isalnum() else "_" for c in term)[:50]
            filename = f"jobs_{safe_term}_{ts}.xlsx"
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
