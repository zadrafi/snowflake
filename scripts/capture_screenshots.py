#!/usr/bin/env python3
"""Capture screenshots of the AP Invoice Processing Streamlit app."""
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

BASE_URL = "http://localhost:8502"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "docs"


async def wait_for_app_ready(page, timeout=90000):
    """Wait for Streamlit app to finish loading."""
    await page.wait_for_load_state("networkidle", timeout=timeout)
    try:
        await page.wait_for_selector(
            '[data-testid="stMetric"], [data-testid="stMarkdown"], [data-testid="stDataFrame"]',
            timeout=timeout,
        )
    except Exception:
        pass
    await page.wait_for_timeout(5000)


async def capture_landing(browser, output_dir):
    """Architecture overview landing page."""
    page = await browser.new_page(viewport={"width": 1440, "height": 900})
    await page.goto(BASE_URL, wait_until="networkidle")
    await wait_for_app_ready(page)
    # Collapse sidebar for cleaner landing page shot
    try:
        collapse_btn = page.locator('[data-testid="stSidebarCollapsedControl"]')
        if await collapse_btn.count() == 0:
            # Sidebar is expanded, collapse it
            collapse_btn = page.locator('[data-testid="stSidebarCollapseButton"]')
            if await collapse_btn.count() > 0:
                await collapse_btn.click()
                await page.wait_for_timeout(500)
    except Exception:
        pass
    await page.screenshot(
        path=str(output_dir / "01_landing_overview.png"), full_page=True
    )
    await page.close()


async def capture_dashboard(browser, output_dir):
    """KPI Dashboard page."""
    page = await browser.new_page(viewport={"width": 1440, "height": 900})
    await page.goto(f"{BASE_URL}/Dashboard", wait_until="networkidle")
    await wait_for_app_ready(page)
    await page.screenshot(
        path=str(output_dir / "02_dashboard.png"), full_page=True
    )
    await page.close()


async def capture_ap_ledger(browser, output_dir):
    """AP Ledger with invoice detail."""
    page = await browser.new_page(viewport={"width": 1440, "height": 900})
    await page.goto(f"{BASE_URL}/AP_Ledger", wait_until="networkidle")
    await wait_for_app_ready(page)
    await page.screenshot(
        path=str(output_dir / "03_ap_ledger.png"), full_page=True
    )
    await page.close()


async def capture_analytics(browser, output_dir):
    """Analytics charts page."""
    page = await browser.new_page(viewport={"width": 1440, "height": 900})
    await page.goto(f"{BASE_URL}/Analytics", wait_until="networkidle")
    await wait_for_app_ready(page)
    await page.screenshot(
        path=str(output_dir / "04_analytics.png"), full_page=True
    )
    await page.close()


async def capture_process_new(browser, output_dir):
    """Process New invoices page."""
    page = await browser.new_page(viewport={"width": 1440, "height": 900})
    await page.goto(f"{BASE_URL}/Process_New", wait_until="networkidle")
    await wait_for_app_ready(page)
    await page.screenshot(
        path=str(output_dir / "05_process_new.png"), full_page=True
    )
    await page.close()


async def capture_ai_extract_lab(browser, output_dir):
    """AI Extract Lab prompt builder page."""
    page = await browser.new_page(viewport={"width": 1440, "height": 900})
    await page.goto(f"{BASE_URL}/AI_Extract_Lab", wait_until="networkidle")
    await wait_for_app_ready(page)
    try:
        await page.wait_for_selector(
            '[data-testid="stRadio"], [data-testid="stExpander"]',
            timeout=30000,
        )
    except Exception:
        pass
    await page.wait_for_timeout(3000)
    await page.screenshot(
        path=str(output_dir / "06_ai_extract_lab.png"), full_page=True
    )
    await page.close()


async def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        tasks = [
            capture_landing(browser, OUTPUT_DIR),
            capture_dashboard(browser, OUTPUT_DIR),
            capture_ap_ledger(browser, OUTPUT_DIR),
            capture_analytics(browser, OUTPUT_DIR),
            capture_process_new(browser, OUTPUT_DIR),
            capture_ai_extract_lab(browser, OUTPUT_DIR),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"  ERROR in task {i}: {result}")
        else:
            print(f"  OK: task {i}")

    for f in sorted(OUTPUT_DIR.glob("*.png")):
        print(f"  {f.name}")


if __name__ == "__main__":
    asyncio.run(main())
