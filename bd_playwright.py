from playwright.sync_api import Playwright, sync_playwright


with sync_playwright() as pw:
    AUTH = 'brd-customer-hl_f416ecd9-zone-bd_scraping_browser_1:tqa4jbuibqcp'
    SBR_WS_CDP = f'wss://{AUTH}@brd.superproxy.io:9222'

    print('Connecting to Scraping Browser...')
    browser = pw.chromium.connect_over_cdp(SBR_WS_CDP)

    print('Connected! Navigating...')
    page = browser.new_page()

    # Navegar al sitio web
    page.goto('https://xxbase.org')

    # Imprimir el título de la página
    print(page.title())

    page.screenshot(path="xxbase.org")


    # Cierra el navegador y el contexto
    page.close()
    browser.close()
