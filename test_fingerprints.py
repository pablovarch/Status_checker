from playwright.sync_api import sync_playwright
import random
from fake_useragent import UserAgent

FINGERPRINTJS_URL = "https://openfpcdn.io/fingerprintjs/v4"
TEST_PAGE = "https://abrahamjuliot.github.io/creepjs/"

# Lista de diferentes User-Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
]
# Página de prueba de FingerprintJS

with sync_playwright() as p:
    # Lanzar el navegador con una identidad diferente en cada ejecución
    browser = p.chromium.launch(headless=False)
    # Seleccionar un User-Agent aleatorio
    ua = UserAgent()
    # user_agent = ua.chrome
    user_agent = random.choice(USER_AGENTS)
    # context = browser.new_context(
    #     user_agent=user_agent,
    #     viewport={"width": 1280, "height": 720},
    #     locale="en-US"
    # )
    proxy_dict = {
        'server': 'brd.superproxy.io:22225',
        'username': 'brd-customer-hl_f416ecd9-zone-mobile-country-us',
        'password': '52ggi1lw5ei1'
    }
    user_profile = 'profile_mark_holliday'
    context = p.chromium.launch_persistent_context(

        user_data_dir='C:/Users/pablo/AppData/Local/Google/Chrome/User Data/',
        headless=False,
        user_agent=user_agent,
        channel='chrome',
        # permissions=['notifications'],
        args=[
            # f"--disable-extensions-except={constants.path_to_extension}",
            # f"--load-extension={constants.path_to_extension}",
            f'--profile-directory={user_profile}'

            "--start-fullscreen"
        ],
        device_scale_factor=1,
        is_mobile=False,
        has_touch=False,
        locale="en-US",


        # proxy=proxy_dict,
        # locale=f'{iso_name}',

    )
    page = context.new_page()
    # Inyectar JavaScript para ocultar automatización
    page.add_init_script("""
           Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
           Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
           Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
       """)
    page.evaluate("navigator.webdriver = undefined")


    # Inyectar FingerprintJS desde la CDN y obtener el fingerprint
    fingerprint_script = """
        () => {
            return new Promise((resolve, reject) => {
                import('https://openfpcdn.io/fingerprintjs/v4')
                    .then(FingerprintJS => FingerprintJS.load())
                    .then(fp => fp.get())
                    .then(result => resolve(result.visitorId))
                    .catch(error => reject(error));
            });
        }
    """
    page.goto(TEST_PAGE)
    fingerprint = page.evaluate(fingerprint_script)
    print(f"Fingerprint obtenido: {fingerprint}")

    browser.close()

