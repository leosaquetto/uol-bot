# ------------------------------
# DIAGNÓSTICO - Descobrir seletor atual
# ------------------------------

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def testar_selector(driver, selector):
    try:
        elementos = driver.find_elements(By.CSS_SELECTOR, selector)
        print(f"  {selector}: {len(elementos)} encontrados")
        return len(elementos)
    except:
        print(f"  {selector}: erro")
        return 0

driver = setup_driver()
driver.get("https://clube.uol.com.br/?order=new")
time.sleep(5)
driver.execute_script("window.scrollBy(0, 1500);")
time.sleep(3)

print("🔍 TESTANDO SELETORES:")
print("-" * 40)

testar_selector(driver, "div.beneficio")
testar_selector(driver, "article")
testar_selector(driver, ".card-oferta")
testar_selector(driver, "[class*='offer']")
testar_selector(driver, "[class*='card']")
testar_selector(driver, ".product-card")
testar_selector(driver, "div[class*='beneficio']")
testar_selector(driver, "a[href*='clube.uol']")
testar_selector(driver, "img[src*='cloudfront']")
testar_selector(driver, "div[class]")

driver.quit()
