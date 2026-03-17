# ------------------------------
# DIAGNÓSTICO COMPLETO - descubra a nova estrutura
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

print("🌐 Iniciando diagnóstico completo...")
driver = setup_driver()

print("📱 Carregando página...")
driver.get("https://clube.uol.com.br/?order=new")
time.sleep(5)

print("\n📄 TÍTULO DA PÁGINA:", driver.title)
print("-" * 50)

# Pega todos os links
links = driver.find_elements(By.TAG_NAME, "a")
print(f"\n🔗 Total de links na página: {len(links)}")

# Pega todas as imagens
imgs = driver.find_elements(By.TAG_NAME, "img")
print(f"🖼️ Total de imagens: {len(imgs)}")

# Pega todos os headings
h1 = driver.find_elements(By.TAG_NAME, "h1")
h2 = driver.find_elements(By.TAG_NAME, "h2")
h3 = driver.find_elements(By.TAG_NAME, "h3")
print(f"📌 Headings: H1:{len(h1)} H2:{len(h2)} H3:{len(h3)}")

# Procura por elementos que parecem cards/ofertas
print("\n🔍 Buscando possíveis containers de oferta:")
containers = driver.find_elements(By.CSS_SELECTOR, 
    "div[class*='card'], div[class*='offer'], div[class*='beneficio'], div[class*='produto'], article, section")
print(f"   Possíveis containers: {len(containers)}")

# Mostra os primeiros 5 containers como exemplo
for i, container in enumerate(containers[:5]):
    print(f"\n--- Container {i+1} ---")
    print(f"  Classe: {container.get_attribute('class')}")
    print(f"  HTML resumido: {container.get_attribute('outerHTML')[:200]}...")

driver.quit()
