import json
import random
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


class UrlScraping:
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_experimental_option("detach", True)
        chrome_options.add_argument("--start-maximized")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def get_all_listing_urls(self):
        start_url = 'https://www.olx.in/en-in/kerala_g2001160/for-sale-houses-apartments_c1725?filter=listed_by_eq_builder'
        self.driver.get(start_url)
        time.sleep(random.randint(1, 3))

        all_urls = set()
        page_count = 1

        while True:
            print(f"⏩ Scraping page #{page_count}")
            time.sleep(random.randint(2, 4))

            items = self.driver.find_elements(By.CSS_SELECTOR, "li[data-aut-id^='itemBox']")
            for item in items:
                try:
                    link = item.find_element(By.CSS_SELECTOR, "a")
                    href = link.get_attribute("href")
                    if href:
                        all_urls.add(href)
                except Exception:
                    continue

            try:
                next_button = self.driver.find_element(By.CSS_SELECTOR, "a[data-aut-id='arrowRight']")
                if "disabled" in next_button.get_attribute("class"):
                    print("🚫 No more pages.")
                    break
                next_button.click()
                page_count += 1
            except Exception as e:
                print("❌ Next button not found or error occurred:", e)
                break

        with open("olx_listing_urls.json", "w") as f:
            json.dump(list(all_urls), f, indent=4)

        print(f"✅ Scraped and saved {len(all_urls)} listing URLs.")


# Run the scraper
if __name__ == "__main__":
    scraper = UrlScraping()
    scraper.get_all_listing_urls()
