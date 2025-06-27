# 🕷️ OLX Scraper with Selenium + Crawl4AI

This project automates the process of scraping listing URLs from [OLX](https://www.olx.in) using Selenium, then extracts structured data (in Markdown format) using Crawl4AI and saves it as a CSV.

---

## 📌 Features

- 🔍 **Selenium-based scraper** (`main.py`):  
  - Collects listing URLs dynamically (supports pagination for N number of listings).
  - Saves all scraped URLs into a JSON file (`olx_urls.json`).

- 🤖 **Crawl4AI integration** (`crawl_ai.py` or similar):
  - Takes collected URLs and extracts listing data (title, price, location, description, etc.).
  - Outputs the results into a **Markdown-formatted CSV** file.

---

## 🚀 How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/shuhaibvvm/Olx_Scrapping.git
cd Olx_Scrapping
