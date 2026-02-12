"""
ITU OBS Ders Programı CSV Çekici (çoklu Chrome instance destekli)

Bu script, OBS Ders Programı sayfasından tüm ders bilgilerini çekip
data/program.csv dosyasına kaydeder. GitHub Actions ile otomatik çalışır.

Tek seferde tüm dersleri tek Chrome ile çekmek yerine, ders kodu listesini
parçalara bölüp aynı anda birden fazla Chrome instance'ı ile paralel olarak
çekebilir.
"""

from __future__ import annotations

import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

# === Ayarlar ===
site_url = "https://obs.itu.edu.tr/public/DersProgram"

# İstersen değiştirip bazı ders kodlarını hariç tutabilirsin
excluded_codes: list[str] = []

# Çekim için Chrome instance sayısı
WORKER_COUNT = 5

# Çıktı klasörü
DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)
CSV_PATH = DATA_DIR / "program.csv"


# === Ortak yardımcılar ===
def clean_text(text: str) -> str:
    return text.strip().replace("\n", " / ")


def create_driver() -> tuple[webdriver.Chrome, WebDriverWait]:
    """Yeni bir headless Chrome driver + WebDriverWait döner."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(service=Service(), options=chrome_options)
    wait = WebDriverWait(driver, 12)
    return driver, wait


def select_course_code(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    ders_kodu: str,
    retries: int = 3,
) -> bool:
    """Dropdown'dan ders kodu seçer (stale retry ile)."""
    for _ in range(retries):
        try:
            ders_select_element = wait.until(
                EC.presence_of_element_located((By.ID, "dersBransKoduId"))
            )
            ders_select = Select(ders_select_element)
            ders_select.select_by_visible_text(ders_kodu)
            return True
        except StaleElementReferenceException:
            time.sleep(0.3)
    return False


def collect_course_entries(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    ders_kodu: str,
    retries: int = 3,
) -> list[dict[str, str]]:
    """Seçili ders kodu için tabloyu okuyup satırları dict listesine çevirir."""
    for _ in range(retries):
        try:
            table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            rows = table.find_elements(By.TAG_NAME, "tr")
            entries: list[dict[str, str]] = []
            for row in rows[1:]:  # İlk satır başlık
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                except StaleElementReferenceException:
                    raise
                if len(cells) < 14:
                    continue

                ogretim_yontemi = clean_text(cells[3].text)
                if ogretim_yontemi == "Fiziksel (Yüz yüze)":
                    ogretim_yontemi = "Fiziksel"
                elif ogretim_yontemi == "Sanal (Çevrimiçi/Online)":
                    ogretim_yontemi = "Online"

                ders_entry = {
                    "Kod": clean_text(cells[1].text),
                    "Ders": clean_text(cells[2].text),
                    "Öğretim Yöntemi": ogretim_yontemi,
                    "Eğitmen": clean_text(cells[4].text),
                    "Gün": clean_text(cells[6].text),
                    "Saat": clean_text(cells[7].text),
                    "Bina": f"{clean_text(cells[5].text)} / {clean_text(cells[8].text)}",
                    "Kayıtlı": clean_text(cells[10].text),
                    "Kontenjan": clean_text(cells[9].text),
                    "Bölüm Sınırlaması": clean_text(cells[12].text),
                    "CRN": clean_text(cells[0].text),
                }

                ders_full_kod = ders_entry["Kod"]
                if ders_full_kod in excluded_codes:
                    continue  # Excluded ise atla

                entries.append(ders_entry)
            return entries
        except StaleElementReferenceException:
            time.sleep(0.4)
    print(f"Stale element hatası (tablo okunamadı): {ders_kodu}")
    return []


def get_all_course_codes() -> list[str]:
    """OBS sayfasından tüm dersBransKodu seçeneklerini çeker (tek driver ile)."""
    driver, wait = create_driver()
    try:
        driver.get(site_url)
        time.sleep(2)

        # Lisans seçimi sadece 1 kere yapılır
        lisans_select = wait.until(
            EC.presence_of_element_located((By.ID, "programSeviyeTipiId"))
        )
        driver.execute_script("arguments[0].value = 'LS';", lisans_select)
        driver.execute_script(
            "arguments[0].dispatchEvent(new Event('change'))", lisans_select
        )
        time.sleep(1)

        ders_select_element = wait.until(
            EC.presence_of_element_located((By.ID, "dersBransKoduId"))
        )
        ders_select = Select(ders_select_element)
        ders_kodlari = [
            option.text.strip()
            for option in ders_select.options
            if option.get_attribute("value")
        ]
        return ders_kodlari
    finally:
        driver.quit()


def scrape_chunk(chunk: list[str]) -> list[dict[str, str]]:
    """Verilen ders kodu listesi için tek Chrome instance'ı ile veri çeker."""
    if not chunk:
        return []

    driver, wait = create_driver()
    entries: list[dict[str, str]] = []
    try:
        driver.get(site_url)
        time.sleep(2)

        # Lisans seçimi
        lisans_select = wait.until(
            EC.presence_of_element_located((By.ID, "programSeviyeTipiId"))
        )
        driver.execute_script("arguments[0].value = 'LS';", lisans_select)
        driver.execute_script(
            "arguments[0].dispatchEvent(new Event('change'))", lisans_select
        )
        time.sleep(1)

        for ders_kodu in chunk:
            try:
                # Her seçim öncesi yeniden çekiyoruz (stale retry)
                if not select_course_code(driver, wait, ders_kodu):
                    print(
                        f"[worker] Stale element hatası (ders kodu seçilemedi): {ders_kodu}"
                    )
                    continue
                time.sleep(0.4)

                # Göster butonuna bas
                for _ in range(3):
                    try:
                        goster_buton = driver.find_element(
                            By.CSS_SELECTOR, "button.btn-primary"
                        )
                        goster_buton.click()
                        break
                    except StaleElementReferenceException:
                        time.sleep(0.3)
                else:
                    print(f"[worker] Stale element hatası (göster butonu): {ders_kodu}")
                    continue

                # Tabloyu bekle
                time.sleep(0.4)
                ders_entries = collect_course_entries(driver, wait, ders_kodu)
                if not ders_entries:
                    continue
                entries.extend(ders_entries)

                print(f"[worker] Çekildi: {ders_kodu}")
            except Exception as e:  # noqa: PERF203
                print(f"[worker] Hata: {ders_kodu} ({e})")
                continue
    finally:
        driver.quit()

    return entries


def main() -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Veri toplama başlıyor...")

    ders_kodlari = get_all_course_codes()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {len(ders_kodlari)} ders kodu bulundu. Çekilmeye başlanıyor.")

    all_entries: list[dict[str, str]] = []

    if not ders_kodlari:
        print("Hiç ders kodu bulunamadı.")
    elif WORKER_COUNT <= 1:
        # Tek worker (eski davranışa yakın)
        all_entries = scrape_chunk(ders_kodlari)
    else:
        # Ders kodlarını WORKER_COUNT sayıda parçaya böl
        worker_count = min(WORKER_COUNT, len(ders_kodlari))
        chunks: list[list[str]] = [[] for _ in range(worker_count)]
        for idx, kod in enumerate(ders_kodlari):
            chunks[idx % worker_count].append(kod)

        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Paralel çekim başlıyor: {worker_count} worker, "
            + ", ".join(str(len(c)) for c in chunks)
            + " ders kodu / worker"
        )

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_idx = {
                executor.submit(scrape_chunk, chunk): i
                for i, chunk in enumerate(chunks)
                if chunk
            }
            for future in as_completed(future_to_idx):
                i = future_to_idx[future]
                try:
                    result = future.result()
                    all_entries.extend(result)
                    print(
                        f"[worker-{i}] Tamamlandı, {len(result)} kayıt eklendi "
                        f"(toplam şu an: {len(all_entries)})"
                    )
                except Exception as e:  # noqa: PERF203
                    print(f"[worker-{i}] Hata: {e}")

    # === Verileri CSV dosyasına yazıyor ===
    if all_entries:
        # Kod'a göre alfabetik sırala (AKM, ALM, ... şeklinde)
        all_entries.sort(key=lambda e: (e.get("Kod", ""), e.get("CRN", "")))

        with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "Kod",
                    "Ders",
                    "Öğretim Yöntemi",
                    "Eğitmen",
                    "Gün",
                    "Saat",
                    "Bina",
                    "Kayıtlı",
                    "Kontenjan",
                    "Bölüm Sınırlaması",
                    "CRN",
                ],
            )
            writer.writeheader()
            writer.writerows(all_entries)

        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] İşlem tamamlandı! {len(all_entries)} ders kaydı "
            f"'{CSV_PATH}' dosyasına kaydedildi."
        )
        print(f"Dosya yolu: {CSV_PATH}")
    else:
        print("!!! Hiç veri bulunamadı!")


if __name__ == "__main__":
    main()

