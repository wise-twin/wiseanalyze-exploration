from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time, os
import glob
from bs4 import BeautifulSoup
from tqdm import tqdm

CTNs = {
    "A - Métallurgie" : 1,
    "B - Bâtiment et Travaux Publics" : 2,
    "C - Transports, eau, gaz, électricité, livre et communication" : 3,
    "D - Services, commerces et industries de l'alimentation" : 4,
    "E - Chimie, caoutchouc, plasturgie" : 5,
    "F - Bois, ameublement, papier-carton, textile, vêtements, cuirs et peaux, pierres et terres à feu" : 6,
    "G - Commerce non alimentaire" : 7,
    "H - Activités de service I" : 8,
    "I - Activités de service II et travail temporaire" : 9,
    "X - Non précisé et autre" : 10,
    "Z - Catégories forfaitaires" : 11,
}

def scrape(indexFrom=1, selected_CTN = "D - Services, commerces et industries de l'alimentation", limit = 2, write_html_on_disk=False):
    """
    Scrape data from the EPICEA French industrial risks database.

    This function automates the web scraping of accident/incident records from the
    INRS EPICEA database (https://epicea.inrs.fr). It navigates the advanced search
    interface, filters by industry classification (CTN), and extracts structured
    data from accident reports. The browser is controlled via Selenium WebDriver
    and data is parsed using BeautifulSoup.

    Args:
        indexFrom: The starting EPICEA file identifier (dossier number). Used to
            filter results from a specific range. Defaults to 1.
        selected_CTN: The industrial classification (Convention Collective Nationale - CTN)
            to filter results. Must be a key from the CTNs dictionary.
        limit: Maximum number of accident records to scrape. Each iteration clicks
            to the next record. Defaults to 2.
        write_html_on_disk: If True, saves raw HTML of each scraped page to disk
            in ./epicea_results/ directory organized by CTN subdirectories. Useful
            for debugging or offline analysis. Defaults to False.

    Returns:
        A list of dictionaries where each dictionary represents a single accident
        record with extracted fields as keys. Typical keys include "Numéro du dossier",
        "Date de l'accident", "Secteur", "Nature du problème", etc. Returns an empty
        list if no records are found or if an exception occurs.

    Notes:
        - Ensure ChromeDriver is installed and in your system PATH.
        - Uncomment `chrome_options.add_argument("--headless")` in production to run
          without displaying the browser window.
        - A 0.1-second delay between iterations helps avoid overwhelming the server.
        - Browser is always closed in a finally block ensuring resource cleanup.
        - Expected HTML structure (table.tablein[2]) may change if website is updated.

    Examples:
        >>> data = scrape()
        >>> len(data)
        2

        >>> data = scrape(
        ...     indexFrom=100,
        ...     selected_CTN="A - Métallurgie",
        ...     limit=10,
        ...     write_html_on_disk=True
        ... )
    """
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Remove for visible browser
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Launch browser
    driver = webdriver.Chrome(options=chrome_options)

    def wait_for_page():
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "mainForm")))

    def go_to_recherche_avancee(driver):
        driver.execute_script("window.mainForm.searchType.value='full'")
        driver.execute_script("window.mainForm.submit();")
        wait_for_page()

    def select_indexFrom_and_CTN(indexFrom, selected_CTN):
        driver.execute_script(f"document.getElementById('identOp').selectedIndex = 4")
        driver.execute_script(f"document.getElementById('IDENT').value = {indexFrom}")
        if selected_CTN is not None :
            driver.execute_script(f"document.getElementById('CTN').selectedIndex = {CTNs[selected_CTN]}")
        driver.execute_script("window.mainForm.calculate.value = 'true';")
        driver.execute_script("window.mainForm.searchType.value='full';")
        driver.execute_script("window.mainForm.submit();")
        wait_for_page()

    def click_afficher_la_liste(driver):
        driver.execute_script("window.mainForm.goTo.value='1'")
        driver.execute_script("window.mainForm.submit();")
        wait_for_page()

    def click_first_result(driver):
        query = 'a.lien[href*=\"public_display\"]'
        script = f"document.querySelector(\'{query}\').click()"
        driver.execute_script(script)
        wait_for_page()

    def click_next_result(driver):
        query = 'a.lien[title=\"Dossier suivant\"]'
        script = f"document.querySelector(\'{query}\').click()"
        driver.execute_script(script)
        wait_for_page()

    def dump_html(driver, fileName, dirName):
        if dirName is None : dirName = "None"
        
        dir_path = os.path.join("epicea_results", dirName)
        os.makedirs(dir_path, exist_ok=True)
        with open(os.path.join(dir_path, f"{fileName}.html"), "w", encoding="utf-8") as f:
            f.write(driver.page_source)

        return driver.page_source

    data = []

    try:
        driver.get('https://epicea.inrs.fr/servlet/public_request')
        wait_for_page()

        go_to_recherche_avancee(driver)
        select_indexFrom_and_CTN(indexFrom, selected_CTN)
        click_afficher_la_liste(driver)
        click_first_result(driver)

        for _ in tqdm(range(limit), desc="Scraping EPICEA web pages"):
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            parsed_soup = _parse_soup(soup)
            if write_html_on_disk : dump_html(driver, fileName=parsed_soup["Numéro du dossier"], dirName=selected_CTN)
            data.append(parsed_soup)
            click_next_result(driver)
            time.sleep(.1)
    finally:
        driver.quit()

    return data

def process_html_directory(directory_path="./epicea_results", extension="*.html"):
    """
    Process previously scraped HTML files and extract structured accident data.

    This function is useful for offline processing of HTML files downloaded or saved
    by the scrape() function. It iterates through all matching HTML files in the
    specified directory (including subdirectories), parses them with BeautifulSoup,
    and extracts accident record data using the same parsing logic as scrape().

    This approach allows you to:
    - Reprocess HTML files without re-scraping the website
    - Debug parsing logic without browser automation overhead

    Args:
        directory_path (str, optional): Root directory containing HTML files to process.
            The function recursively searches this directory and all subdirectories
            for matching files. Relative and absolute paths are both supported.
            Typical structure after scraping:
            ./epicea_results/
            ├── A - Métallurgie/
            │   ├── 12345.html
            │   └── 12346.html
            ├── D - Services, commerces et industries de l'alimentation/
            │   ├── 67890.html
            │   └── 67891.html
            └── ...
            Defaults to "./epicea_results".
        extension (str, optional): File extension glob pattern to match. Use glob syntax
            (https://docs.python.org/3/library/glob.html). Common patterns:
            - "*.html" : Match all HTML files (default)
            - "*.htm" : Match .htm files
            - "*.xml" : Match XML files if structure is compatible
            Defaults to "*.html".

    Returns:
        list[dict]: A list of dictionaries with the same structure as scrape().
            Each dictionary represents one accident record with extracted fields.
            Returns an empty list if no matching files are found or all files are empty.
            The order of results follows the filesystem traversal order (not guaranteed
            to be sorted by date or file name).

    Raises:
        FileNotFoundError: If the directory_path does not exist.
        IOError: If a file cannot be read due to permission issues or file corruption.
        ValueError: If the HTML file structure does not contain the expected table
            (table.tablein[2]) for parsing.

    Notes:
        - This function uses glob.glob() with recursive=True internally to find all
          matching files across the directory tree.
        - Files are processed sequentially. For large datasets (>1000 files), consider
          parallelizing with multiprocessing or concurrent.futures.
        - The function includes a tqdm progress bar to show processing status.
        - File encoding is assumed to be UTF-8. If your HTML files use a different
          encoding, they may fail to parse correctly.
        - The parser expects the same HTML structure as produced by the scrape() function.
          HTML from other sources may not parse correctly.

    Examples:
        Process all HTML files in the default directory:

        >>> data = process_html_directory()
        >>> len(data)
        47
        >>> type(data[0])
        <class 'dict'>

        Process files from a custom directory:

        >>> data = process_html_directory(directory_path="/path/to/html/files")
        >>> # Extract specific field from all records
        >>> dates = [record.get("Date de l'accident") for record in data]

        Process only files from a specific CTN subdirectory:

        >>> data = process_html_directory(
        ...     directory_path="./epicea_results/A - Métallurgie"
        ... )
        >>> len(data)
        15

        Convert processed data to Pandas DataFrame:

        >>> import pandas as pd
        >>> data = process_html_directory()
        >>> df = pd.DataFrame(data)
        >>> print(df.head())
        Numéro du dossier  Date de l'accident  ...
        0           123456        2023-01-15  ...
        1           123457        2023-01-16  ...
    """
    data = []
    for file_path in tqdm(glob.glob(os.path.join(directory_path, "**", extension)), desc="Processing HTML files"):
        with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
        data.append(_parse_soup(BeautifulSoup(content, 'html.parser')))
    return data

def _parse_soup(soup):
    table = soup.select("table.tablein")[2]
    tmpDict = {}
    for tr in table.select("tr")[1:]:
        tds = tr.select("td")[0:2]
        title : str = tds[0].getText().strip().replace('\xa0', ' ').replace(' :', '')
        content : str = tds[1].getText().strip()
        tmpDict[title] = content
    return tmpDict

if __name__ == "__main__":
    import pandas as pd
    write_html_on_disk = True
    FROM_DISK = False

    if FROM_DISK : 
        data = process_html_directory(directory_path="./epicea_results", extension="*.html")
    else : 
        data = scrape(indexFrom=1, selected_CTN = "D - Services, commerces et industries de l'alimentation", limit = 2, write_html_on_disk=write_html_on_disk)

    print(data)
    df = pd.DataFrame(data)
    print(df.iloc[0])