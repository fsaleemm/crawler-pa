from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
import requests, os, logging
from urllib.parse import urlparse, urlunparse

class WebCrawler:
    def __init__(self, base_url, exclude_urls, driver_path=None, agent=None, include_domains=None, include_urls=None, include_urls_regex=None, include_domains_regex=None, ignore_anchor_link=False):
        self.chrome_options = Options()
        # Run Chrome in headless mode
        self.chrome_options.add_argument("--headless")

        # Disable GPU hardware acceleration
        self.chrome_options.add_argument("--disable-gpu")

        # Disable infobars on startup
        self.chrome_options.add_argument("--disable-infobars")

        # Disable notifications
        self.chrome_options.add_argument("--disable-notifications")

        # Disable pop-up blocking
        self.chrome_options.add_argument("--disable-popup-blocking")

        # Disable automatic software updates
        self.chrome_options.add_argument("--disable-software-rasterizer")

        # Disable prompt for user data sync
        self.chrome_options.add_argument("--disable-sync")

        # Disable translate UI
        self.chrome_options.add_argument("--disable-translate")

        # Disable save password bubbles
        self.chrome_options.add_argument("--disable-save-password-bubble")

        # Disable autoplay of embedded videos
        self.chrome_options.add_argument("--autoplay-policy=user-gesture-required")

        # Additional options to speed up Chrome
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")

        # Enhanced container-specific options
        self.chrome_options.add_argument("--disable-extensions")
        self.chrome_options.add_argument("--disable-background-timer-throttling")
        self.chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        self.chrome_options.add_argument("--disable-renderer-backgrounding")
        self.chrome_options.add_argument("--disable-features=TranslateUI")
        self.chrome_options.add_argument("--disable-web-security")
        self.chrome_options.add_argument("--ignore-certificate-errors")
        self.chrome_options.add_argument("--allow-running-insecure-content")
        self.chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Memory optimization
        self.chrome_options.add_argument("--memory-pressure-off")
        self.chrome_options.add_argument("--max_old_space_size=4096")

        # Disable logging
        self.chrome_options.add_argument("--log-level=3")

        # Silent logging
        self.chrome_options.add_argument("--silent")

        # Set user agent
        self.chrome_options.add_argument(f'user-agent={agent}')
        
        # Exclude automation switches
        self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.base_url = base_url
        self.exclude_urls = exclude_urls
        self.include_domains = include_domains
        self.include_urls = include_urls
        self.ignore_anchor_link = ignore_anchor_link
        self.include_urls_regex = include_urls_regex
        self.include_domains_regex = include_domains_regex
        self.agent = agent

    def is_session_active(self):
        """Check if the WebDriver session is still active."""
        try:
            self.driver.current_url
            return True
        except Exception:
            return False

    def recover_session(self):
        """Recover from a failed WebDriver session."""
        try:
            if hasattr(self, 'driver'):
                self.driver.quit()
        except:
            pass
        
        # Reinitialize the driver
        self.driver = webdriver.Chrome(options=self.chrome_options)

    def visit_url(self, url):
        try:
            if not self.is_session_active():
                logging.warning(f"WebDriver session inactive, recovering session for {url}")
                self.recover_session()
                
            self.driver.set_page_load_timeout(20)  # Set timeout to 20 seconds
            self.driver.get(url)
        except TimeoutException as e:
            logging.error(f"Page load timed out for {url}, attempting recovery: {e}")
            self.recover_session()
            try:
                self.driver.get(url)
            except Exception as recovery_error:
                logging.error(f"Recovery failed for {url}: {recovery_error}")
                raise
        except Exception as e:
            error_msg = str(e).lower()
            if "invalid session id" in error_msg or "session" in error_msg:
                logging.warning(f"Session error for {url}, attempting recovery: {e}")
                self.recover_session()
                try:
                    self.driver.get(url)
                except Exception as recovery_error:
                    logging.error(f"Recovery failed for {url}: {recovery_error}")
                    raise
            else:
                logging.error(f"Error occurred while loading {url}, Exception: {e}")
                raise

    def get_page_source(self):
        return self.driver.page_source   


    def get_elements(self, strategy, element_selector):
        try:
            elements = self.driver.find_elements(strategy, element_selector)
            return elements
        except Exception as e:
            logging.error(f"Error: {e}")
            return None
        

    def parse_tables(self):
        tables = self.get_elements(By.TAG_NAME, "table")
        table_dict = {}

        for table in tables:
            rows = table.find_elements(By.TAG_NAME, "tr")
            header = []

            for row in rows:
                row_dict = {}
                row_dict["metadata"] = {}
                ref_links = []

                cols = row.find_elements(By.TAG_NAME,"td")

                key_links = []
                if cols:
                    key_links  = self.get_links(cols[0], exclude=True)
                    if key_links:
                        ref_links.extend(key_links)
                    elif len(cols) > 3:
                        key_links = self.get_solicitation_links(cols[3])

                    if len(cols) > 2:
                        ref_links.extend(self.get_links(cols[2], exclude=True))
                    if len(cols) > 3:
                        ref_links.extend(self.get_links(cols[3], exclude=True))
                
                if not header:
                    header = [col.text.strip() for col in row.find_elements(By.TAG_NAME,"th")]
                row_data = [col.text for col in cols]
                
                if row_data:
                    for i in range(len(header)):
                        if i < len(row_data):
                            row_dict["metadata"][header[i]] = row_data[i].strip()
                    
                    if ref_links:
                        deduped_links = list(set(ref_links))
                        row_dict["links"] = deduped_links

                    if row_dict and key_links:
                        table_dict[key_links[0].strip()] = row_dict

        return table_dict


    def get_links(self, element, exclude=False, file_types=None, include=True):
        links = []

        ref_links = element.find_elements(By.TAG_NAME, "a")

        raw_links = []

        if len(ref_links) > 0:
            for ref_link in ref_links:
                link = ref_link.get_attribute("href")
                raw_links.append(link)

                if link:
                    link = link.strip()
                    logging.info(f"Link found on the page: {link}")
                else:
                    continue

                parsed_link = urlparse(link)

                if self.ignore_anchor_link:
                    parsed_link = parsed_link._replace(fragment='')
                    link = urlunparse(parsed_link)

                if not link and not parsed_link:
                    continue


                if not link.startswith('mailto:') and not (exclude and any(link.startswith(prefix) for prefix in self.exclude_urls)):
                    logging.info(f"Link not excluded by exclude URLs: {link}")
                    if self.include_domains and parsed_link.netloc.lower() not in self.include_domains:
                        logging.info(f"Link not included by domain rules: {link}")
                        continue

                    if self.include_domains_regex and not any(pattern.fullmatch(parsed_link.netloc.lower()) for pattern in self.include_domains_regex):
                        logging.info(f"Link not included by domain regex rules: {link}")
                        continue

                    if include:
                        logging.info(f"Link checking with Include URLs: {link}")
                        include_match = False

                        if self.include_urls_regex and any(pattern.fullmatch(link) for pattern in self.include_urls_regex):
                            logging.debug(f"Link: {link} matched with one of include url regex")
                            include_match = True

                        if self.include_urls:
                            for include_url in self.include_urls:
                                logging.debug(f"Comparing link: {link} with include url: {include_url}")
                                if link.lower().startswith(include_url):
                                    logging.debug(f"Link: {link.lower()} matched include url: {include_url}")
                                    include_match = True
                                    break

                        if not include_match:
                            continue

                    
                    if file_types:
                        logging.info(f"Link checking with File Types: {link}")
                        for file_type in file_types:
                            logging.debug(f"Link: {parsed_link.path.lower()} comparing with file type: {file_type}")
                            if parsed_link.path.lower().endswith(file_type):
                                links.append(link)
                                logging.info(f"Link extracted: {link}")
                                break
                            if file_type == 'html' and '.' not in parsed_link.path:
                                links.append(link)
                                logging.info(f"Link extracted: {link}")
                                break
                    else:
                        logging.info(f"Link extracted: {link}")
                        links.append(link)
        
        logging.debug(f"All Links Extracted : {links}")
        return self.remove_duplicate_links(links), raw_links
    
    def remove_duplicate_links(self, links):
        seen = set()
        unique_links = []
        for link in links:
            if link not in seen:
                unique_links.append(link)
                seen.add(link)
        return unique_links

    def get_solicitation_links(self, element):
        links = []

        ref_links = element.find_elements(By.LINK_TEXT, "Solicitation Document")

        if len(ref_links) > 0:
            for ref_link in ref_links:
                links.append(ref_link.get_attribute("href").strip())
        
        return links
    
    def get_pdf(self, url):
        
        headers = {
                "User-Agent": self.agent
            }
        
        response = requests.get(url=url, headers=headers)
        return response

    def parse_page(self):
        main_content = self.driver.find_element(By.TAG_NAME, "body")
        return main_content.text

    def close(self):
        self.driver.quit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()
