from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
import requests, os, logging, time, json
from urllib.parse import urlparse, urlunparse
from typing import List, Dict, Optional, Tuple
from functools import wraps
from dataclasses import dataclass, field

# Configuration classes and utilities
@dataclass
class CrawlerConfig:
    """Configuration class for WebCrawler"""
    base_url: str
    exclude_urls: List[str] = field(default_factory=list)
    include_domains: Optional[List[str]] = None
    include_urls: Optional[List[str]] = None
    include_urls_regex: Optional[List] = None
    include_domains_regex: Optional[List] = None
    ignore_anchor_link: bool = False
    headless: bool = True
    timeout: int = 20
    user_agent: Optional[str] = None
    max_retries: int = 3
    retry_delay: int = 1
    enable_javascript: bool = True
    scroll_strategy: str = "auto"

class ScrollStrategy:
    """Configuration for different scrolling strategies"""
    AUTO = "auto"           # Automatic detection and scrolling
    FULL = "full"           # Scroll to absolute bottom
    LAZY = "lazy"           # Target lazy-loaded elements specifically
    INCREMENTAL = "incremental"  # Small incremental scrolls
    INTERACTIVE = "interactive"  # Include user interaction simulation

def retry_on_failure(max_retries=3, delay=1):
    """Decorator for retrying failed operations"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logging.error(f"Failed after {max_retries} attempts: {e}")
                        raise
                    logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                    time.sleep(delay * (attempt + 1))  # Exponential backoff
            return None
        return wrapper
    return decorator

class WebCrawler:
    def __init__(self, base_url, exclude_urls, driver_path=None, agent=None, 
                 include_domains=None, include_urls=None, include_urls_regex=None, 
                 include_domains_regex=None, ignore_anchor_link=False, 
                 enable_javascript=True, scroll_strategy="auto", timeout=20):
        
        self.base_url = base_url
        self.exclude_urls = exclude_urls or []
        self.include_domains = include_domains
        self.include_urls = include_urls
        self.ignore_anchor_link = ignore_anchor_link
        self.include_urls_regex = include_urls_regex
        self.include_domains_regex = include_domains_regex
        self.scroll_strategy = scroll_strategy
        self.timeout = timeout
        
        # Dynamic content configuration
        self.scroll_pause_time = 2  # Time to wait after each scroll
        self.max_scroll_attempts = 10  # Maximum number of scroll attempts
        self.element_load_timeout = 10  # Timeout for waiting for elements
        
        self.driver = self._create_driver(agent, enable_javascript)
    
    def _create_driver(self, agent=None, enable_javascript=True):
        """Create Chrome driver optimized for dynamic content"""
        chrome_options = Options()
        
        # Run Chrome in headless mode
        chrome_options.add_argument("--headless")

        # Performance optimizations
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        
        # Memory optimizations
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=4096")
        
        # Optimize for dynamic content
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Set window size for consistent rendering
        chrome_options.add_argument("--window-size=1920,1080")

        # Disable infobars on startup
        chrome_options.add_argument("--disable-infobars")

        # Disable notifications
        chrome_options.add_argument("--disable-notifications")

        # Disable pop-up blocking
        chrome_options.add_argument("--disable-popup-blocking")

        # Disable automatic software updates
        chrome_options.add_argument("--disable-software-rasterizer")

        # Disable prompt for user data sync
        chrome_options.add_argument("--disable-sync")

        # Disable translate UI
        chrome_options.add_argument("--disable-translate")

        # Disable save password bubbles
        chrome_options.add_argument("--disable-save-password-bubble")

        # Disable autoplay of embedded videos
        chrome_options.add_argument("--autoplay-policy=user-gesture-required")

        # Disable logging
        chrome_options.add_argument("--log-level=3")

        # Silent logging
        chrome_options.add_argument("--silent")
        
        # Conditionally disable JavaScript
        if not enable_javascript:
            chrome_options.add_argument("--disable-javascript")
        
        # Don't load images unless needed for dynamic content
        if not enable_javascript:
            chrome_options.add_argument("--disable-images")

        # Set user agent if provided
        if agent:
            chrome_options.add_argument(f'--user-agent={agent}')
        
        driver = webdriver.Chrome(options=chrome_options)
        
        # Execute script to hide automation indicators
        if enable_javascript:
            try:
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            except Exception as e:
                logging.debug(f"Could not hide automation indicators: {e}")
        
        return driver

    @retry_on_failure(max_retries=3)
    def visit_url(self, url):
        """Visit URL with improved error handling and retries"""
        try:
            self.driver.set_page_load_timeout(self.timeout)
            self.driver.get(url)
            
            # Wait for initial page load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            logging.info(f"Successfully loaded: {url}")
            return True
        except TimeoutException as e:
            logging.error(f"Page load timed out for {url}: {e}")
            raise
        except Exception as e:
            logging.error(f"Error loading {url}: {e}")
            raise
    
    def visit_url_with_dynamic_content(self, url: str) -> bool:
        """Visit URL and handle dynamic content loading"""
        try:
            self.driver.set_page_load_timeout(30)
            self.driver.get(url)
            
            # Wait for initial page load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Handle dynamic content based on strategy
            if self.scroll_strategy == "auto":
                self._auto_scroll_and_load()
            elif self.scroll_strategy == "full":
                self._scroll_to_bottom_with_loading()
            elif self.scroll_strategy == "lazy":
                self._trigger_lazy_loading()
            elif self.scroll_strategy == "incremental":
                self._incremental_scroll()
            elif self.scroll_strategy == "interactive":
                self._auto_scroll_and_load()
                self.simulate_user_interactions()
            
            logging.info(f"Successfully loaded dynamic content for: {url}")
            return True
            
        except TimeoutException as e:
            logging.error(f"Page load timed out for {url}: {e}")
            return False
        except Exception as e:
            logging.error(f"Error loading {url}: {e}")
            return False

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
        """Enhanced link extraction with better error handling"""
        try:
            link_elements = element.find_elements(By.TAG_NAME, "a")
            raw_links = [elem.get_attribute("href") for elem in link_elements if elem.get_attribute("href")]
            
            processed_links = []
            for link in raw_links:
                if processed_link := self._process_link(link, exclude, file_types, include):
                    processed_links.append(processed_link)
            
            return self.remove_duplicate_links(processed_links), raw_links
        except Exception as e:
            logging.error(f"Error extracting links: {e}")
            return [], []

    def _process_link(self, link, exclude, file_types, include):
        """Process individual link with filtering rules"""
        if not link or link.startswith('mailto:'):
            return None
        
        link = link.strip()
        logging.info(f"Link found on the page: {link}")
        
        parsed_link = urlparse(link)
        
        # Handle anchor links
        if self.ignore_anchor_link:
            parsed_link = parsed_link._replace(fragment='')
            link = urlunparse(parsed_link)
        
        # Apply filters
        if not self._passes_filters(link, parsed_link, exclude, file_types, include):
            return None
        
        logging.info(f"Link extracted: {link}")
        return link

    def _passes_filters(self, link, parsed_link, exclude, file_types, include):
        """Check if link passes all filtering criteria"""
        # Exclude filter
        if exclude and any(link.startswith(prefix) for prefix in self.exclude_urls):
            logging.info(f"Link excluded by exclude URLs: {link}")
            return False
        
        # Domain filters
        if not self._passes_domain_filters(parsed_link, link):
            return False
        
        # Include URL filters
        if include and not self._passes_include_filters(link):
            return False
        
        # File type filters
        if file_types and not self._passes_file_type_filters(parsed_link, file_types, link):
            return False
        
        return True
    
    def _passes_domain_filters(self, parsed_link, link):
        """Check domain filtering rules"""
        if self.include_domains and parsed_link.netloc.lower() not in self.include_domains:
            logging.info(f"Link not included by domain rules: {link}")
            return False

        if self.include_domains_regex and not any(pattern.fullmatch(parsed_link.netloc.lower()) for pattern in self.include_domains_regex):
            logging.info(f"Link not included by domain regex rules: {link}")
            return False
        
        return True
    
    def _passes_include_filters(self, link):
        """Check include URL filtering rules"""
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

        return include_match
    
    def _passes_file_type_filters(self, parsed_link, file_types, link):
        """Check file type filtering rules"""
        logging.info(f"Link checking with File Types: {link}")
        for file_type in file_types:
            logging.debug(f"Link: {parsed_link.path.lower()} comparing with file type: {file_type}")
            if parsed_link.path.lower().endswith(file_type):
                return True
            if file_type == 'html' and '.' not in parsed_link.path:
                return True
        return False
    
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
        response = requests.get(url)
        return response

    def parse_page(self):
        main_content = self.driver.find_element(By.TAG_NAME, "body")
        return main_content.text

    def close(self):
        """Clean up resources"""
        try:
            self.cleanup_driver_cache()
        except Exception as e:
            logging.warning(f"Error during cleanup: {e}")
        finally:
            if self.driver:
                self.driver.quit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def _auto_scroll_and_load(self):
        """Automatically scroll and detect new content loading"""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        
        while scroll_attempts < self.max_scroll_attempts:
            # Scroll down to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait for new content to load
            time.sleep(self.scroll_pause_time)
            
            # Check if new content was loaded
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                # Try scrolling in smaller increments to trigger lazy loading
                self._incremental_scroll()
                break
            
            last_height = new_height
            scroll_attempts += 1
            
        logging.info(f"Completed auto-scroll after {scroll_attempts} attempts")
    
    def _scroll_to_bottom_with_loading(self):
        """Scroll to bottom and wait for all content to load"""
        # Get initial page height
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        
        while True:
            # Scroll down to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Wait for page to load
            time.sleep(self.scroll_pause_time)
            
            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                break
                
            last_height = new_height
        
        logging.info("Scrolled to bottom and loaded all content")
    
    def _incremental_scroll(self):
        """Scroll in small increments to trigger lazy loading"""
        try:
            viewport_height = self.driver.execute_script("return window.innerHeight")
            page_height = self.driver.execute_script("return document.body.scrollHeight")
            
            current_position = 0
            scroll_increment = viewport_height // 3  # Scroll 1/3 of viewport at a time
            
            while current_position < page_height:
                current_position += scroll_increment
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                
                # Wait for potential lazy loading
                time.sleep(0.5)
                
                # Check for new elements that might have loaded
                self._wait_for_lazy_elements()
            
            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error during incremental scroll: {e}")
    
    def _trigger_lazy_loading(self):
        """Specifically target lazy-loaded elements"""
        lazy_selectors = [
            "[data-lazy]",
            "[lazy]", 
            "[data-src]",
            ".lazy",
            ".lazyload",
            "[loading='lazy']"
        ]
        
        for selector in lazy_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    try:
                        # Scroll element into view
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        time.sleep(0.5)
                        
                        # Try to trigger loading by hovering
                        ActionChains(self.driver).move_to_element(element).perform()
                        time.sleep(0.5)
                        
                    except StaleElementReferenceException:
                        continue
                        
            except Exception as e:
                logging.debug(f"Error processing lazy selector {selector}: {e}")
    
    def _wait_for_lazy_elements(self):
        """Wait for lazy elements to become visible"""
        try:
            # Wait for images to load
            WebDriverWait(self.driver, 2).until(
                lambda driver: driver.execute_script(
                    "return Array.from(document.images).every(img => img.complete)"
                )
            )
        except TimeoutException:
            pass  # Continue if images don't load within timeout
    
    def simulate_user_interactions(self):
        """Simulate user interactions to trigger dynamic content"""
        try:
            # Simulate mouse movements and clicks to trigger hover effects
            body = self.driver.find_element(By.TAG_NAME, "body")
            
            # Move mouse around the page
            actions = ActionChains(self.driver)
            actions.move_to_element(body).perform()
            
            # Try clicking on common interactive elements
            interactive_selectors = [
                "button:not([disabled])",
                ".load-more",
                ".show-more", 
                "[data-toggle]",
                ".accordion-header",
                ".tab-header"
            ]
            
            for selector in interactive_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements[:3]:  # Limit to first 3 elements
                        try:
                            if element.is_displayed() and element.is_enabled():
                                self.driver.execute_script("arguments[0].click();", element)
                                time.sleep(1)
                        except Exception:
                            continue
                except Exception:
                    continue
                    
        except Exception as e:
            logging.error(f"Error simulating user interactions: {e}")
    
    def get_all_content_including_hidden(self) -> Dict[str, any]:
        """Extract all content including initially hidden elements"""
        content = {
            'visible_text': self.get_visible_text(),
            'hidden_content': self.get_hidden_content(),
            'all_links': self.get_all_links_including_hidden(),
            'dynamic_elements': self.get_dynamic_elements(),
            'page_metadata': self.get_page_metadata()
        }
        
        return content
    
    def get_visible_text(self) -> str:
        """Get all visible text content"""
        try:
            # Get text from body, excluding script and style tags
            visible_text = self.driver.execute_script("""
                function getVisibleText(element) {
                    if (element.tagName === 'SCRIPT' || element.tagName === 'STYLE') {
                        return '';
                    }
                    
                    let text = '';
                    for (let child of element.childNodes) {
                        if (child.nodeType === 3) { // Text node
                            text += child.textContent + ' ';
                        } else if (child.nodeType === 1) { // Element node
                            let style = window.getComputedStyle(child);
                            if (style.display !== 'none' && style.visibility !== 'hidden') {
                                text += getVisibleText(child);
                            }
                        }
                    }
                    return text;
                }
                return getVisibleText(document.body);
            """)
            
            return visible_text.strip()
            
        except Exception as e:
            logging.error(f"Error getting visible text: {e}")
            return self.driver.find_element(By.TAG_NAME, "body").text
    
    def get_hidden_content(self) -> List[Dict[str, str]]:
        """Extract content from hidden elements"""
        hidden_content = []
        
        try:
            # Find elements that are hidden but contain content
            hidden_elements = self.driver.execute_script("""
                let hiddenElements = [];
                let allElements = document.querySelectorAll('*');
                
                for (let element of allElements) {
                    let style = window.getComputedStyle(element);
                    let hasContent = element.textContent.trim().length > 0 || 
                                   element.querySelector('img, video, audio');
                    
                    if (hasContent && (
                        style.display === 'none' || 
                        style.visibility === 'hidden' ||
                        style.opacity === '0' ||
                        element.hidden ||
                        element.getAttribute('aria-hidden') === 'true'
                    )) {
                        hiddenElements.push({
                            tagName: element.tagName,
                            className: element.className,
                            id: element.id,
                            textContent: element.textContent.trim().substring(0, 500),
                            innerHTML: element.innerHTML.substring(0, 1000)
                        });
                    }
                }
                
                return hiddenElements;
            """)
            
            hidden_content = hidden_elements
            
        except Exception as e:
            logging.error(f"Error extracting hidden content: {e}")
        
        return hidden_content
    
    def get_all_links_including_hidden(self) -> Tuple[List[str], List[str]]:
        """Get all links including those in hidden elements"""
        try:
            all_links = self.driver.execute_script("""
                let links = [];
                let allLinks = document.querySelectorAll('a[href]');
                
                for (let link of allLinks) {
                    links.push(link.href);
                }
                
                return links;
            """)
            
            # Filter links using existing logic
            filtered_links = []
            for link in all_links:
                if processed_link := self._process_link_simple(link):
                    filtered_links.append(processed_link)
            
            return self.remove_duplicate_links(filtered_links), all_links
            
        except Exception as e:
            logging.error(f"Error getting all links: {e}")
            return [], []
    
    def get_dynamic_elements(self) -> List[Dict[str, str]]:
        """Get information about dynamic elements on the page"""
        try:
            dynamic_info = self.driver.execute_script("""
                let dynamicElements = [];
                
                // Find elements with data attributes commonly used for dynamic content
                let selectors = [
                    '[data-lazy]',
                    '[data-src]',
                    '[data-bind]',
                    '[ng-repeat]',
                    '[v-for]',
                    '[data-react]',
                    '.lazy',
                    '.lazyload'
                ];
                
                for (let selector of selectors) {
                    let elements = document.querySelectorAll(selector);
                    for (let element of elements) {
                        dynamicElements.push({
                            selector: selector,
                            tagName: element.tagName,
                            className: element.className,
                            id: element.id,
                            attributes: Array.from(element.attributes).map(attr => 
                                ({name: attr.name, value: attr.value})
                            )
                        });
                    }
                }
                
                return dynamicElements;
            """)
            
            return dynamic_info
            
        except Exception as e:
            logging.error(f"Error getting dynamic elements: {e}")
            return []
    
    def get_page_metadata(self) -> Dict[str, any]:
        """Get page metadata and performance information"""
        try:
            metadata = self.driver.execute_script("""
                return {
                    title: document.title,
                    url: window.location.href,
                    scrollHeight: document.body.scrollHeight,
                    clientHeight: document.documentElement.clientHeight,
                    imageCount: document.images.length,
                    linkCount: document.links.length,
                    scriptCount: document.scripts.length,
                    hasLazyElements: document.querySelectorAll('[data-lazy], [lazy], [data-src]').length > 0,
                    hasInfiniteScroll: document.body.scrollHeight > window.innerHeight * 3
                };
            """)
            
            return metadata
            
        except Exception as e:
            logging.error(f"Error getting page metadata: {e}")
            return {}
    
    def _process_link_simple(self, link: str) -> Optional[str]:
        """Simplified link processing for dynamic content"""
        if not link or link.startswith('mailto:') or link.startswith('javascript:'):
            return None
        
        # Basic filtering - you can expand this based on your existing logic
        if self.exclude_urls and any(link.startswith(prefix) for prefix in self.exclude_urls):
            return None
        
        return link.strip()
    
    def crawl_dynamic_content(self, url: str, scroll_strategy: str = None) -> Dict[str, any]:
        """Enhanced method to crawl pages with dynamic content"""
        if scroll_strategy:
            original_strategy = self.scroll_strategy
            self.scroll_strategy = scroll_strategy
        
        try:
            # Visit URL and handle dynamic content
            if self.visit_url_with_dynamic_content(url):
                # Extract all content including hidden elements
                content = self.get_all_content_including_hidden()
                
                # Simulate user interactions if needed
                if self.scroll_strategy == "interactive":
                    self.simulate_user_interactions()
                    
                    # Get updated content after interactions
                    updated_content = self.get_all_content_including_hidden()
                    
                    # Merge content
                    content['post_interaction_links'] = updated_content['all_links']
                
                return content
            else:                return None
                
        finally:
            if scroll_strategy:
                self.scroll_strategy = original_strategy
    
    def cleanup_driver_cache(self):
        """Clean up driver cache and temporary files"""
        try:
            if self.driver:
                # Clear cookies
                try:
                    self.driver.delete_all_cookies()
                except Exception as e:
                    logging.debug(f"Could not clear cookies: {e}")
                
                # Clear localStorage and sessionStorage
                try:
                    # Check if localStorage is available before clearing
                    self.driver.execute_script("""
                        try {
                            if (typeof(Storage) !== "undefined" && window.localStorage) {
                                window.localStorage.clear();
                            }
                        } catch(e) {
                            console.log('localStorage not available:', e);
                        }
                    """)
                except Exception as e:
                    logging.debug(f"Could not clear localStorage: {e}")
                
                try:
                    # Check if sessionStorage is available before clearing
                    self.driver.execute_script("""
                        try {
                            if (typeof(Storage) !== "undefined" && window.sessionStorage) {
                                window.sessionStorage.clear();
                            }
                        } catch(e) {
                            console.log('sessionStorage not available:', e);
                        }
                    """)
                except Exception as e:
                    logging.debug(f"Could not clear sessionStorage: {e}")
                    
        except Exception as e:
            logging.debug(f"Error during cache cleanup: {e}")

    def take_screenshot(self, filename=None):
        """Take screenshot for debugging"""
        if not filename:
            filename = f"debug_screenshot_{int(time.time())}.png"
        try:
            self.driver.save_screenshot(filename)
            logging.info(f"Screenshot saved: {filename}")
        except Exception as e:
            logging.error(f"Failed to take screenshot: {e}")

    def get_page_metrics(self):
        """Get page performance metrics"""
        try:
            navigation_timing = self.driver.execute_script(
                "return window.performance.timing"
            )
            return {
                'load_time': navigation_timing['loadEventEnd'] - navigation_timing['navigationStart'],
                'dom_ready': navigation_timing['domContentLoadedEventEnd'] - navigation_timing['navigationStart']
            }
        except Exception as e:
            logging.error(f"Error getting page metrics: {e}")
            return {}
