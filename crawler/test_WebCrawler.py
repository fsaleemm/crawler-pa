import unittest
from selenium.webdriver.common.by import By
from WebCrawler import WebCrawler

class TestWebCrawler(unittest.TestCase):
    def setUp(self):
        self.base_url = "https://www.panynj.gov/port-authority/en/business-opportunities/solicitations-advertisements/Construction.html"
        self.exclude_list = ["https://panynj.bonfirehub.com", "https://www.panynj.gov/content/dam/port-authority/pdfs/bid-proposal/Addendum"]
        self.crawler = WebCrawler(base_url=self.base_url, exclude_urls=self.exclude_list)

    def test_visit_url(self):
        self.crawler.visit_url(self.base_url)
        self.assertEqual(self.crawler.driver.current_url, self.base_url)

    def test_get_elements(self):
        self.crawler.visit_url(self.base_url)
        elements = self.crawler.get_elements(By.TAG_NAME, "table")
        self.assertGreater(len(elements), 0)

    def test_parse_tables(self):
        self.crawler.visit_url(self.base_url)
        tables = self.crawler.get_elements(By.TAG_NAME, "table")
        table_dict, deduped_links = self.crawler.parse_tables(tables)
        self.assertIsInstance(table_dict, dict)
        self.assertIsInstance(deduped_links, list)
        self.assertTrue(len(deduped_links) > 0)
        self.assertFalse([item for item in deduped_links if item.startswith("https://panynj.bonfirehub.com")])
        self.assertFalse([item for item in deduped_links if item.startswith("https://www.panynj.gov/content/dam/port-authority/pdfs/bid-proposal/Addendum")])

    def tearDown(self):
        self.crawler.close()

if __name__ == '__main__':
    unittest.main()