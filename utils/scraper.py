from dotenv import dotenv_values, find_dotenv
from screeninfo import get_monitors
from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver import EdgeOptions, ChromeOptions
# from selenium.webdriver.edge import webdriver
from selenium.webdriver.chrome import webdriver
from selenium.webdriver.support.expected_conditions import visibility_of_element_located, any_of
from selenium.webdriver.support.wait import WebDriverWait

config = dotenv_values(find_dotenv())

DOWNLOADS = r'D:\Forge\WorkBench\Appraisal\downloads'
RESOURCE = r'D:\Forge\WorkBench\Appraisal\resources'


class Scraper:

    def __init__(self, url: str):
        self.url = url
        # self.options = EdgeOptions()
        self.options = ChromeOptions()
        self.options.headless = config['HEADLESS'] == 'True'

        # if config['HEADLESS'] == 'True':
        #     self.options.add_argument('--headless')
        #     self.options.add_argument('disable-gpu')

        self.options.add_experimental_option('prefs', {
            'download.default_directory': DOWNLOADS,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True
        })

        self.monitor = get_monitors()[0]

        # self.driver = webdriver.WebDriver(options=self.options)
        self.driver = webdriver.WebDriver(options=self.options)
        # self.driver.maximize_window()
        self.driver.set_window_size(width=self.monitor.width, height=self.monitor.height)
        self.driver.implicitly_wait(int(config['TIMEOUT']))

        self.wait = WebDriverWait(self.driver, int(config['PERIOD']))

    def __enter__(self):
        try:

            self.driver.get(self.url)

        except Exception as e:
            print(e)
            raise e
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.close()
        self.driver.quit()

    def xpath_soup(self, element) -> str:
        components = []
        child = element if element.name else element.parent
        for parent in child.parents:
            siblings = parent.find_all(child.name, recursive=False)
            components.append(
                child.name
                if siblings == [child] else
                '%s[%d]' % (child.name, 1 + siblings.index(child))
            )
            child = parent
        components.reverse()
        return '/%s' % '/'.join(components)

    def element(self, parameters: list[tuple]):
        element = None

        for (by, value) in parameters:
            try:
                element = self.driver.find_element(by, value)
                break
            except NoSuchElementException:
                print('Element not found')

        return element

    def elements(self, parameters: list[tuple]):
        elements = None

        for (by, value) in parameters:
            try:
                element = self.driver.find_elements(by, value)
                break
            except NoSuchElementException:
                print('Elements not found')

        return elements

    def waited_element(self, parameters: list[tuple]):
        element = None

        try:
            WebDriverWait.until(any_of([visibility_of_element_located(b, v) for b, v in parameters]))

            for (by, value) in parameters:
                try:
                    element = self.driver.find_element(by, value)
                    break
                except NoSuchElementException:
                    print('Element not found')
        except TimeoutException:
            print('Element still not present')

        return element

    def get_elements(self, parameters: list[tuple]) -> tuple:
        return tuple([self.element([(b, v)]) for b, v in parameters])
