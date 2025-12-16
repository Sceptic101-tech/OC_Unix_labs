import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from worker_config import Config
import random
import time
import os.path

# TODO mimicking a human reaction for actions

WAITING_TIME_LB, WAITING_TIME_UB = (Config.PARSER_WAITING_TIME_LB, Config.PARSER_WAITING_TIME_UB)

def extract_reviews(self, raw_html:str) -> list:
    """
    Извлекает отзывы из html‑страницы.

    Parameters
    ----------
    raw_html : str
        Сырой HTML-код страницы с отзывами.

    Returns
    -------
    list[dict]
       Список словарей, где каждый элемент представляет один отзыв.
        Ключи: 'Достоинства', 'Недостатки', 'Комментарий', 'Фото'.
    """
    soup = BeautifulSoup(raw_html, 'html.parser')
    # search for all reviews. Depends on the page source code. We will get a list with all the reviews on the page.
    # 'ow-opinion__texts' contains lables(Достоинства, Недостатки, ...) and user comments block
    all_reviews = soup.find_all('div', 'ow-opinion__texts', recursive=True)
    parsed_reviews = []
    for review in all_reviews:
        title_part = review.find_all('div', 'ow-opinion__text-title') # extracting only "pros" and "cons" labels
        desc_part = review.find_all('div', 'ow-opinion__text-desc') # extracting only user comments
        parsed_reviews.append({'Достоинства' : None, 'Недостатки' : None, 'Комментарий' : None, 'Фото' : None}) # not a bug, just feature
        for el in zip(title_part, desc_part):
            parsed_reviews[-1][el[0].text] = el[1].text # merging label with comments to dictionary as key and value
    return parsed_reviews # list of dict, each element is review

class BrowserManager:
    """
    Упрощённый обёртка над undetected_chromedriver.

    Attributes
    ----------
    browser : uc.Chrome
        Экземпляр браузера, который используется для всех операций.
    """


    def __init__(self, headless: bool):
        """
        Инициализация экземпляра BrowserManager.

        Parameters
        ----------
        headless : bool
            Если True – открывать браузер в режиме «headless» (без UI).
        """
        self.browser = uc.Chrome(headless=headless)
        self.browser.implicitly_wait(5)
        self.browser.maximize_window()
        time.sleep(random.randint(Config.PARSER_WAITING_TIME_LB, Config.PARSER_WAITING_TIME_UB))


    def _change_main_window(self):
        """
        Переключает фокус на главное окно браузера.
        Если открыто несколько вкладок, переключается к той,
        которая не является «основной».
        """
        if len(self.browser.window_handles) > 1:
            original_window = self.browser.current_window_handle
            for window_handle in self.browser.window_handles:
                if window_handle != original_window:
                    self.browser.switch_to.window(window_handle)
                    break


    def _close_excess_windows(self):
        """
        Закрывает все лишние вкладки, оставляя только главную.
        """
        if len(self.browser.window_handles) > 1:
            original_window = self.browser.current_window_handle
            for window_handle in self.browser.window_handles:
                if window_handle != original_window:
                    self.browser.switch_to.window(window_handle)
                    self.browser.close()
                    self.browser.switch_to.window(original_window)


    def write_cookies(self, filename : str):
        """
        Сохраняет cookies текущей сессии в файл.

        Parameters
        ----------
        filename : str
            Путь к файлу, куда будут записаны cookies.
        """
        with open(filename, 'w') as file:
            cookies = self.browser.get_cookies()
            file.write(cookies)


    def read_cookies(self, filename : str):
        """
        Загружает cookies из файла и добавляет их в текущую сессию.

        Parameters
        ----------
        filename : str
            Путь к файлу с cookies.
        """
        if os.path.exists(filename):
            with open(filename, 'r') as file:
                cookies = file.read()
                self.browser.add_cookie(cookies)


    def __del__(self) -> None:  # pragma: no cover
        """
        Закрывает браузер при удалении объекта.
        """
        self.browser.quit()


class NavigationManager:
    """
    Упрощённый менеджер навигации, скрывающий детали работы с Selenium.

    Parameters
    ----------
    browser : BrowserManager
        Экземпляр BrowserManager, через который осуществляется взаимодействие.
    """
    def __init__(self, browser : BrowserManager):
        self.browser_manager = browser


    def _handle_403_error(self):
        """
        При возникновении ошибки 403 открывает новую вкладку с https://ya.ru,
        переключается на неё и закрывает все лишние окна.
        """
        self.browser_manager.browser.execute_script('''window.open("https://ya.ru","_blank");''')
        self.browser_manager._change_main_window()
        time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))
        self.browser_manager._close_excess_windows()


    # finding the query line and enters the query. Works only for Yandex search
    def _enter_query(self, query:str):
        """
        Вводит поисковый запрос в поле поиска Яндекс.

        Parameters
        ----------
        query : str
            Текст запроса.
        """
        search = self.browser_manager.browser.find_element(By.ID, "text")
        search.clear()
        search.send_keys(query)
        time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))
        search.send_keys(Keys.RETURN)


    # GET query with 403_error handle 
    def try_open_page(self, url:str, attempts:int):
        """
        Пытается открыть страницу по URL с заданным числом попыток.

        Parameters
        ----------
        url : str
            Адрес страницы.
        attempts : int
            Максимальное число попыток.

        Returns
        -------
        bool
            True – если страница открылась успешно, False – иначе.
        """
        for i in range(attempts):
            self.browser_manager.browser.get(url)
            # in case of an 403 authorisation error, try to open a new tab and try again.
            if 'HTTP 403' in self.browser_manager.browser.page_source:
                if i == attempts-1:
                    print('Couldnt open the page in the specified number of attempts')
                    return False
                print(f'Error 403. Trying open link in another tab. Attempt {i+1}')
                self._handle_403_error()
                time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))
            else:
                return True


    # The main idea is to create a website cookie by visiting it through a search. This helps to bypass the bot check
    def try_open_page_via_search(self, search_query:str, url_part:str, attempts:int):
        """
        Открывает страницу по URL, предварительно найдя её в поиске Яндекса.
        Это помогает обойти проверку на наличие cookie.

        Parameters
        ----------
        search_query : str
            Текст для поиска (например, «днс»).
        url_part : str
            Фрагмент адреса сайта, который нужно найти в результатах поиска
            (например, «dns-shop.ru»).
        attempts : int
            Максимальное число попыток.

        Returns
        -------
        bool
            True – если страница открылась успешно, False – иначе.
        """
        for i in range(attempts):
            self.browser_manager.browser.get('https://ya.ru')
            time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))
            try:
                self._enter_query(search_query)
                time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))

                # button with dumb suggestions may appear, this will help us get rid of it.
                notification_close_button = WebDriverWait(self.browser_manager.browser, 8).until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.Distribution-ButtonClose')))
                notification_close_button.click()
                time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))

                # looking for url, that matches url_part
                link = self.browser_manager.browser.find_element(By.PARTIAL_LINK_TEXT, url_part)
                time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))
                link.click()
                self.browser_manager._change_main_window()
                time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))
                self.browser_manager._close_excess_windows()
            except:
                print('Cant find interactive element :(')
            
            # in case of an 403 authorization error, opening a new tab and try again
            if 'HTTP 403' in self.browser_manager.browser.page_source:
                if i == attempts-1:
                    print('Couldnt open the page in the specified number of attempts')
                    return False
                print(f'Error 403. Trying open link in another tab. Attempt {i+1}')
                self._handle_403_error()
                time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))
            else:
                return True


class ParseHelper:
    """
    Утилита для работы с содержимым открытой страницы.

    Parameters
    ----------
    browser : BrowserManager
        Экземпляр BrowserManager, через который можно получить html‑код.
    """
    def __init__(self, browser : BrowserManager):
        self.browser_manager = browser


    def get_page_raw_html(self) -> str:
        """
        Возвращает сырой HTML-код открытой страницы.

        Returns
        -------
        str
            Строка с полным html‑кодом.
        """
        return self.browser_manager.browser.page_source


    def page_to_txt(self, file_name:str):
        """
        Сохраняет raw‑html в файл.

        Parameters
        ----------
        file_name : str
            Путь к файлу, куда будет записан html‑код.
        """
        with open(file_name, 'w', encoding='utf-8') as file:
            raw_html = self.get_page_raw_html()
            file.write(raw_html)


class DNS_Shop_Parser:
    """
    Парсер отзывов с сайта dns‑shop.ru.

    Parameters
    ----------
    headless : bool
        Если True – браузер запускается без UI.
    """
    def __init__(self, headless : bool):
        self.browser_manager = BrowserManager(headless=headless)
        self.navigator = NavigationManager(self.browser_manager)
        self.parse_helper = ParseHelper(self.browser_manager)
        self.successful_open=False


    def how_to_use(self):
        """
        Возвращает краткую инструкцию по использованию парсера.
        """
        return '''
                1. Create an instance of the DNS_parser object (this has already been done)
                2. Use the /.open_DNS_site()/ method to avoid bot-checking for access to the DNS-site
                3.1. Use the /.get_product_reviews('product_reviews_url')/ to get reviews in list of dictionaries format
                3.2. Use the /parse_helper.page_to_txt()/ method to get raw html code in .txt file'''


    # works only for dns_review page
    def show_more_reviews(self, desired_review_cnt:int):
        """
        Клик по кнопке «Показать ещё» до тех пор, пока не будет загружено
        нужное количество отзывов.

        Parameters
        ----------
        desired_review_cnt : int
            Желаемое число отзывов на странице.
        """
        button_click_cnt = (desired_review_cnt-4)//10 + 1 # magic number. Each button click produces 10 reviews. There is only 4 reviews displayed at the beggining
        for _ in range(button_click_cnt):
            time.sleep(random.randint(WAITING_TIME_LB, WAITING_TIME_UB))
            try:
                # show_more_button = self.browser_manager.browser.find_element(By.CSS_SELECTOR, "button.paginator-widget__more")
                show_more_button = WebDriverWait(self.browser_manager.browser, 8).until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.paginator-widget__more')))
                show_more_button.click()
            except:
                print('Couldnt get the desired number of reviews')
    

    def open_DNS_site(self, attempts: int):
        """
        Пытается открыть главную страницу dns‑shop.ru через поисковую систему.
        Если открытие прошло успешно – устанавливает flag `successful_open`.
        """
        if self.navigator.try_open_page_via_search('днс', 'dns-shop.ru', attempts=attempts):
            self.successful_open=True


    def get_product_reviews(self, url_review_page: str, desired_review_cnt: int, attempts: int):
        """
        Загружает страницу с отзывами и возвращает список отзывов.

        Parameters
        ----------
        url_review_page : str
            URL страницы с отзывами конкретного товара.
        desired_review_cnt : int
            Желаемое число отзывов для загрузки.

        Returns
        -------
        list[dict]
            Список словарей с отзывами, полученный функцией `extract_reviews`.
        """
        self.navigator.try_open_page(url_review_page, attempts=attempts)
        self.show_more_reviews(desired_review_cnt)
        raw_html = self.parse_helper.get_page_raw_html()
        return self.extract_reviews(raw_html)
    
__all__ = ['DNS_Shop_Parser']