import os

class Config:

    USE_PREPARED = True

    # Пути
    DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
    COOKIES_PATH =  os.path.join(DATA_DIR, 'cookies.pkl')
    REVIEWS_JSON_PATH = os.path.join(DATA_DIR, 'reviews.json')
    RAW_HTML_PATH = os.path.join(DATA_DIR, 'raw_html.txt')
    STATIC_DIR = os.path.join(os.path.dirname(__file__), 'static')
    WORDCLOUDS_PATH = os.path.join(os.path.dirname(__file__), 'preapred_wordclouds')

    # Парсер
    PARSER_WAITING_TIME_LB = 4 # Нижняя граница задержки перед осуществлением действия в браузере
    PARSER_WAITING_TIME_UB = 6 # Верхняя граница
    PARSER_REVIEWS_LB = 10 # min желаемого количества отзывов
    PARSER_REVIEWS_UB = 400 # max желаемого количества отзывов

    # Parser Pool
    PARSER_POOL_SIZE = 2 # Размер пула парсеров
    PARSER_POOL_BLOCK = True # Если не удается получить/вернуть в пул экземпляр парсера, то через timeout будет вызвано исключение Empty/Full
    PARSER_POOL_TIMEOUT = 5 # Время ожидания получения/возврата парсера в пул

    # Браузер
    BROWSER_HEADLESS = False # Режим управления браузером без UI
    BROWSER_CITE_OPENNIG_ATTEMPS = 2 # Попытки на открытие сайта при возникновении ошибки 403

    # Анализ
    ANALYZER_TO_LOWERCASE = True # К нижнему регистру
    ANALYZER_MIN_REVIEW_LEN_TRESHOLD = 5 # Минимальная длина отзыва
    ANALYZER_LANGUAGE = 'russian' # 
    ANALYZER_ERASE_PUNCTUATION = False # Удаление пунктуации
    ANALYZER_TOP_N_KEYWORDS = 2 # Количество извлекаемых ключевых фраз
    ANALYZER_CONFIDENCE_TRESHOLD = 0.5 # Порог косинусного сходства фразы с отзывом
    ANALYZER_KEYPHRASE_NGRAM_RANGE = (2, 3) # Количество токенов в ключевой фразе
    # ANALYZER_KEYBERT_MODEL_NAME = 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2' Плохое качество ключевых фраз
    ANALYZER_KEYBERT_MODEL_NAME = 'sentence-transformers/paraphrase-multilingual-mpnet-base-v2'
    ANALYZER_KEYBERT_DIVERSITY = 0.7 # Разнообразие ключевых фраз. Чем выше, тем более разнообразны фразы
    ANALYZER_ALLOWED_STOPWORDS = ['не', 'ни', 'но'] # Стоп-слова, которые не будут удаляться
    ANALYSIS_TIMEOUT = 60 # Таймаут ожидания анализа отзвов

    # WordCloud
    WORDCLOUD_WIDTH = 1000 # 
    WORDCLOUD_HEIGHT = 1000 # 
    WORDCLOUD_BACKGROUND_COLOR = 'black' # 
    WORDCLOUD_COLORMAP = 'Pastel1' # 
    WORDCLOUD_RANDOM_STATE = 42 # 
    WORDCLOUD_COLLOCATIONS = True # Позволяет использовать N_GRAMMы
    WORDCLOUD_MARGIN = 20 # 