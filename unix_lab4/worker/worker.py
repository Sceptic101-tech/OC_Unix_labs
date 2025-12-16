# Функционалом данной лабораторной работы является один из моих pet-проектов: парсинг отзывов и выделение ключеывых слов.  
# Главный скрипт контейнеров работников. Под капотом произаодится парсинг, обработка и анализ отзывов.
# Парсер базируется на веб движке chrome и эмулирует действия пользователя, воизбежание детекции анти-бот проверкой.
# Для данной работы обрезена возможность парсить отзывы, поскольку такая эмуляция требует порядка двух минут ожидания + дополнительные расходы памяти
# Сырой html код страницы с отзывами лежит в директории ./data
# В результате работы получаются два облака слов - положительные и отрицательные аспекты

import json
import os
import socket
import time
import logging
from keybert import KeyBERT
from pymystem3 import Mystem
from bs4 import BeautifulSoup
import scripts.text_processing as tp
import scripts.analyzer as text_analyzer
from scripts.vizualizer import WordCloudGenerator
from kafka import KafkaConsumer
from worker_config import Config
import nltk
import hashlib

# Конфигурация Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'tasks'

logging.warning('Загрузка данных')
keybert_model = KeyBERT(Config.ANALYZER_KEYBERT_MODEL_NAME)
mystem = Mystem()
nltk.download('stopwords')
nltk.download('punkt_tab')
logging.warning('данные загружены')

# Получаем имя хост машины для идентификации воркера
container_id = socket.gethostname()

def extract_reviews(raw_html:str) -> list:
    """
    Извлекает текст отзывов из html‑страницы.

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
    # Ищем все отзывы на странице и собираем в список. 'ow-opinion__texts' содержит пункты (Достоинства, Недостатки, ...) и блок с комментарием пользователя 
    all_reviews = soup.find_all('div', 'ow-opinion__texts', recursive=True)
    parsed_reviews = []
    for review in all_reviews:
        title_part = review.find_all('div', 'ow-opinion__text-title') # Извлекает только метки "Преимущества", "Недостатки"
        desc_part = review.find_all('div', 'ow-opinion__text-desc') # Извлекает только комментарии пользователя к меткам
        parsed_reviews.append({'Достоинства' : None, 'Недостатки' : None, 'Комментарий' : None, 'Фото' : None})
        for el in zip(title_part, desc_part):
            parsed_reviews[-1][el[0].text] = el[1].text # Слияние категорий и текстов к ним в словарь, где каждой категории соответствует текст пользователя
    return parsed_reviews # Список словарей. Каждый элемент - отдельный отзыв со всеми категориями (Достоинства, Недостатки, ...)

def process_task(task_data):
    '''Ключевой метод анализа текстов. Включает препроцессинг, извлечение ключевых фраз, а также генерацию облака слов'''
    review_url = task_data['url']
    review_cnt = task_data['review_cnt']
    try:
        # if parser is None:
        #     raise Exception('не удалось найти свободный парсер')
        
        # Использовать или не использовать специально подготовленный html файл для извлечения отзывов
        # if not Config.USE_PREPARED:
        #     if not parser.successful_open:
        #         parser.open_DNS_site(attempts=Config.BROWSER_CITE_OPENNIG_ATTEMPS) # Исполняется только единожды!!!
        #     reviews = parser.get_product_reviews(review_url, review_cnt, Config.BROWSER_CITE_OPENNIG_ATTEMPS) # propper form: list of dictionaries
        # else:
        reviews = []
        with open(Config.RAW_HTML_PATH, 'r', encoding='utf-8') as file:
            reviews = extract_reviews(file.read()) # propper form: list of dictionaries
        
        logging.warning('Сплитинг отзывов')
        # Отделение от каждого отзыва преимуществ и недостатков
        positive_list, negative_list = tp.split_positive_negative_parts(reviews, Config.ANALYZER_MIN_REVIEW_LEN_TRESHOLD)

        logging.warning('Предобработка отзывов')
        # Предобработка отзывов
        positive_list = tp.preprocess_list_of_texts(positive_list, 
                                                    to_lower=Config.ANALYZER_TO_LOWERCASE, 
                                                    erase_punct=Config.ANALYZER_ERASE_PUNCTUATION)
        
        negative_list = tp.preprocess_list_of_texts(negative_list, 
                                                    to_lower=Config.ANALYZER_TO_LOWERCASE, 
                                                    erase_punct=Config.ANALYZER_ERASE_PUNCTUATION)
        
        # Лемматизация отзывов (не используется, поскольку показывает результаты хуже ,модель не улавливает семантику)
        # positive_list, negative_list = (tp.lemmatize_texts(positive_list, mystem), tp.lemmatize_texts(negative_list, mystem))

        logging.warning('Удаление стоп-слов')
        # Удаление стоп-слов
        positive_list, negative_list = (tp.delete_stop_words(positive_list, Config.ANALYZER_LANGUAGE, Config.ANALYZER_ALLOWED_STOPWORDS),
                                        tp.delete_stop_words(negative_list, Config.ANALYZER_LANGUAGE, Config.ANALYZER_ALLOWED_STOPWORDS))

        logging.warning('Извлечение ключевых фраз')
        # Извлечение ключевых фраз
        positive_raw_keywords_list = text_analyzer.extract_keyphrases(positive_list, keybert_model,
                                                                      Config.ANALYZER_TOP_N_KEYWORDS,
                                                                      Config.ANALYZER_KEYPHRASE_NGRAM_RANGE,
                                                                      Config.ANALYZER_KEYBERT_DIVERSITY)
        negative_raw_keywords_list = text_analyzer.extract_keyphrases(negative_list, keybert_model,
                                                                      Config.ANALYZER_TOP_N_KEYWORDS,
                                                                      Config.ANALYZER_KEYPHRASE_NGRAM_RANGE,
                                                                      Config.ANALYZER_KEYBERT_DIVERSITY)
        
        logging.warning('Преобразование формата')
        # Преобразование в словарь + фильтрация
        positive_keywords_dict = text_analyzer.get_dict_keyphrases(positive_raw_keywords_list, threshold=Config.ANALYZER_CONFIDENCE_TRESHOLD)
        negative_keywords_dict = text_analyzer.get_dict_keyphrases(negative_raw_keywords_list, threshold=Config.ANALYZER_CONFIDENCE_TRESHOLD)

        logging.warning('Генерация облаков слов')
        # Генерация облака слов
        hashed_url = hashlib.sha1(review_url.encode('utf-8')).hexdigest()
        currrent_wordcloud_path = os.path.join(Config.WORDCLOUDS_PATH, hashed_url)
        os.makedirs(currrent_wordcloud_path, exist_ok=True)
        WordCloudGenerator.generate(positive_keywords_dict, os.path.join(currrent_wordcloud_path, 'positive.png'), save_image=True)
        WordCloudGenerator.generate(negative_keywords_dict, os.path.join(currrent_wordcloud_path, 'negative.png'), save_image=True)
        logging.warning('Облака сгенерированы')

    except Exception as ex:
        logging.error(f'Произошла ошибка обработки отзывов {str(ex)}')

if __name__ == '__main__':    
    # Пытаемся подключиться к контейнеру с kafka
    max_retries = 10
    retry_delay = 5
    
    for i in range(max_retries):
        try:
            time.sleep(3)
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='wordcloud_workers')
            
            logging.warning(f'worker_{container_id}: успешное подключение к kafka')
            break
        except Exception as e:
            if i == (max_retries-1):
                logging.error(f'worker_{container_id}: не удалось подключиться к kafka. Ошибка: {str(e)}')
                raise
            logging.error(f'worker_{container_id}: не удалось подключиться к kafka. Попытка {i+1}. Ошибка: {str(e)}')
            time.sleep(retry_delay)
    
    # Обработка сообщений
    try:
        for message in consumer:
            task = message.value
            logging.warning(f"worker {container_id} получил {task}")
            start_time = time.time()
            process_task(task)
            logging.warning(f"worker {container_id} завершил обработку задачи за {time.time() - start_time} секунд")
    except Exception as e:
        logging.error(f"Ошибка при обработке сообщений: {str(e)}")