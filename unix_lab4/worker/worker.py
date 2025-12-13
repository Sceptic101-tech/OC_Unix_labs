import json
import os
import socket
import time
import logging
from keybert import KeyBERT
from pymystem3 import Mystem
# from scripts.parser_pool import ParserPool
from scripts.parser import extract_reviews
import scripts.text_processing as tp
import scripts.analyzer as text_analyzer
from scripts.vizualizer import WordCloudGenerator
from kafka import KafkaConsumer
from config import Config
import nltk
import hashlib

# Конфигурация Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPIC_NAME = 'tasks'
GROUP_ID = 'worker-group'

keybert_model = KeyBERT(Config.ANALYZER_KEYBERT_MODEL_NAME)
# parser_pool = ParserPool(size=Config.PARSER_POOL_SIZE, block=Config.PARSER_POOL_BLOCK,
#                          timeout=Config.PARSER_POOL_TIMEOUT, browser_headless=Config.BROWSER_HEADLESS)
# parser = parser_pool.get()
mystem = Mystem()
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('punkt_tab')

# Получаем имя хост машины для идентификации воркера
worker_id = socket.gethostname()

def process_task(task_data):
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
        
        # Отделение от каждого отзыва преимуществ и недостатков
        positive_list, negative_list = tp.split_positive_negative_parts(reviews, Config.ANALYZER_MIN_REVIEW_LEN_TRESHOLD)

        # Предобработка отзывов
        positive_list = tp.preprocess_list_of_texts(positive_list, 
                                                    to_lower=Config.ANALYZER_TO_LOWERCASE, 
                                                    erase_punct=Config.ANALYZER_ERASE_PUNCTUATION)
        
        negative_list = tp.preprocess_list_of_texts(negative_list, 
                                                    to_lower=Config.ANALYZER_TO_LOWERCASE, 
                                                    erase_punct=Config.ANALYZER_ERASE_PUNCTUATION)
        
        # Лемматизация отзывов (не используется, поскольку показывает результаты хуже)
        # positive_list, negative_list = (tp.lemmatize_texts(positive_list, mystem), tp.lemmatize_texts(negative_list, mystem))

        # Удаление слов 
        positive_list, negative_list = (tp.delete_stop_words(positive_list, Config.ANALYZER_LANGUAGE, Config.ANALYZER_ALLOWED_STOPWORDS),
                                        tp.delete_stop_words(negative_list, Config.ANALYZER_LANGUAGE, Config.ANALYZER_ALLOWED_STOPWORDS))

        # Извлечение ключевых фраз
        positive_raw_keywords_list = text_analyzer.extract_keyphrases(positive_list, keybert_model,
                                                                      Config.ANALYZER_TOP_N_KEYWORDS,
                                                                      Config.ANALYZER_KEYPHRASE_NGRAM_RANGE,
                                                                      Config.ANALYZER_KEYBERT_DIVERSITY)
        negative_raw_keywords_list = text_analyzer.extract_keyphrases(negative_list, keybert_model,
                                                                      Config.ANALYZER_TOP_N_KEYWORDS,
                                                                      Config.ANALYZER_KEYPHRASE_NGRAM_RANGE,
                                                                      Config.ANALYZER_KEYBERT_DIVERSITY)
        
        # Преобразование в словарь + уверенность + фильтрация
        positive_keywords_dict = text_analyzer.get_dict_keyphrases(positive_raw_keywords_list, threshold=Config.ANALYZER_CONFIDENCE_TRESHOLD)
        negative_keywords_dict = text_analyzer.get_dict_keyphrases(negative_raw_keywords_list, threshold=Config.ANALYZER_CONFIDENCE_TRESHOLD)

        # Генерация облака слов
        hashed_url = hashlib.sha1(review_url.encode('utf-8')).hexdigest()
        currrent_wordcloud_path = os.path.join(Config.WORDCLOUDS_PATH, hashed_url)
        os.makedirs(currrent_wordcloud_path, exist_ok=True)
        WordCloudGenerator.generate(positive_keywords_dict, os.path.join(currrent_wordcloud_path, 'positive.png'), save_image=True)
        WordCloudGenerator.generate(negative_keywords_dict, os.path.join(currrent_wordcloud_path, 'negative.png'), save_image=True)

    except Exception as ex:
        logging.error(f'Произошла ошибка {str(ex)}')
    
    # finally:
    #     if parser_pool.put(parser) is None:
    #         raise Exception('Не удалось вернуть парсер в пул')

# Ожидаем загрузки контейнера с kafka
max_retries = 5
retry_delay = 5
for i in range(1, max_retries):
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True)
        break
    except Exception as e:
        if i == max_retries:
            raise
        time.sleep(retry_delay)
    
# Обработка сообщений
for message in consumer:
    task = message.value
    logging.info(f"воркер {worker_id} получил {task}")
    process_task(task)
