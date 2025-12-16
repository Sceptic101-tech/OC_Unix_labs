from kafka import KafkaProducer
from flask import Flask, url_for, render_template, request, redirect
from web_config import Config
import logging
import time
import json
import os
import hashlib
import socket

# ожидаем загрузки kafka
time.sleep(10)

error_codes_dict = {1 : 'Неизвестная ошибка',
                    520 : 'Timeout. Не удалось получить экземпляр парсера.',
                    521 : 'Не удалось вернуть экземпляр парсера в очередь',
                    522 : 'Таймаут'}

app = Flask(__name__, template_folder='templates', static_folder='/app/static/prepared_wordclouds')

# Конфигурация Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'tasks'
container_id = socket.gethostname()


@app.route('/', methods=['GET', 'POST'])
def index():
    """Главная страница - форма ввода URL и количества отзывов."""
    if request.method == 'POST':
        review_url = request.form['review_url']
        review_cnt = int(request.form['review_cnt'])
        logging.warning('Перенаправление на analyzing')
        return redirect(url_for('analyzing', review_url=review_url, review_cnt=review_cnt))
    else:
        return render_template('index.html', min_reviews_value=Config.PARSER_REVIEWS_LB, max_review_value=Config.PARSER_REVIEWS_UB)

@app.route('/analyzing', methods=['GET'])
def analyzing():
    '''Страница перенаправления'''
    logging.warning('analyzing')
    review_url = request.args.get('review_url')
    review_cnt = int(request.args.get('review_cnt'))

    hashed_url = hashlib.sha1(review_url.encode('utf-8')).hexdigest() # Хэшируем url воизбежание появления символов, недопустимых в названии 
    wordcloud_dir = os.path.join(Config.WORDCLOUDS_PATH, hashed_url)
    
    # Проверяем, есть ли уже готовые облака слов
    if os.path.exists(wordcloud_dir):
        positive_path = os.path.join(wordcloud_dir, 'positive.png')
        negative_path = os.path.join(wordcloud_dir, 'negative.png')
        if os.path.exists(positive_path) and os.path.exists(negative_path):
            return redirect(url_for('results', hashed_url=hashed_url))
    
    task_data = {
        'url': review_url,
        'review_cnt': review_cnt}

    logging.warning('Отправка данных в топик')
    try:
        # Отправляем сообщение и ждем подтверждения
        future = producer.send(TOPIC_NAME, value=task_data)
        result = future.get(timeout=10)
        logging.warning(f'Сообщение отправлено в топик {TOPIC_NAME}, партиция {result.partition}, offset {result.offset}')
    except Exception as e:
        logging.error(f'Ошибка отправки в топик kafka: {e}')
        return redirect(url_for('error', code=522))
    
    logging.warning('Ожидание появления файла')
    start_time = time.time()
    # Ожидаем появления файлов
    while (not os.path.exists(wordcloud_dir)) or ((time.time() - start_time) <= Config.ANALYSIS_TIMEOUT):
        if (time.time() - start_time) > Config.ANALYSIS_TIMEOUT:
            logging.error(f'Таймаут ожидания облака слов.')
            return redirect(url_for('error', code=522))
            
        time.sleep(3)
        # Проверяем создались ли оба файла
        positive_path = os.path.join(wordcloud_dir, 'positive.png')
        negative_path = os.path.join(wordcloud_dir, 'negative.png')
        if os.path.exists(positive_path) and os.path.exists(negative_path):
            break
    
    return redirect(url_for('results', hashed_url=hashed_url))

  
@app.route('/results', methods=['GET'])
def results():
   '''Страница результатов'''
   hashed_url = request.args.get('hashed_url')

   return render_template('results.html', positive_img_src = url_for('static', filename=f'{hashed_url}/positive.png'),\
                          negative_img_src = url_for('static', filename=f'{hashed_url}/negative.png'))


@app.route('/error', methods=['GET'])
def error():
    error_code = request.args.get('code')
    if error_code in error_codes_dict:
        return render_template('error.html', error_cause=error_codes_dict[error_code])
    else:
        return render_template('error.html', error_cause=error_codes_dict[1])


if __name__ == "__main__":
    logging.warning('Инициализация producer')

    # Ожидаем загрузки kafka в контейнере
    max_retries = 5
    retry_delay = 5
    for i in range(max_retries):
        start_time = time.time()
        try:
            # Инициализация продюсера kafka
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'), retries=5)
            logging.warning(f'producer_{container_id}: успешное подключение к kafka')
            break
        except:
            if i == (max_retries-1):
                logging.error(f'producer_{container_id}: не удалось подключиться к kafka')
                raise
            logging.error(f'producer_{container_id}: не удалось подключиться к kafka. Попытка {i+1}')
            time.sleep(retry_delay)

    logging.warning('Запуск веба')
    app.run(host='0.0.0.0', port=5000, debug=True)