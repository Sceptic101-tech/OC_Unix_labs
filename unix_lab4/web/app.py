from kafka import KafkaProducer
from flask import Flask, url_for, render_template, request, redirect
from config import Config
import logging
import time
import json
import os

error_codes_dict = {1 : 'Неизвестная ошибка',
                    520 : 'Timeout. Не удалось получить экземпляр парсера.',
                    521 : 'Не удалось вернуть экземпляр парсера в очередь',
                    522 : 'Таймаут'}

app = Flask(__name__, template_folder='templates')

# Ожидаем загрузки контейнера с kafka
time.sleep(15)

# Конфигурация Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPIC_NAME = 'tasks'

# Инициализация продюсера Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5)


@app.route('/', methods=['GET', 'POST'])
def index():
    """Главная страница - форма ввода URL и количества отзывов."""
    if request.method == 'POST':
        review_url = request.form['review_url']
        review_cnt = int(request.form['review_cnt'])
        return redirect(url_for('analyzing', review_url=review_url, review_cnt=review_cnt))
    else:
        return render_template(
            'index.html',
            min_reviews_value=Config.PARSER_REVIEWS_LB,
            max_review_value=Config.PARSER_REVIEWS_UB)

@app.route('/analyzing', methods=['GET'])
def analyzing():
    review_url = request.args.get('review_url')
    review_cnt = int(request.args.get('review_cnt'))
    if os.path.exists(os.path.join(Config.WORDCLOUDS_PATH, review_url)):
        return redirect(url_for('results'))
    
    task_data = {
        'url': review_url,
        'review_cnt' : review_cnt}

    producer.send(TOPIC_NAME, task_data)
    producer.flush()

    start_time = time.time()
    # Ожидаем появления файла
    while not os.path.exists(os.path.join(Config.WORDCLOUDS_PATH, review_url)):
        if (time.time() - start_time) > Config.ANALYSIS_TIMEOUT:
            logging.error(f'Таймаут ожидания облака слов. Прошло {Config.ANALYSIS_TIMEOUT} секунд')
            return redirect(url_for('error'), 522)
        time.sleep(1)
    return redirect(url_for('results', review_url=review_url))

  
@app.route('/results', methods=['GET'])
def results():
   url = request.get('review_url')
   return render_template('results.html',
                                   positive_img_src=os.path.join(Config.WORDCLOUDS_PATH, url, 'positive.png'),
                                   negative_img_src=os.path.join(Config.WORDCLOUDS_PATH, url, 'negative.png'))

@app.route('/error', methods=['GET'])
def error():
    error_code = request.args.get('code')
    if error_code in error_codes_dict:
        return render_template('error.html', error_cause=error_codes_dict[error_code])
    else:
        return render_template('error.html', error_cause=error_codes_dict[1])

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=42960, debug=False)
    # app.run(debug=True)
