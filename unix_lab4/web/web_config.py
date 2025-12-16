import os

class Config:
    # Пути
    WORDCLOUDS_PATH = os.path.join('/app/static/prepared_wordclouds')

    PARSER_REVIEWS_LB = 10 # min желаемого количества отзывов
    PARSER_REVIEWS_UB = 400 # max желаемого количества отзывов

    ANALYSIS_TIMEOUT = 80 # Таймаут ожидания анализа отзвов