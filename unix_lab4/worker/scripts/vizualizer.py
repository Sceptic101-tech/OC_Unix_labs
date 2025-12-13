from wordcloud import WordCloud
from config import Config

class WordCloudGenerator:
    """
    Генератор облаков слов.

    Параметры визуализации берутся из модуля `config`.
    """
    def __init__(self):
        pass

    @classmethod
    def generate(cls, freq_dict: dict[str, float], filepath, save_image=True):
        """
        Создаёт облако слов и сохраняет его в файл.

        Parameters
        ----------
        freq_dict : dict[str, float]
            Словарь с частотами ключевых слов.
        filepath : str
            Путь к файлу, куда будет сохранено изображение.
        save_image : bool, optional
            Если True – сохранить картинку в `filepath`, иначе вернуть объект Image.

        Returns
        -------
        PIL.Image.Image
            Созданное облако слов (изображение).
        """
        wc = WordCloud(
            width=Config.WORDCLOUD_WIDTH,
            height=Config.WORDCLOUD_HEIGHT,
            background_color=Config.WORDCLOUD_BACKGROUND_COLOR,
            colormap=Config.WORDCLOUD_COLORMAP,
            collocations=Config.WORDCLOUD_COLLOCATIONS,
            random_state=Config.WORDCLOUD_RANDOM_STATE,
        )
        wc.generate_from_frequencies(freq_dict)
        image = wc.to_image()
        if save_image:
            image.save(filepath)
        return image

