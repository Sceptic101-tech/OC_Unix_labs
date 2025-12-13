def extract_keyphrases(
    corpus_list: list[str], model, top_n: int, keyphrase_ngram_range: tuple[int, int] | None = None, diversity: float = 0.7) -> list:
    """
    Извлекает ключевые N‑граммы из списка текстов.

    Parameters
    ----------
    corpus_list : list[str]
        Список строк (тексты), из которых нужно извлечь ключевые фразы.
    model : KeyBERT
        Экземпляр модели KeyBERT, используемый для поиска.
    top_n : int
        Количество ключевых фраз, возвращаемых для каждого текста.
    keyphrase_ngram_range : tuple[int, int] | None, optional
        Диапазон N‑грамм, которые можно извлекать).
    diversity : float, optional
        Параметр `diversity` для функции `use_mmr=True`. Чем выше значение,
        тем более разнообразными будут ключевые фразы.

    Returns
    -------
    list
        Список списков кортежей вида `(ключевая_фраза, score)`,
        где `score` – косинусное сходство с исходным текстом.
    """
    raw_keywords_list = []
    for corpus in corpus_list:
        raw_keywords_list.append(
            model.extract_keywords(
                docs=corpus,
                keyphrase_ngram_range=keyphrase_ngram_range or (1, 3),
                stop_words=None,
                top_n=top_n,
                diversity=diversity,
                use_mmr=True
            )
        )
    return raw_keywords_list


def get_dict_keyphrases(keywords_lists: list[tuple[str, float]], threshold: float) -> dict:
    """
    Преобразует список ключевых фраз в словарь с учётом порога качества.

    Parameters
    ----------
    keywords_lists : list[tuple[str, float]]
        Список списков кортежей вида `(ключевая_фраза, score)`,
        полученных функцией `extract_keyphrases`.
    threshold : float
        Минимальный порог для значения `score`. Фразы с меньшим значением
        будут отбрасываться.

    Returns
    -------
    dict
        Словарь вида `{ключевая_фраза: score}`, содержащий только те фразы,
        где `score` >= `threshold`.
    """
    proper_keywords = {}
    for keyword_list in keywords_lists:
        for keyword, score in keyword_list:
            if score >= threshold:
                proper_keywords[keyword] = score
    return proper_keywords
