from queue import Queue, Empty, Full
from scripts.parser import DNS_Shop_Parser

class ParserPool:
    """
    Пул объектов `DNS_Shop_Parser`.

    Parameters
    ----------
    size : int
        Количество парсеров в пуле.
    block : bool
        Если True – при попытке получить/положить элемент из пула, ожидать до timeout.
    timeout : float
        Время ожидания (сек.) для операций `get` и `put`.
    browser_headless : bool
        Флаг, передаваемый в конструктор каждого парсера.
    """
    def __init__(self, size:int, block:bool, timeout:float, browser_headless:bool):
        self._pool = Queue(maxsize=size)
        self._block = block
        self._timeout = timeout
        for _ in range(size):
            parser = DNS_Shop_Parser(browser_headless)
            self._pool.put(parser, self._block, self._timeout)

    def get(self)->DNS_Shop_Parser:
        """
        Получает парсер из пула.

        Returns
        -------
        DNS_Shop_Parser | None
            Возвращает объект парсера или `None`, если очередь пуста.
        """
        try:
           return self._pool.get(self._block, self._timeout)
        except Empty:
            return None


    def put(self, parser):
        """
        Положить парсер обратно в пул.

        Parameters
        ----------
        parser : DNS_Shop_Parser
            Объект, который нужно вернуть в пул.

        Returns
        -------
        bool | None
            `True` – если добавлено успешно, `None` – при переполнении.
        """
        try:
            self._pool.put(parser, self._block, self._timeout)
            return True
        except Full:
            return None
            
