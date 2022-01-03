"""
Subclass BGGClient to add support for BGG threads.
"""

from copy import copy
import datetime
from boardgamegeek import BGGClient
from boardgamegeek.exceptions import BGGItemNotFoundError, BGGValueError
from boardgamegeek.utils import DictObject, request_and_parse_xml


class Article(DictObject):  # pylint: disable=too-few-public-methods
    """
    A BoardGameGeek forum article, i.e. a single post in a thread.

    :param dict data: a dictionary containing the article data
    """

    def __init__(self, data):
        data_copy = copy(data)

        for date in ("postdate", "editdate"):
            if date in data_copy:
                if not isinstance(data_copy[date], datetime.datetime):
                    try:
                        data_copy[date] = datetime.datetime.fromisoformat(
                            data_copy[date]
                        )
                    except ValueError:
                        data_copy[date] = None

        super().__init__(data_copy)


class Thread(DictObject):
    """
    A BoardGameGeek forum thread.

    :param dict data: a dictionary containing the thread header data
    """

    def __init__(self, data):
        self._articles = []
        super().__init__(copy(data))

    def __getitem__(self, item):
        return self._articles.__getitem__(item)

    def __len__(self):
        return len(self._articles)

    def add_article(self, data):
        """Add article data to the thread."""
        self._articles.append(Article(data))

    @property
    def articles(self):
        """
        :return: articles
        :rtype: list of :py:class:`ThreadArticle`
        """
        return self._articles


def create_thread_from_xml(xml_root):
    """Helper function to create a Thread object from XML input."""

    if "link" not in xml_root.attrib:
        raise BGGItemNotFoundError("link not found")

    return Thread({"id": int(xml_root.attrib["id"]), "link": xml_root.attrib["link"]})


def add_articles_from_xml(thread, xml_root):
    """Helper function to create Article objects from XML input and add them to a Thread object."""
    added_items = False

    for item in xml_root.find("articles").findall("article"):
        data = {
            "id": int(item.attrib["id"]),
            "username": item.attrib["username"],
            "link": item.attrib["link"],
            "postdate": item.attrib["postdate"],
            "editdate": item.attrib["editdate"],
            "numedits": int(item.attrib["numedits"]),
        }
        thread.add_article(data)
        added_items = True

    return added_items


class BGGClientWithThreadSupport(BGGClient):
    """
    Python client for www.boardgamegeek.com's XML API 2.

    Caching for the requests can be used by specifying an URI for the ``cache`` parameter. By
    default, an in-memory cache is used, with sqlite being the other currently supported option.
        :param :py:class:`boardgamegeek.cache.CacheBackend` cache: An object to be used for caching
        :param float timeout: Timeout for network operations, in seconds
        :param int retries: Number of retries to perform in case the API returns HTTP 202
        :param float retry_delay: Time to sleep, in seconds, between retries on HTTP 202
        :param disable_ssl: ignored, left for backwards compatibility
        :param requests_per_minute: how many requests per minute to allow to go out to BGG
        Example usage::
            >>> bgg = BGGClient()
            >>> game = bgg.game("Android: Netrunner")
            >>> game.id
            124742
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._thread_api_url = "https://www.boardgamegeek.com/xmlapi2/thread"

    def thread(self, thread_id):
        """
        Retrieves details about a thread

        :param integer thread_id: the id number of the thread
        :return: ``Thread`` object containing the data
        :return: ``None`` if the information couldn't be retrieved
        :rtype: :py:class:`Thread`
        :raises: :py:exc:`BGGValueError` in case of an invalid parameter(s)
        :raises: :py:exc:`boardgamegeek.exceptions.BGGApiRetryError` if user should retry later
        :raises: :py:exc:`boardgamegeek.exceptions.BGGApiError` if the response couldn't be parsed
        :raises: :py:exc:`boardgamegeek.exceptions.BGGApiTimeoutError` if there was a timeout
        """
        try:
            thread_id = int(thread_id)
        except Exception as error:
            raise BGGValueError from error

        xml_root = request_and_parse_xml(
            self.requests_session,
            self._thread_api_url,
            params={"id": thread_id},
            timeout=self._timeout,
            retries=self._retries,
            retry_delay=self._retry_delay,
        )
        thread = create_thread_from_xml(xml_root)
        add_articles_from_xml(thread, xml_root)
        return thread
