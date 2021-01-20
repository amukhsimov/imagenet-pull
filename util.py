from constants import *
import requests
import urllib3
import time
import asyncio
import aiohttp
import socket


def get(url, max_retries=MAX_RECONNECT_ATTEMPTS, timeout=10):
    response = False
    current_attempt = 0
    result = ''
    while not response:
        # if retry count exceeded, return empty string
        if current_attempt > max_retries:
            break
        current_attempt = current_attempt + 1
        try:
            result = requests.get(url, timeout=timeout)
            response = True
        except urllib3.exceptions.MaxRetryError as ex:
            time.sleep(4)
            continue
        except:
            continue
    return result


def get_async(urls_list, timeout=10):
    """
    Fetches data from given urls within specified timeout, and returns result in structure
    (url, bytes), or None if couldn't fetch anything.
    Optionaly, urls_list's element may be an array of two elements (url, id),
    if so, (url, id, bytes) will be returned instead of (url, bytes)
    """

    async def _fetch(session, url, timeout):
        if isinstance(url, str):
            url_id = None
        else:
            url, url_id = url[0], url[1]
        try:
            timeout = aiohttp.ClientTimeout(total=timeout)
            async with session.get(url, timeout=timeout) as response:
                data = await response.read()
                if url_id:
                    return url, url_id, data
                else:
                    return url, data
        except BaseException as ex:
            if url_id:
                return url, url_id
            return (url,)

    async def _get(urls, timeout):
        connector = aiohttp.TCPConnector(limit=1000, limit_per_host=50, family=socket.AF_INET, verify_ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # results = await asyncio.gather(*[_fetch(session, url, timeout) for url in urls])
            # return results
            tasks = [asyncio.create_task(_fetch(session, url, timeout)) for url in urls]
            return await asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    lst = loop.run_until_complete(_get(urls_list, timeout))
    # lst = asyncio.run(_get(urls_list, timeout))
    # loop = asyncio.get_event_loop()
    # lst = loop.run_until_complete(_get(urls_list, timeout))

    return lst
