from imagenet_pkg.constants import *
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


async def _fetch_v2(session, data_in, timeout):
    """
    If success, returns (URL_SUCCESS, data_in, bytes), if failed, returns (URL_FAILED, data_in)
    """
    if isinstance(data_in, str):
        url = data_in
    else:
        url = data_in[0]
    try:
        kwargs = {'url': url}
        if timeout:
            kwargs['timeout'] = timeout
        async with session.get(**kwargs) as response:
            data = await response.read()
            return URL_SUCCESS, data_in, data
    except BaseException as ex:
        return URL_FAILED, data_in


def fetch_with_callback(list_data,
                        callback_on_fetch,
                        callback_on_fail=None,
                        **kwargs):
    """
    :param list_data: string or list of data, where the first element is url
    :param callback_on_fetch:
    :param callback_on_fail:
    :param kwargs:
    :return: None
    """
    timeout = kwargs.get('timeout', None)
    on_fail = kwargs.get('on_fail', URL_ON_FAIL_IGNORE)
    async_limit = kwargs.get('async_limit', MAX_ASYNC_REQUESTS_DEFAULT)

    def limited_as_completed(coros, limit):
        """
        This function is a simple generator, which returns one-by-one coroutine on finish,
        executes "limit" coroutines simultaneously.
        """
        futures = [asyncio.ensure_future(c) for c in coros[:limit]]

        async def first_to_finish():
            done, pending = await asyncio.wait(futures, return_when=asyncio.FIRST_COMPLETED)
            obj = list(done)[0]
            futures.remove(obj)

            return obj.result()

        add_index = limit
        while len(futures) > 0:
            result = first_to_finish()
            if add_index < len(coros):
                futures.append(asyncio.ensure_future(coros[add_index]))
                add_index += 1
            yield result

    async def _get(list_data, async_limit, timeout):
        conn = aiohttp.TCPConnector(limit=1000, limit_per_host=30, family=socket.AF_INET, ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            # defining coroutines
            coros = [_fetch_v2(session, data, timeout) for data in list_data]
            for resp in limited_as_completed(coros, async_limit):
                resp = await resp
                status, response = resp[0], resp[1:]

                if status == URL_SUCCESS:
                    callback_on_fetch(response)
                if status == URL_FAILED:
                    callback_on_fail(response[0])
                    if on_fail == URL_ON_FAIL_RETRY:
                        coros.append(_fetch_v2(session, response[0], timeout))  # where response[0] is data_in

    asyncio.run(_get(list_data, async_limit, timeout))


def get_async(urls_list, timeout=10):
    """
    Fetches data from given urls within specified timeout, and returns result in structure
    (url, bytes), or None if couldn't fetch anything.
    Optionaly, urls_list's element may be an array of two elements (url, id),
    if so, (url, id, bytes) will be returned instead of (url, bytes)
    """

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

    return lst
