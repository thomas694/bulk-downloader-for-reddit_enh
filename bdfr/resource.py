#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import logging
import re
import time
import urllib.parse
from collections.abc import Callable
from typing import Optional

import _hashlib
import requests
from praw.models import Submission

from bdfr.exceptions import BulkDownloaderException

logger = logging.getLogger(__name__)


class Resource:
    def __init__(self, source_submission: Submission, url: str, download_function: Callable, extension: str = None):
        self.source_submission = source_submission
        self.content: Optional[bytes] = None
        self.url = url
        self.hash: Optional[_hashlib.HASH] = None
        self.extension = extension
        self.download_function = download_function
        if not self.extension:
            self.extension = self._determine_extension()

    def url(self):
        return self.url

    @staticmethod
    def retry_download(url: str) -> Callable:
        return lambda global_params: Resource.http_download(url, global_params)

    def download(self, download_parameters: Optional[dict] = None):
        if download_parameters is None:
            download_parameters = {}
        if not self.content:
            try:
                content = self.download_function(download_parameters)
            except requests.exceptions.ConnectionError as e:
                raise BulkDownloaderException(f"Could not download resource: {e}")
            except BulkDownloaderException:
                raise
            if content:
                self.content = content
        if not self.hash and self.content:
            self.create_hash()

    def create_hash(self):
        self.hash = hashlib.md5(self.content)

    def _determine_extension(self) -> Optional[str]:
        extension_pattern = re.compile(r".*(\..{3,5})$")
        stripped_url = urllib.parse.urlsplit(self.url).path
        match = re.search(extension_pattern, stripped_url)
        if match:
            return match.group(1)

    @staticmethod
    def http_download(url: str, download_parameters: dict) -> Optional[bytes]:
        headers = download_parameters.get("headers")
        fail_fast = download_parameters.get("fail_fast", False)
        current_wait_time = 60 if not fail_fast else 5
        if "max_wait_time" in download_parameters and not fail_fast:
            max_wait_time = download_parameters["max_wait_time"]
        else:
            max_wait_time = 300 if not fail_fast else 10
        while True:
            try:
                response = requests.get(url, headers=headers)
                if re.match(r"^2\d{2}", str(response.status_code)) and response.content:
                    return response.content
                elif response.status_code in (408, 429):
                    raise requests.exceptions.ConnectionError(f"Response code {response.status_code}")
                else:
                    raise BulkDownloaderException(
                        f"Unrecoverable error requesting resource: HTTP Code {response.status_code}"
                    )
            except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
                logger.warning(f"Error occured downloading from {url}, waiting {current_wait_time} seconds: {e}")
                if fail_fast and ("[Errno 11001]" in str(e) or "[WinError 10061]" in str(e)):
                    #getaddrinfo failed #No connection could be made because the target machine actively refused it
                    raise
                time.sleep(current_wait_time)
                if current_wait_time < max_wait_time:
                    current_wait_time += 60 if not fail_fast else 5
                else:
                    logger.error(f"Max wait time exceeded for resource at url {url}")
                    raise
