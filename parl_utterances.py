"""
Functions for scraping parliamentary utterances, extracting relevant data, and storing info in SQL database.
"""

import requests
import time
from zipfile import ZipFile, ZIP_DEFLATED
import random
import os.path


def scrape_parliament_speeches(outdir):
    """
    Scrape archive of Romanian parliament (lower and upper chamber) for speeches, and dump htmls of scraped speeches
    into zip archive. One zip archive for lower chamber of parliament, one for upper. Within each archive, at day level,
    Every one session of talk (e.g. that time chunk when the lower house sits between 10AM and 3PM) is its own file,
    simply marked by yr-mo-day-counter, where default counter = 0.
    :param outdir: directory in which we dump zip archives
    :return: None
    """

    # it looks like the base page which contains a series of parliamentary utterances about a certain topic
    # is this below ; of note that basically all of them increment up to ~8200 (as of September 2020). Sometimes there
    # are gaps, and not all the numbers refer to the same sort of plenary meeting (e.g. combined chambers vs. just
    # senat or CD) but if you scrape all of them and dump them in a archive you can deal with details later.
    # http://www.cdep.ro/pls/steno/steno2015.stenograma?ids=8197&idm=7&idl=1

    # check directory for archive of a certain name
    # if zip archive not there, make it
    zip_archive_path = outdir + "/" + "parl_utterances_raw_htmls.zip"
    if not os.path.exists(zip_archive_path):
        with ZipFile(zip_archive_path, 'w'):
            print("WROTE FRESH ZIP ARCHIVE")

    # if failed-request file isn't there, make it
    failed_requests_path = outdir + "/" + "failed_requests.txt"
    if not os.path.exists(failed_requests_path):
        with open(failed_requests_path, 'w'):
            print("WROTE FRESH FAILED REQUESTS LOG")

    # make header to pass to requests, tell site who I am
    header = {'User-Agent': 'Mozilla/5.0 (Linux Mint 18, 32-bit)'}

    ten_html_queue = {}

    # 8301 is about a hundred more than the max page count as of 04/09/2020
    for page_counter in range(1, 8301):
        print(page_counter)
        url_base = "http://www.cdep.ro/pls/steno/steno2015.stenograma?ids="
        url_tail = str(page_counter) + "&idm=7&idl=1"
        utterances_page_url = url_base + url_tail

        try:
            time.sleep(random.uniform(0, 0.5))
            parl_utterances_page = requests.get(utterances_page_url, headers=header)
            ten_html_queue.update({utterances_page_url: parl_utterances_page})

        # catch three sorts of errors that have previous come up
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout) as e:
            print(e)
            # save the failed pages to a fail log on disk
            with open(failed_requests_path, 'a') as in_f:
                in_f.write(utterances_page_url)
                in_f.write("\n")
            # give it a minute before continuing
            time.sleep(60)

        # every ten pages append the text of the accumulated htmls to the zip archive on disk
        if page_counter % 10 == 0:
            with ZipFile(zip_archive_path, mode='a') as zip_archive:
                for url, html in ten_html_queue.items():
                    zip_archive.writestr(url, html.content, compress_type=ZIP_DEFLATED)
            # then reset the ten html queue dict so we don't make a massive dict in memory
            ten_html_queue = {}
