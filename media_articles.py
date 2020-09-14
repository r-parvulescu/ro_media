"""
Scrape media sites for article-level data and dump said data into a zip archive, with the following structure:
year
    month
        day
            data on articles for that day
"""

import requests
import time
from bs4 import BeautifulSoup
from zipfile import ZipFile, ZIP_DEFLATED
from datetime import datetime
from local import root
import json
import random
import os.path


# MEDIA SCRAPER FUNCTIONS #


def scrape_hotnews(outdir):
    """
    Scrape archives of news webiste "hotnews.ro" and dump htmls of scraped articles into zip archive.
    Within day no further organisation; article file names are yr-mo-day-timestamp-counter. If two articles have the
    same time-stamp increment counter by one, otherwise article has counter = 0

    :param outdir: directory in which we dump zip archive
    :return: None
    """
    # set up the time units over which we'll be iterating
    years = [str(i) for i in range(2004, 2021)]
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    one_nine = ['01', '02', '03', '04', '05', '06', '07', '08', '09']
    twenty_eight, twenty_nine = one_nine + [str(i) for i in range(10, 29)], one_nine + [str(i) for i in range(10, 30)]
    thirty, thirty_one = one_nine + [str(i) for i in range(10, 31)], one_nine + [str(i) for i in range(10, 31)]

    # start date
    start_date = '2015-02-06'

    # check directory for archive of a certain name
    # if zip archive not there, make it
    zip_archive_path = outdir + "/" + "hotnews_articles.zip"
    if not os.path.exists(zip_archive_path):
        with ZipFile(zip_archive_path, 'w'):
            print("WROTE FRESH ZIP ARCHIVE")

    # if failed-request file isn't there, make it
    failed_page_requests_path = outdir + "/" + "hotnews_failed_page_requests.txt"
    if not os.path.exists(failed_page_requests_path):
        with open(failed_page_requests_path, 'w'):
            print("WROTE FRESH FAILED PAGE REQUESTS LOG")

    # if failed-request file isn't there, make it
    failed_article_requests_path = outdir + "/" + "hotnews_failed_article_requests.txt"
    if not os.path.exists(failed_article_requests_path):
        with open(failed_article_requests_path, 'w'):
            print("WROTE FRESH FAILED ARTICLE REQUESTS LOG")

    url_base = "https://www.hotnews.ro/arhiva/"

    for yr in years:
        leap_year = True if yr in {'2004', '2008', '2012', '2016', '2020'} else False
        for mo in months:

            if mo in {'01', '03', '05', '07', '09', '11'}:
                days = thirty_one
            elif mo in {'04', '06', '08', '10', '12'}:
                days = thirty
            else:
                days = twenty_nine if leap_year else twenty_eight

            for d in days:
                day_queue = {}

                day_page_stem = '-'.join([yr, mo, d])
                day_page_url = url_base + day_page_stem

                # get articles after start date and before today
                if datetime.strptime(start_date, "%Y-%m-%d") < datetime.strptime(day_page_stem, "%Y-%m-%d") < \
                        datetime.today():

                    # show us what day we're on
                    print(day_page_url)

                    # get the urls of the articles we want to scrape
                    article_urls = get_article_urls(day_page_url, failed_page_requests_path)

                    if article_urls:  # avoid failures to get the article urls, which generate a None value

                        # scrape html of each article and add to the day's article queue
                        for idx, art_url in enumerate(article_urls):
                            # path where the article info will live in the zip archive
                            file_path = '/'.join([yr, mo, d]) + '/' + '-'.join([yr, mo, d]) + '_' + str(idx) + '.txt'
                            # get the data and add it to the day queue dict
                            day_queue.update({file_path: scrape_article_data(art_url, file_path,
                                                                             failed_article_requests_path)})

                        # at the end of every day, update the archive by appending day queue contents to archive
                        with ZipFile(zip_archive_path, mode='a') as zip_archive:
                            for file_path, data_dict in day_queue.items():
                                zip_archive.writestr(file_path, json.dumps(data_dict), compress_type=ZIP_DEFLATED)


def get_article_urls(day_url, failed_page_requests_path):
    """
    Return a list of all urls leading to articles.

    :param day_url: str, url indicating the day we're scraping
    :param failed_page_requests_path: path to .txt file where we dump links of article that requests couldn't ping
    :return: list of urls, each pointing to an individual article
    """
    # make header to pass to requests, tell site who I am
    header = {'User-Agent': 'Mozilla/5.0 (Linux Mint 18, 32-bit)'}

    article_urls = []

    # need to ping the default day site to see how many pages are available on that day
    # Days with 20+ articles will have multiple pages listing articles. This chunk gets the pagination
    try:
        pre_list_page = requests.get(day_url, headers=header)
    except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ReadTimeout) as e:
        print(e)
        with open(failed_page_requests_path, 'a') as in_f:
            in_f.write(day_url), in_f.write("\n")
        return

    pre_soup = BeautifulSoup(pre_list_page.text, 'html.parser')
    pages = pre_soup.find('div', class_='paginare').text.split()

    # now iterate through that day's pages and extract the htmls pointing to individual articles
    for p in pages:
        try:
            time.sleep(random.uniform(0, 0.5))
            list_page = requests.get(day_url + '/' + p, headers=header)
            update_url_list(list_page.text, article_urls)

        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout) as e:
            print(e)
            with open(failed_page_requests_path, 'a') as in_f:
                in_f.write(day_url + '/' + p), in_f.write("\n")
            time.sleep(60)  # give it a minute before continuing

    return article_urls


def update_url_list(list_page_text, article_url_list):
    """
    Pings the hotnews.ro website and update the list of urls, where each urls points to a different article.

    :param list_page_text: html from the list page which has been text-ified by BeautifulSoup
    :param article_url_list: list of urls pointing to specific articles
    :return: None
    """
    soup = BeautifulSoup(list_page_text, 'html.parser')
    if 'Pagina Ceruta nu exista' not in list_page_text:  # avoids errors when the page doesn't actually exist
        for link in soup.find_all('a', class_="result_title"):
            article_url_list.append(link.get('href'))


def scrape_article_data(url, file_path, failed_article_requests_path):
    """
    Pull desired data from site an.

    :param url: str, url of site
    :param file_path: path the file will have in the zip archive
    :param failed_article_requests_path: path to .txt file where we dump article links that requests couldn't ping
    """
    print(file_path, '----------', datetime.now())

    # recall date is in filepath, which is of form e.g. "2004/06/23/2004-06-23_34.txt"
    date = file_path.split('/')[3].split('.')[0].split('_')[0]

    # make header to pass to requests, tell site who I am
    header = {'User-Agent': 'Mozilla/5.0 (Linux Mint 18, 32-bit)'}

    # leave a little, random gap for courtesy
    rand_gap = random.uniform(0, 0.1)
    time.sleep(rand_gap)

    try:  # try downloading the file
        html = requests.get(url, headers=header)
        return extract_article_info(html, date)

    except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ReadTimeout) as e:
        print(e)
        with open(failed_article_requests_path, 'a') as in_f:
            in_f.write(url), in_f.write("\n")
            time.sleep(60)  # wait a minute before moving on
        return


def extract_article_info(hotnews_article_html, date):
    """
    Extract desired data from html of hotnews.ro article and dump data into dict. Desired data are year-month-day,
    article source (sometimes this news source directly circulates stories from other outlets / newswires), author,
    topic, title, and the article text itself.

    :param hotnews_article_html: str, html of hotnews article web page
    :param date: str, date in format YEAR-MONTH-DATE
    :return: dict with desired data
    """
    dates = date.split('-')

    # soup the html
    soup = BeautifulSoup(hotnews_article_html.text, 'html.parser')

    return {'year': dates[0], 'month': dates[1], 'day': dates[2], 'source': get_hotnews_article_source(soup),
            'author': get_hotnews_article_author(soup), 'topic': get_hotnews_article_topic(soup),
            'title': get_hotnews_article_title(soup), 'text_body': get_hotnews_article_text(soup)}


def get_hotnews_article_author(soup):
    """
    Get article author of a hotnews article. If none can be found, return None

    NB: up to 2009-01-20 (inclusive) there is patchy information on author name, because of a bug

    :param soup: a BeautifulSoup object
    :return: author name as str, or None
    """
    author_source = soup.find('div', class_='autor')

    if author_source is not None:
        if author_source.contents is not None:
            for auth_sour in author_source.contents:
                # author names are either in format "Surname GivenName" or "SN.GN.", where SN & GN are intitial
                # the other things you can find are empty space and "HotNews.ro" ; ignore these and you get the names
                if "bs4.element.Tag" in str(type(auth_sour)):
                    if auth_sour.text != "HotNews.ro" and not auth_sour.text.isspace():
                        print(auth_sour.text)
                        return auth_sour.text
            else:  # if the loop ends without finding a name, just return None
                return None
        else:
            return None
    else:
        return None


def get_hotnews_article_title(soup):
    """
    Get the title of a hotnews article, if there is one. If not, return None
    :param soup: a BeautifulSoup object
    :return: article title as str, or None
    """
    # title string of form "TITLE - SOURCE - HotNews.ro"
    if soup.title is not None:
        if soup.title.string is not None:
            return soup.title.string.split(' - ')[0]
        else:
            return None
    else:
        return None


def get_hotnews_article_source(soup):
    """
    Get the source (e.g. "hotnews.ro") of an article on the hotnews website. If no source is provided, return None.
    :param soup: a BeautifulSoup object
    :return: source as str, or None
    """
    # I extract source from its own class since sometimes author class is empty but source is not
    if soup.find('span', class_='sursa') is not None:
        return soup.find('span', class_='sursa').text
    else:
        return None


def get_hotnews_article_text(soup):
    """
    Get the text body (i.e. the actual article) from an article on the hotnews website. If none provided, return None.
    :param soup: a BeautifulSoup object
    :return: article text as str, or None
    """
    # text body sometimes includes photo data and bangs together paragraphs; I ignore these problems for now
    if soup.find(id='articleContent') is not None:
        return soup.find(id='articleContent').text
    else:
        return None


def get_hotnews_article_topic(soup):
    """
    Get the topic (e.g. "politics", "sports") from an article on the hotnews website. If none provided, return None.
    :param soup: a BeautifulSoup object
    :return: article topic as str, or None
    """
    # NB: topics only introduced in latter article
    if soup.find('a', class_="atual") is not None:
        return soup.find('a', class_="atual").text
    else:
        return None


if __name__ == '__main__':
    scrape_hotnews(root + 'data/discourse/media')
