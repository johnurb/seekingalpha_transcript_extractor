#!/usr/bin/env python3

# we need these
import requests
from bs4 import BeautifulSoup
from time import sleep
import random
import os
from get_agents import get_agent_list
from load_urls import load_urls
import browser_cookie3
import sys


# function to save an individual transcript page
def save_scrapes(url, title, content):
    # create directory to hold scraped transcript html files if not already present
    subdirectory = 'scrapes'
    try:
        os.mkdir(subdirectory)
    except Exception:
        pass

    # set the filename for a specific scrape; title of the transcript
    filename = title + '.html'
    filename = filename.replace(' ', '')
    path = os.path.join(subdirectory, filename)

    # write the page body to the file; page body is the scraped html
    url_string = '<!-- {} -->'.format(url)
    with open(path, 'w') as f:
        f.write(url_string + '\n')
        f.write(content)

# main function (obviously)
def main():
    urls = load_urls() # load in the urls to scrape
    agents = get_agent_list() # bring in list of user-agents for the page requests
    
    # create a new HTTP requests 'browsing' session; for cookie persistence
    session = requests.Session()
    # set the session headers; these are sent in the HTTP request so hopefully we don't get locked out
    session.headers = {
        'User-Agent': random.choice(agents),
        'X-Forwarded-For':'1.1.1.1',
        'Referer': random.choice(urls),
        '_px2': 'True',
        '_px': 'True',
        'PerimeterX': 'True',
        'Content-Encoding':'gzip',
        'Strict-Transport-Security':'max-age=15552000; preload',
        'X-Frame-Options':'DENY',
        'X-Content-Type-Options':'nosniff',
        'Connection':'keep-alive',
        'Transfer-Encoding':'chunked',
    }
    
    # SeekingAlpha restricts most transcript pages from being viewed in single page/entirely to only subscribed accounts
    # pull in the cookies from Google Chrome
    # make sure user is logged in on the SeekingAlpha website so there is an account authorization cookie
    # set the session cookies
    cookie = browser_cookie3.chrome()
    session.cookies.update(cookie)

    # set a counter for the number of pages successfully scraped & downloaded in the 'scrapes' folder
    # set a counter for the number of failures. Program will terminate after too many.
    successfully_scraped = 0
    failures = 0
    for url in urls:
        if failures >= 5:
            session.close()
            print('Blocked out. Terminating Program\n')
            sys.exit()
        try:
            # send a request to a transcript page
            r = session.get(url, timeout=5)
            r.raise_for_status()
            
            # create a BeautifulSoup object from the returned response
            soup = BeautifulSoup(r.text, 'html.parser')
            # if the page includes an 'Analysts' text element we know (or assume) it returned properly
            find_text = soup.body.find(text='Analysts')
            
            # set a flag for routing
            correct_data = True
            # if the page did not return properly the following executres
            if not find_text:
                correct_data = False
                # if the HTTP request was successful but the returned content wasn't what we needed the following will execute
                if r.status_code == 200:
                    # open the text file containing successfully scraped pages and update with the new one
                    # the page was successfully scraped (HTTP request returned properly) we just didn't get the desired content
                    successful_urls = open('scraped_urls.txt', 'a')
                    successful_urls.write(url + '\n')
                    successful_urls.close()
                    successfully_scraped += 1
                    print('Scraped Pages: {}'.format(successfully_scraped))
        
            # if the HTTP request was successful and the returned content was what we needed (or assumed to be) the following will execute
            if correct_data:
                # get the article title; used for the file name
                article_title = soup.find('h1', attrs={'itemprop':'headline'}).text.lower().replace('earnings call transcript','').replace('–', '').replace('-', '').replace('/','_').replace(',','').strip()

                # save successfully scraped html pages
                save_scrapes(url, article_title, r.text)
    
                # open the text file containing successfully scraped pages and update with the new one
                # the page was successfully scraped (HTTP request returned properly) and we got our desired (or assumed) content
                successful_urls = open('scraped_urls.txt', 'a')
                successful_urls.write(url + '\n')
                successful_urls.close()
    
                # update the counter of successfully scraped urls that were saved in the 'scrapes' folder
                successfully_scraped += 1
                print('Scraped Pages: {}'.format(successfully_scraped))

        # something went wrong, we're going to keep trying
        except requests.ConnectionError as err:
            print(err)
        # page request timed out, we're going to keep trying
        except requests.Timeout:
            print('Timeout Error')

        # we got blocked, update the counter of failures, we'll try again but only a couple times with the same problem
        except requests.RequestException:
            print('Requests Error: {}'.format(r.status_code))
            failures += 1

        # broad failure, try again
        except Exception as err:
            print(err)
           
        # we have to have a delay in the scraping because to view the pages we have to be logged in, which makes it easy for them to see we're scraping. Proxies don't work. 
        sleep(random.randint(60,90))
    
main()
