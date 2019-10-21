import os

def load_urls():
    urls_to_scrape = []
    
    if os.path.exists('scraped_urls.txt'):
        # read in the scraped urls
        scraped_urls_file = open('scraped_urls.txt', 'r')
        scraped_urls = scraped_urls_file.readlines()
        scraped_urls_file.close()
        
        # read in the total url list
        all_urls_file = open('transcript_urls.txt', 'r')
        all_urls = all_urls_file.readlines()
        all_urls_file.close()
        
        # get list of currently unscraped urls
        unscraped_urls = [url for url in all_urls if url not in scraped_urls]
        
        urls_to_scrape = unscraped_urls
    else:
        url_file = open('transcript_urls.txt', 'r')
        urls = url_file.readlines()
        url_file.close()
        
        urls_to_scrape = urls
    
    urls_to_scrape = [url.replace('\n', '') for url in urls_to_scrape]

    print('Loaded Urls\n')
    #print(urls_to_scrape)
    return urls_to_scrape
