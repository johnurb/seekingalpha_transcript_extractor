import os
from bs4 import BeautifulSoup
import concurrent.futures
from analyst_class import Analyst
from executive_class import Executive
import linecache
import sys
import csv
from time import sleep


def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))


def process_saved_page(item, path):  # function to get the title, date, and company info of a transcript article
    try:
        local_url_path = os.path.join(path, item)
        f = open(local_url_path, 'r')
        content = f.read()
        f.close()
        soup = BeautifulSoup(content, 'html.parser')

        # get the title of the transcript article
        try:
            title = soup.find('h1', attrs={'itemprop': 'headline'}).text.lower().replace('earnings call transcript',
                                                                                         '').replace('–', '').replace(
                '-', '').strip()
        except Exception:
            title = 'Unknown Call'

        try:
            # get the time of the transcript article
            time_container = soup.find('div', class_='a-info clearfix')
            date = time_container.find('time')['content'].split('T')[0]
        except Exception:
            date = 'nonefound'

        # get the company name
        try:
            company_name = soup.find('div', attrs={'itemprop': 'articleBody'}).p.text.split('(')[0]
        except Exception:
            company_name = 'nonefound'

        try:
            # c = current
            c_ticker = soup.find('span', attrs={'id': 'about_primary_stocks'}).a['href'].split('/symbol/')[1]
        except Exception:
            c_ticker = 'undefined'

        # get quarter & year
        try:
            q_a_y = soup.find('div', attrs={'itemprop': 'articleBody'}).p.text.split(')')[1].strip()
            q_a_y_text = q_a_y.split()
            quarter = q_a_y_text[0]
            year = q_a_y_text[1]
        except Exception:
            quarter = 'undefined'
            year = 'undefined'

        # ensure all meta-data is lowercase & remove beginning/ending whitespace
        title = title.lower().strip()
        date = date.lower().strip()
        company_name = company_name.lower().strip()

        c_ticker = c_ticker.lower().strip()
        quarter = quarter.lower().strip()
        year = year.lower().strip()

        # consolidate the transcript article info
        info = {
            'title': title,
            'date': date,
            'company_name': company_name,
            'quarter': quarter,
            'year': year,
            'ticker': c_ticker
        }

        # get filler content for the output file name string for each individual's transcribed text
        quarter = info['quarter']
        year = info['year']
        ticker = info['ticker']
        outfile = '{ticker}_{quarter}_{year}'.format(ticker=ticker, quarter=quarter, year=year)

        try:
            sub1 = 'output_files'
            dir_path = os.path.join(sub1, outfile)
            os.makedirs(dir_path)
        except Exception:
            pass

        # get body content
        # get paragraphs
        content = soup.find('div', class_='sa-art article-width')

        # get list of executives
        # get the next <p> tag; <p> tags are either transcript content, name specifier, or a name
        try:
            para = content.find('p')

            # iterate through the <p> elements until we reach the list of executives on the call
            reached_executives = False
            while not reached_executives:
                if para.text.lower().strip() == 'executives' or para.text.lower() == 'company participants':
                    reached_executives = True
                para = para.find_next_sibling('p')
            # if we have not reached the analysts list we are still looking at executives
            # iterate through the executives until we reach the analysts
            executives_list = []
            executives = []
            reached_analysts = False
            while not reached_analysts:
                if para == None:
                    pass
                if para.text == '':
                    pass
                elif para.text.lower().strip() == 'analysts' or para.text.lower() == 'conference call participants':
                    reached_analysts = True
                else:
                    if ' - ' in para.text.lower().replace('–', '-'):
                        executive = para.text.lower().replace('–', '-').split(' - ')

                        name = executive[0].strip()
                        job = executive[1].strip()
                        temp = Executive()
                        temp.name = name
                        temp.job = job
                        output_string = '{name}_{quarter}_{year}_{ticker}_{role}'.format(
                            name=name,
                            quarter=info['quarter'],
                            year=info['year'],
                            ticker=info['ticker'],
                            role=job.replace(' ', '').replace(',', '-'))
                        temp.outfile = output_string[:250] + '.txt'
                        executives_list.append(temp)
                        executives.append(name)
                    else:
                        executive = para.text.lower().replace('–', '-')
                        name = executive.strip()
                        job = 'unspecified'
                        temp = Executive()
                        temp.name = name
                        temp.job = job
                        output_string = '{name}_{quarter}_{year}_{ticker}_{role}'.format(
                            name=name,
                            quarter=info['quarter'],
                            year=info['year'],
                            ticker=info['ticker'],
                            role=job.replace(' ', '').replace(',', '-'))
                        temp.outfile = output_string[:250] + '.txt'
                        executives_list.append(temp)
                        executives.append(name)

                para = para.find_next_sibling('p')

            # get list of analysts
            analysts_list = []
            analysts = []
            # iterate through the <p> elements until we reach the operator text, that indicates the end of the analyst list
            reached_operator = False
            while not reached_operator:
                if para == None:
                    pass
                elif para.text == '':
                    pass
                elif para.text.lower().strip() == 'operator':
                    reached_operator = True
                else:
                    if ' - ' in para.text.lower().replace('–', '-'):
                        analyst = para.text.lower().replace('–', '-').split(' - ')


                        name = analyst[0].strip()
                        job = analyst[1].strip()
                        temp = Analyst()
                        temp.name = name
                        temp.company = job
                        output_string = '{name}_{firm}_{quarter}_{year}_{ticker}_{role}'.format(name=name,
                                                                                                firm=job.upper().replace(
                                                                                                    ',', '-').replace(
                                                                                                    '/', '-').replace(
                                                                                                    '[', '').replace(
                                                                                                    ']', ''),
                                                                                                quarter=info['quarter'],
                                                                                                year=info['year'],
                                                                                                ticker=info['ticker'],
                                                                                                role='analyst')
                        temp.outfile = output_string[:250] + '.txt'
                        analysts_list.append(temp)
                        analysts.append(name)
                    else:
                        analyst = para.text.lower().replace('–', '-')
                        name = analyst.strip()
                        job = 'unspecified'
                        temp = Analyst()
                        temp.name = name
                        temp.company = job
                        output_string = '{name}_{firm}_{quarter}_{year}_{ticker}_{role}'.format(name=name,
                                                                                                firm=job.upper().replace(
                                                                                                    ',', '-').replace(
                                                                                                    '/', '-').replace(
                                                                                                    '[', '').replace(
                                                                                                    ']', ''),
                                                                                                quarter=info['quarter'],
                                                                                                year=info['year'],
                                                                                                ticker=info['ticker'],
                                                                                                role='analyst')
                        temp.outfile = output_string[:250] + '.txt'
                        analysts_list.append(temp)
                        analysts.append(name)
                para = para.find_next_sibling('p')

            # for exec in executives_list:
            #      print('Name: {} | Firm: {} | Outfile: {}'.format(exec.name, exec.job, exec.outfile))
            # print()
            # for analyst in analysts_list:
            #     print('Name: {} | Firm: {} | Outfile: {}'.format(analyst.name, analyst.company, analyst.outfile))
            #
            # print()
            # print()
            # for exec in executives:
            #     print(exec)
            # print()
            # for analyst in analysts:
            #     print(analyst)

            # get Q&A content
            # iterate through the <p> elements of the call transcript until we reach the Q & A session, which is what we care about
            reached_qa = False
            while not reached_qa:
                try:
                    if para.text.lower().strip() == 'question-and-answer' or para.text.lower().strip() == 'question-and-answer session':
                        reached_qa = True
                except Exception:
                    break
                para = para.find_next_sibling('p')

            # process to end of transcript
            # this is the stuff we care about
            analysts_order = []
            current_speaker = ''
            previous_speaker = ''
            reached_end = False
            while not reached_end:
                # handle the <p> elements that are a person's name/title
                if para == None:
                    reached_end = True
                elif para.text == '':
                    pass
                elif para.text.lower().strip() == 'operator':
                    previous_speaker = current_speaker[:]
                    current_speaker = 'operator'


                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                # handle <p> elements that contain a split
                elif ' - ' in para.text.lower()[:50].strip().replace('–', '-'):
                    # handle executives
                    if any(para.text.lower().split(' - ')[0].strip() in exec for exec in executives):
                        previous_speaker = current_speaker[:]
                        current_speaker = para.text.lower().split(' - ')[0].strip()

                        for executive in executives_list:
                            if executive.name == current_speaker:
                                if not previous_speaker == current_speaker:
                                    executive.total_dialogues += 1
                                for analyst in analysts_list:
                                    if analyst.name == previous_speaker:
                                        if 'chief executive officer' in str(executive.job) or 'ceo' in str(
                                                executive.job):
                                            analyst.ceo_dialogues += 1
                                        elif 'chief financial officer' in str(executive.job) or 'cfo' in str(
                                                executive.job):
                                            analyst.cfo_dialogues += 1
                                        elif 'chief operating officer' in str(executive.job) or 'coo' in str(
                                                executive.job):
                                            analyst.coo_dialogues += 1

                    # handle analysts
                    elif any(para.text.lower().split(' - ')[0].strip() in analyst for analyst in analysts):
                        previous_speaker = current_speaker[:]
                        current_speaker = para.text.lower().split(' - ')[0].strip()

                        for analyst in analysts_list:
                            if analyst.name == current_speaker:
                                if not previous_speaker == current_speaker:
                                    analyst.total_dialogues += 1
                        # get analyst order in question queue
                        if current_speaker not in analysts_order:
                            analysts_order.append(current_speaker)

                    else:
                        if 'unknown' in para.text.lower().strip() or 'unidentified' in para.text.lower().strip():
                            previous_speaker = current_speaker[:]
                            current_speaker = 'unknown'

                # handle <p> elements that don't contain a split - Executives
                elif any(para.text.lower().strip() in exec for exec in executives):
                    previous_speaker = current_speaker[:]
                    current_speaker = para.text.lower().strip()

                    for executive in executives_list:
                        if executive.name == current_speaker:
                            if not previous_speaker == current_speaker:
                                executive.total_dialogues += 1
                            for analyst in analysts_list:
                                if analyst.name == previous_speaker:
                                    if 'chief executive officer' in str(executive.job) or 'ceo' in str(executive.job):
                                        analyst.ceo_dialogues += 1
                                    elif 'chief financial officer' in str(executive.job) or 'cfo' in str(executive.job):
                                        analyst.cfo_dialogues += 1
                                    elif 'chief operating officer' in str(executive.job) or 'coo' in str(executive.job):
                                        analyst.coo_dialogues += 1



                # handle <p> elements that don't contain a split - Analysts
                elif any(para.text.lower().strip() in analyst for analyst in analysts):
                    previous_speaker = current_speaker[:]
                    current_speaker = para.text.lower().strip()

                    for analyst in analysts_list:
                        if analyst.name == current_speaker:
                            if not previous_speaker == current_speaker:
                                analyst.total_dialogues += 1
                    # get analyst order in question queue
                    if current_speaker not in analysts_order:
                        analysts_order.append(current_speaker)




                ######
                ######
                # handle other cases, general unknown/unidentified
                else:
                    if current_speaker == 'operator':
                        pass
                    elif any(current_speaker in exec for exec in executives):
                        for executive in executives_list:
                            if executive.name == current_speaker:
                                output_file = os.path.join(dir_path, executive.outfile)
                                f = open(output_file, 'a')
                                f.write(para.text.lower() + '\n' + '\n')
                                f.close()

                                for analyst in analysts_list:
                                    if analyst.name == previous_speaker:
                                        current_words = para.text.lower().split()
                                        num_current_words = len(current_words)
                                        if 'chief executive officer' in str(executive.job) or 'ceo' in str(
                                                executive.job):
                                            analyst.total_ceo_words += num_current_words
                                        elif 'chief financial officer' in str(executive.job) or 'cfo' in str(
                                                executive.job):
                                            analyst.total_cfo_words += num_current_words
                                        elif 'chief operating officer' in str(executive.job) or 'coo' in str(
                                                executive.job):
                                            analyst.total_coo_words += num_current_words


                    elif any(current_speaker in analyst for analyst in analysts):
                        for analyst in analysts_list:
                            if analyst.name == current_speaker:
                                output_file = os.path.join(dir_path, analyst.outfile)
                                f = open(output_file, 'a')
                                f.write(para.text.lower() + '\n' + '\n')
                                f.close()
                    else:
                        pass

                ######
                ######
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                ##
                # check if we have more content to grab
                try:
                    para = para.find_next_sibling('p')
                except Exception:
                    pass

            # set analyst speaking order for each analyst object
            for analyst in analysts_list:
                for person in analysts_order:
                    if analyst.name == person:
                        analyst.order_in_queue = analysts_order.index(person) + 1
            print()
            # get total number of words spoken by analyst
            try:
                for analyst in analysts_list:
                    output_string = analyst.outfile
                    file_path = os.path.join(dir_path, output_string)
                    f = open(file_path, 'r')
                    content = f.readlines()
                    for line in content:
                        line_words = len(line.split())
                        analyst.total_words += line_words
            except Exception:
                # print('Analyst never spoke')
                pass

            try:
                for executive in executives_list:
                    output_string = executive.outfile
                    file_path = os.path.join(dir_path, output_string)
                    f = open(file_path, 'r')
                    content = f.readlines()
                    for line in content:
                        line_words = len(line.split())
                        executive.total_words += line_words
            except Exception:
                # print('Executive never spoke')
                pass

            # fix broken analyst/executive data
            for analyst in analysts_list:
                if len(analyst.name.split()) > 4:
                    analyst.name = 'broken'
                    analyst.outfile = 'broken'
            for executive in executives_list:
                if len(executive.name.split()) > 4:
                    executive.name = 'broken'
                    executive.outfile = 'broken'

            # output to csv
            output_to_csv = []
            header_row = ['Name', 'Position', 'Firm', 'Quarter', 'Ticker', 'Date', 'Analyst', 'Order of Question',
                          'Number of Analysts', 'Number of Dialogues',
                          'Number of CEO Dialogues', 'Number of CEO Words', 'Number of CFO Dialogues',
                          'Number of CFO Words', 'Number of COO Dialogues', 'Number of COO Words', 'Total Words',
                          'Text']
            output_to_csv.append(header_row)
            for executive in executives_list:
                new_row = [executive.name, executive.job, ticker, quarter, ticker, date,
                           0, 'N/A', len(analysts_list), executive.total_dialogues, 'N/A',
                           'N/A', 'N/A', 'N/A', 'N/A', 'N/A', executive.total_words, executive.outfile]
                output_to_csv.append(new_row)
            for analyst in analysts_list:
                new_row = [analyst.name, 'analyst', analyst.company, quarter, ticker, date, 1,
                           analyst.order_in_queue, len(analysts_list), analyst.total_dialogues,
                           analyst.ceo_dialogues, analyst.total_ceo_words, analyst.cfo_dialogues,
                           analyst.total_cfo_words, analyst.coo_dialogues, analyst.total_coo_words, analyst.total_words,
                           analyst.outfile]
                output_to_csv.append(new_row)

            sub1 = 'output_files'
            sub2 = '{ticker}_{quarter}_{year}'.format(ticker=info['ticker'], quarter=info['quarter'], year=info['year'])
            file_name = '{name}_{quarter}_{year}.csv'.format(
                name = name,
                quarter = quarter,
                year = year
            ).replace(' ', '_').replace('\'', '')
               
            save_path = os.path.join(sub1, sub2, file_name)
            with open(save_path, 'w') as f:
                writer = csv.writer(f)
                writer.writerows(output_to_csv)

            print('success')

        except Exception:
            print('no body')
    except:
        PrintException()


def folder_cleanup():  # function to remove empty folders from the processed items
    sub1 = 'output_files'
    path = os.path.join(sub1)
    for root, dirs, files in os.walk(path):
        for directory in dirs:
            dir_path = os.path.join(root, directory)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)


def main():
    sub1 = 'scrapes'
    dirs = os.listdir(sub1)

    # test_list = []
    # for file in dirs:
    #     test_list.append(file)
    #
    # x = test_list[0]
    #
    # process_saved_page(file, sub1)

    with concurrent.futures.ProcessPoolExecutor(max_workers=None) as executor:
        future_to_file = {executor.submit(process_saved_page, file, sub1): file for file in dirs}

    folder_cleanup()


main()
