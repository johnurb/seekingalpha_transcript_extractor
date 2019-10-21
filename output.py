import os
from bs4 import BeautifulSoup
import concurrent.futures
from analyst_class import Analyst
from executive_class import Executive
import linecache
import sys
import csv
from time import sleep
import re
import html2text
from unidecode import unidecode


def fix_codec():
    in_dir = 'raw_texts'
    out_dir = 'texts2'
    
    try:
        os.mkdir(out_dir)
    except Exception:
        pass
    
    current_files = os.listdir(in_dir)
    
    at_analysts = False
    at_start = False
    has_sep = False
    for item in current_files:
        file_name = item
        
        if file_name == '.DS_Store':
            pass
        
        else:
            with open(os.path.join(in_dir, file_name), 'r') as fin:
                raw_lines = fin.readlines()

            for line in raw_lines:
                if line.lower().strip().startswith('**analysts'):
                    at_analysts = True
                if at_analysts == True and line.lower().strip().startswith('**'):
                    at_start = True
                
                if at_start == False and ' - ' in line.lower().replace('–','-'):
                    has_sep = True
            
            at_analysts = False
            at_start = False
            fixed_lines = []
            for line in raw_lines:
                if line.lower().strip().startswith('**analysts'):
                    at_analysts = True
                if at_analysts == True and line.lower().strip().startswith('**'):
                    at_start = True
                    
                if has_sep == True:
                    fixed_lines.append(unidecode(line).replace('–','-'))
                else:
                    if at_start == False:
                        fixed_lines.append(unidecode(line).replace('–','-').replace(',', ' - ', 1).replace(',',''))
                        
                    else:
                        if line.lower().startswith('**'):
                            fixed_lines.append(unidecode(line).replace('–','-').replace(',', ' - ', 1).replace(',',''))
                        else:
                            fixed_lines.append(unidecode(line).replace('–','-'))
                            
                        
                
            with open(os.path.join(out_dir, file_name), 'w') as fout:
                for line in fixed_lines:
                    fout.write(line)
            
    
    
# function to get the data from a transcript .txt file in order to determine if its complete and then determine output filename
def get_page_meta(lines):
    regex = r"\b[q][1-4]\b" # q[1-4]
    regex2 = r"\b[f][1-4][q][0-9]{2}\b" # f[1-4]q[xx]
    regex3 = r"\b[f][1-4][q][0-9]{4}\b" # f[1-4]q[xxxx]
    
    # initialize transcript metadata variables
    date = ''
    name = ''
    ticker = ''
    quarter = ''
    
    has_qa = False
    # remove the lines of the transcript text that are unnecessary/mess up formatting
    fixed_lines = []
    for line in lines:
        if line == '' or line == '\n' or line.lower().strip() == 'earnings call transcript':
            pass
        else:
            fixed_lines.append(line.replace('\n','').replace('–','-').strip())
    
    # iterate through each line in the text, grabbing desired data
    reached_date = False
    reached_about = False # company name and ticker
    has_qa = False
    got_quarter = False
    
    for i, line in enumerate(fixed_lines):
        
        # iterate through until we reach and pull out the date
        if line.lower().startswith('transcript') and reached_date == False:
            date_index = i + 2
            date = fixed_lines[date_index]
            
            #format for use in outfiles and spreadsheet
            date_parts = date.split()
            date = ' '.join(date_parts[0:3]).replace(',', '').replace('.', '')
            year = date.split()[-1]
            
            reached_date = True
            
        # iterate through until we reach and pull out the company name and ticker
        if line.lower().startswith('| about:') and reached_about == False:
            company_data = line.split(':')[1].strip()
            name = company_data.split('(')[0].strip()
            ticker = company_data.split('(')[1].strip().split(')')[0]
            reached_about = True
        
        # get the quarter of the call
        if reached_date == True and reached_about == True:
            generic_re = re.compile('{}|{}|{}'.format(regex, regex2, regex3)).findall(line.lower())
            if generic_re:
                quarter = generic_re[0]
                
                got_quarter = True
                
                if reached_date == True and reached_about == True and got_quarter == True:
                    file_name = '{name}_{ticker}_{quarter}_{year}'.format(
                        name = name,
                        ticker = ticker,
                        quarter = quarter,
                        year = year,
                    ).lower().replace(' ','').replace('.','').replace(',','').replace('/','').strip() + '.txt'
                    
                
            
        # QUESSTION-AND-ANSWER
        if line.lower().startswith('**question-and-answer'):
            has_qa = True
       
    try:        
        if has_qa == True:
            return file_name
        else:
            return None
    except:
        pass
    
    
# convert scraped html pages to .txt
def scrape2text(file):
    archive = 'scrapes'
    out = 'textfiles'

    # work through file
    
    location = os.path.join(archive, file)
  
     # read in individual HTML file
    with open(location, 'r') as fin:
        text = fin.read()
    
    # convert individual HTML page to HTML2Text content
    text_maker = html2text.HTML2Text()
    text_maker.ignore_links = True
    text_maker.ignore_images = True
    text_maker.ignore_anchors = True
    text_maker.body_width = 0
    text = text_maker.handle(text)
    
    with open('temp.txt', 'w') as temp:
        temp.write(text)
        
    with open('temp.txt', 'r') as temp:
        temp_lines = temp.readlines()
        
    os.remove('temp.txt')
            
    file_name = get_page_meta(temp_lines)
        
    # write to .txt
    if file_name == None:
        pass
    else:
        fout = os.path.join(out, file_name)
        with open(fout, 'w') as fout:
            fout.write(text)


# function to initialize the conversions from the scraped html pages to .txts
def init_conversion():
    archive = 'scrapes'
    html_files = os.listdir(archive)
    
    out = 'textfiles'
    try:
        os.mkdir(out)
    except Exception:
        pass
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=None) as executor:
        future_to_file = {executor.submit(scrape2text, file): file for file in html_files}
    
 
# process files
def analyze(filein):
    with open(filein, 'r') as fin:
        lines = fin.readlines()
        
    outputs = 'outputs'
    out_folder = 'YYYYYYYYYYYYYYYYYYYY'
    try:
        os.mkdir(os.path.join(outputs, out_folder))
    except Exception:
        pass
    
    regex = r"\b[q][1-4]\b" # q[1-4]
    regex2 = r"\b[f][1-4][q][0-9]{2}\b" # f[1-4]q[xx]
    regex3 = r"\b[f][1-4][q][0-9]{4}\b" # f[1-4]q[xxxx]
    
    # initialize transcript metadata variables
    date = ''
    name = ''
    ticker = ''
    quarter = ''
    year = ''
    
    # remove the lines of the transcript text that are unnecessary/mess up formatting
    fixed_lines = []
    for line in lines:
        if line == '' or line == '\n' or line.lower().strip() == 'earnings call transcript':
            pass
        else:
            fixed_lines.append(line.replace('\n','').replace('–','-').strip())
    
    # iterate through each line in the text, grabbing desired data
    reached_date = False
    reached_about = False # company name and ticker
    through_execs = False
    at_execs = False
    through_analysts = False
    at_analysts = False
    at_qa = False
    got_quarter = False
    
    executives_list = []
    executives = []
    analysts_list = []
    analysts = []
    
    analysts_order = []
    current_speaker = ''
    previous_speaker = ''
    for i, line in enumerate(fixed_lines):
        
        # iterate through until we reach and pull out the date
        if line.lower().startswith('transcript') and reached_date == False:
            date_index = i + 2
            date = fixed_lines[date_index]
            
            #format for use in outfiles and spreadsheet
            date_parts = date.split()
            date = ' '.join(date_parts[0:3]).replace(',', '').replace('.', '')
            year = date.split()[-1]
            
            reached_date = True
            
        # iterate through until we reach and pull out the company name and ticker
        if line.lower().startswith('| about:') and reached_about == False:
            company_data = line.split(':')[1].strip()
            name = company_data.split('(')[0].strip()
            ticker = company_data.split('(')[1].strip().split(')')[0]
            reached_about = True
        
        # get the quarter of the call
        if reached_date == True and reached_about == True and got_quarter == False:
            generic_re = re.compile('{}|{}|{}'.format(regex, regex2, regex3)).findall(line.lower())
            if generic_re:
                quarter = generic_re[0]
                
                got_quarter = True
                
                if reached_date == True and reached_about == True and got_quarter == True:
                    out_folder = '{ticker}_{quarter}_{year}'.format(
                        ticker = ticker,
                        quarter = quarter,
                        year = year
                    ).lower()
                    
                    
                
                    try:
                        os.mkdir(os.path.join(outputs, out_folder))
                    except Exception:
                        pass
                
                    
        
        
        # EXECUTIVES
        # EXECUTIVES
        # EXECUTIVES
        # EXECUTIVES
        # EXECUTIVES
        if line.lower().startswith('**executives'):
            at_execs = True
        # get the executives
        if through_execs == False and at_execs == True:
            if line.lower().startswith('**analysts'):
                through_execs = True
                
            else:
                if ' - ' in line:
                    executive = line.lower().split(' - ')
                    exec_name = executive[0].strip()
                    job = executive[1].strip()
                    temp = Executive()
                    temp.name = exec_name
                    temp.job = job
                    output_string = '{exec_name}_{quarter}_{year}_{ticker}_{role}'.format(
                        exec_name = exec_name.strip(),
                        quarter = quarter.strip(),
                        year = year.strip(),
                        ticker = ticker.strip(),
                        role = job.replace(' ', '').replace(',', '-')
                    ).replace('/','-')[:230] + '.txt'
                    temp.outfile = os.path.join(out_folder, output_string)
                    executives_list.append(temp)
                    executives.append(exec_name)
                    
                else:
                    if(line.lower().startswith('**executives')) or '**' in line:
                        pass
                    else:
                        executive = line.lower().strip()
                        exec_name = executive
                        job = 'undefined'
                        temp = Executive()
                        temp.name = exec_name
                        temp.job = job
                        output_string = '{exec_name}_{quarter}_{year}_{ticker}_{role}'.format(
                            exec_name = exec_name.strip(),
                            quarter = quarter.strip(),
                            year = year.strip(),
                            ticker = ticker.strip(),
                            role = job.replace(' ', '').replace(',', '-')
                        ).replace('/','-')[:230] + '.txt'
                        temp.outfile = os.path.join(out_folder, output_string)
                        executives_list.append(temp)
                        executives.append(exec_name)
                            
        # ANALYSTS
        # ANALYSTS
        # ANALYSTS
        # ANALYSTS
        # ANALYSTS
        if line.lower().startswith('**analysts'):
            at_analysts = True
        # get the analysts
        if through_analysts == False and at_analysts == True:
            if line.lower().startswith('**presentation') or line.lower().startswith('**operator') or (line.lower().startswith('**') and not 'analysts' in line.lower()):
                through_analysts = True
                
            else:
                if ' - ' in line:
                    analyst = line.lower().split(' - ')
                    analyst_name = analyst[0].strip()
                    job = analyst[1].strip()
                    temp = Analyst()
                    temp.name = analyst_name
                    temp.company = job
                    output_string = '{analyst_name}_{firm}_{quarter}_{year}_{ticker}_{role}'.format(
                        analyst_name = analyst_name,
                        firm = job.replace(',','').replace('.','').upper().strip(),
                        quarter = quarter.strip(),
                        year = year.strip(),
                        ticker = ticker.strip(),
                        role = 'analyst'
                    ).replace('/','-')[:230] + '.txt'
                    temp.outfile = os.path.join(out_folder, output_string)
                    analysts_list.append(temp)
                    analysts.append(analyst_name)
                    
                else:
                    if(line.lower().startswith('**analysts')) or '**' in line:
                        pass
                    else:
                        analyst = line.lower().strip()
                        analyst_name = analyst
                        job = 'undefined'
                        temp = Analyst()
                        temp.name = analyst_name
                        temp.company = job
                        output_string = '{analyst_name}_{firm}_{quarter}_{year}_{ticker}_{role}'.format(
                            analyst_name = analyst_name,
                            firm = job.replace(',','').replace('.','').upper(),
                            quarter = quarter.strip(),
                            year = year.strip(),
                            ticker = ticker.strip(),
                            role = 'analyst'
                        ).replace('/','-')[:230] + '.txt'
                        temp.outfile = os.path.join(out_folder, output_string)
                        analysts_list.append(temp)
                        analysts.append(analyst_name)
        
        # QUESSTION-AND-ANSWER
        # QUESSTION-AND-ANSWER
        # QUESSTION-AND-ANSWER
        # QUESSTION-AND-ANSWER
        # QUESSTION-AND-ANSWER
        if line.lower().startswith('**question-and-answer'):
            at_qa = True
        # get q&a data
        if at_qa == True:
            # this part gets the speaker
            if line.startswith('**'):
                current_name = ''
                if ' - ' in line:
                    current_name = line.lower().split(' - ')[0].replace('**','')
                    previous_speaker = current_speaker[:]
                    current_speaker = current_name
                else:
                    current_name = line.lower().replace('**','')
                    previous_speaker = current_speaker[:]
                    current_speaker = current_name
                
                # test
                #if any(current_speaker in name for name in analysts) or any(current_speaker in name for name in executives):
                    #print(current_speaker)   
            
            # this part handles the actual spoken words       
            else:
                # handle words from executives
                if any(current_speaker in name for name in executives):
                    for executive in executives_list:
                        if current_speaker == executive.name:
                            with open(os.path.join(outputs, executive.outfile), 'a') as fout:
                                fout.write(line + '\n')
                            
                            if fixed_lines[i-1].startswith('**'):
                                executive.total_dialogues += 1
                            executive.total_words += len(line.split())
                                
                            for analyst in analysts_list:
                                if analyst.name == previous_speaker:
                                    current_words = line.split()
                                    num_current_words = len(current_words)
                                    
                                    if 'chief executive officer' in str(executive.job) or 'ceo' in str(executive.job):
                                        analyst.total_ceo_words += num_current_words
                                        if fixed_lines[i-1].startswith('**'):
                                            analyst.ceo_dialogues += 1
                                    elif 'chief financial officer' in str(executive.job) or 'cfo' in str(executive.job):
                                        analyst.total_cfo_words += num_current_words
                                        if fixed_lines[i-1].startswith('**'):
                                            analyst.cfo_dialogues += 1
                                    elif 'chief operating officer' in str(executive.job) or 'coo' in str(executive.job):
                                        analyst.total_coo_words += num_current_words
                                        if fixed_lines[i-1].startswith('**'):
                                            analyst.coo_dialogues += 1
                
                # handle words from analysts
                elif any(current_speaker in name for name in analysts):
                    for analyst in analysts_list:
                        if current_speaker == analyst.name:
                            with open(os.path.join(outputs, analyst.outfile), 'a') as fout:
                                fout.write(line + '\n')
                            
                            if fixed_lines[i-1].startswith('**'):
                                analyst.total_dialogues += 1
                            analyst.total_words += len(line.split())
                            
                            if current_speaker not in analysts_order:
                                analysts_order.append(current_speaker)
                                
                
                # else, should be operator text [unknown analyst/executive]
                else:
                    if current_speaker == 'operator':
                        pass
                    elif 'unknown' in current_speaker or 'undefined' in current_speaker:
                        text_name = 'unknown.txt'
                        file_out = os.path.join(outputs, out_folder, text_name)
                        with open(file_out, 'a') as fout:
                            fout.write(line + '\n')
                    else:
                        pass
    
    
    # set analyst speaking order for each analyst object
    for analyst in analysts_list:
        for person in analysts_order:
            if analyst.name == person:
                analyst.order_in_queue = analysts_order.index(person) + 1
                
                
                
    # output to csv
    output_to_csv = []
    header_row = ['Name', 'Position', 'Firm', 'Quarter', 'Ticker', 'Date', 'Analyst', 'Order of Question',
                    'Number of Analysts', 'Number of Dialogues',
                    'Number of CEO Dialogues', 'Number of CEO Words', 'Number of CFO Dialogues',
                    'Number of CFO Words', 'Number of COO Dialogues', 'Number of COO Words', 'Total Words',
                    'Text']
    output_to_csv.append(header_row)
    
    for executive in executives_list:
        if executive.total_words == 0:
            executive.outfile = 'na/none'
        new_row = [executive.name, executive.job, ticker, quarter, ticker, date,
                    0, 'N/A', len(analysts_list), executive.total_dialogues, 'N/A',
                    'N/A', 'N/A', 'N/A', 'N/A', 'N/A', executive.total_words, executive.outfile.split('/')[1]]
        output_to_csv.append(new_row)
        
    for analyst in analysts_list:
        if analyst.total_words == 0:
            analyst.outfile = 'na/none'
        new_row = [analyst.name, 'analyst', analyst.company.upper(), quarter, ticker, date, 1,
                    analyst.order_in_queue, len(analysts_list), analyst.total_dialogues,
                    analyst.ceo_dialogues, analyst.total_ceo_words, analyst.cfo_dialogues,
                    analyst.total_cfo_words, analyst.coo_dialogues, analyst.total_coo_words, analyst.total_words,
                    analyst.outfile.split('/')[1]]
        output_to_csv.append(new_row)

    out_dir = os.path.join(outputs, out_folder)
    file_name = '{name}_{quarter}_{year}.csv'.format(
        name = name,
        quarter = quarter,
        year = year
    ).replace(' ', '').replace('/', '').replace(',','').strip()
    save_path = os.path.join(out_dir, file_name)
            
    with open(save_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(output_to_csv)
    
    print('success')
            
          
     
def main():
    # read-in transcript call text
    #archive = 'fixed_texts'
    archive = 'fixed_texts'
    files = os.listdir(archive)
    
    try:
        os.mkdir('outputs')
    except Exception:
        pass
    
    for file in files:
        filein = os.path.join(archive, file)
        
        analyze(filein)

def count():    
    cpt = sum([len(files) for r, d, files in os.walk('expanded')])
    print(cpt)


def count_broke():
    bad_dir = os.path.join('outputs', 'YYYYYYYYYYYYYYYYYYYY')
    print(len(os.listdir(bad_dir)))
main()
#count_broke()




    