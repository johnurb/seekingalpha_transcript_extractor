import os
import shutil
import csv


def clean_names():
    src_folder = 'expanded'
    dest_folder = 'expanded'
    if not os.path.exists(dest_folder):
        os.mkdir(dest_folder)
    for root, dirs, files in os.walk(os.path.normpath(src_folder)):
        for name in files:
            replaced = name.replace('[','').replace(']','').replace('(', '').replace(')', '')
            os.rename(os.path.join(root, name), os.path.join(root, replaced))


def one_folder():
    src_folder = 'outputs'
    dest_folder = 'expanded'
    if not os.path.exists(dest_folder):
        os.mkdir(dest_folder)

    for root, dirs, files in os.walk(os.path.normpath(src_folder)):
          for name in files:
             if name.endswith('.txt'):
                src = os.path.join(root, name)
                shutil.copy2(src, dest_folder)



def clean_folders():
    src_folder = 'output_files'
    dest_folder = 'bad'
    if not os.path.exists(dest_folder):
        os.mkdir(dest_folder)

    for root, dirs, files in os.walk(os.path.normpath(src_folder)):
        for dir in dirs:
            ending = dir[-4:]
            ending_list = list(ending)
            move = False
            for item in ending_list:
                if not item.isdigit():
                    move = True
            if 'undefined' in str(dir.lower()) or 'unspecified' in str(dir.lower()) or move:
                dir_to_move = os.path.join(src_folder, dir)
                shutil.move(dir_to_move, dest_folder)
                print('moved')


def something():
     for root, dirs, files in os.walk(os.path.normpath(src_folder)):
         all_good = False
         for name in files:
             if name.endswith('analyst.txt'):
                all_good = True
         if not all_good:
             dir_to_move = os.path.join(src_folder, dir)
             try:
                 shutil.move(dir_to_move, dest_folder)
                 print('moved')
             except:
                 print('wah')

def clean_csv():
    subdir = 'outputs'
    dirName = os.path.join(subdir)
    listOfFiles = []
    for(dirpath, dirnames, filenames) in os.walk(dirName):
        listOfFiles += [file for file in filenames]
    
    csv_header = 'Name,Position,Firm,Quarter,Ticker,Date,Analyst,Order of Question,Number of Analysts,' \
             'Number of Dialogues,Number of CEO Dialogues,Number of CEO Words,Number of CFO Dialogues,' \
             'Number of CFO Words,Number of COO Dialogues,Number of COO Words,Total Words,Text'

    csv_out = 'consolidated.csv'
    
    csv_merge = open(csv_out, 'w')
    csv_merge.write(csv_header)
    csv_merge.write('\n')
    csv_merge.close()
    
    original_lines = []
    with open('consolidated.csv', 'r') as f:
        reader = csv.reader(f)
        for i, line in enumerate(reader):
            if i == 0:
                pass
            else:
                original_lines += [line]
            
    
    
    with open(csv_out, 'a') as outfile:
        writer = csv.writer(outfile)
        for file in listOfFiles:
            for item in original_lines:
                if len(item) < 1:
                    pass
                else:
                    if file == item[-1]:
                       writer.writerow(item)
                    
            
        
def main():
    #clean_csv()
    one_folder()
        
        
        
        
        
        
    
    
    
    
main()