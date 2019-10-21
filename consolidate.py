import os
import csv


def consol():
   subdir = 'outputs'
   dirName = os.path.join(subdir)
   listOfFiles = []
   for(dirpath, dirnames, filenames) in os.walk(dirName):
       listOfFiles += [os.path.join(dirpath, file) for file in filenames]
       print('Added file')
   
   csvs = []
   for file in listOfFiles:
      if str(file).endswith('.csv'):
       csvs.append(str(file))
   
   csv_header = 'Name,Position,Firm,Quarter,Ticker,Date,Analyst,Order of Question,Number of Analysts,' \
                'Number of Dialogues,Number of CEO Dialogues,Number of CEO Words,Number of CFO Dialogues,' \
                'Number of CFO Words,Number of COO Dialogues,Number of COO Words,Total Words,Text'
   
   csv_out = 'consolidated.csv'
   
   csv_merge = open(csv_out, 'w')
   csv_merge.write(csv_header)
   csv_merge.write('\n')
   csv_merge.close()
   
   # counter for lines
   none_counter = 0
   
   for file in csvs:
       csv_merge = open(csv_out, 'a')
       csv_in = open(file)
       for i, line in enumerate(csv_in):
         if line.split(',')[-1].lower().strip() == 'none':
            none_counter += 1
         if i == 0:
            pass
         else:
            csv_merge.write(line)
       csv_in.close()
       csv_merge.write('\n')
       csv_merge.write('\n')
       csv_merge.close()
       print('Verify consolidated csv file : ' + csv_out)
      
   print()
   print()
   print(none_counter)


def count_empty():
   empty_lines = 0
   total_lines = 0
   lines = []
   with open('consolidated.csv', 'r') as fin:
      reader = csv.reader(fin)
      for line in reader:
         lines.append(line)
   
   for line in lines:
      total_lines += 1
      
      if bool(line) == False:
         empty_lines += 1
   
   print('Total Lines in file: {}'.format(total_lines))
   print('Total empty lines in file: {}'.format(empty_lines))
   print('Lines of content in file: {}'.format(total_lines - empty_lines))

consol()
count_empty()
   