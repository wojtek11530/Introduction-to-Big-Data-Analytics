import time

import multiprocessing as mp
import requests, bs4, os, errno, zipfile, glob
from urllib.request import urlretrieve
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer

# import nltk
# nltk.download('stopwords')
# nltk.download('punkt')

FILENAME = 'urls_to_books.txt'
STOPWORDS = stopwords.words('english')


def run():
    # get_urls_to_zip_files()
    # get_books_from_urls_to_zip_files()

    books_number = 150
    top_counts = 20
    lines = read_book_lines(books_number)

    beginning_time = time.time()
    word_counts = count_words_serialized_manner(lines)
    elapsed_time_serial_processing = time.time() - beginning_time

    print('SERIAL PROCESSING')
    print('Elapsed time: ' + str(elapsed_time_serial_processing) + ' s\n')
    print_sorted_counts(word_counts, top=top_counts)

    beginning_time = time.time()
    word_counts = count_words_parallel_manner(lines)
    elapsed_time_parallel_processing = time.time() - beginning_time

    print('\n' + '#' * 25 + '\n')
    print('PARALLEL PROCESSING')
    print('Elapsed time: ' + str(elapsed_time_parallel_processing) + ' s\n')
    print_sorted_counts(word_counts, top=top_counts)


def count_words_serialized_manner(lines):
    single_word_counts = []
    for line in lines:
        counts_list = count_words_mapper(line)
        single_word_counts += counts_list

    word_counts = reduce_counts(single_word_counts)
    return word_counts


def count_words_parallel_manner(lines):
    pool = mp.Pool(mp.cpu_count() - 1)
    single_word_counts = pool.map(count_words_mapper, [line for line in lines])
    single_word_counts = [item for sublist in single_word_counts for item in sublist]
    word_counts = reduce_counts(single_word_counts)
    return word_counts


def count_words_mapper(line):
    word_count_list = []
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(line)
    tokens_without_stopwords = [w for w in tokens if not w in STOPWORDS]
    for token in tokens_without_stopwords:
        word_count_list.append((token, 1))
    return word_count_list


def reduce_counts(single_words_count):
    word_count = {}
    for word, _ in single_words_count:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1
    return word_count


def print_sorted_counts(word_counts, top=10):
    word_counts_sorted = {k: v for k, v in sorted(word_counts.items(), key=lambda item: item[1], reverse=True)}

    i = 1
    print('Top ' + str(top) + ' counts')
    for k, v in word_counts_sorted.items():
        print(str(i) + '.' + k + ': ' + str(v))
        i += 1
        if i > 20:
            break


def read_book_lines(books_number=150):
    folder_path = 'books/'

    start_reading_line = '*** START OF THIS PROJECT GUTENBERG EBOOK'
    end_reading_line = 'End of the Project Gutenberg EBook'

    prestart = False
    reading_line = False
    produce_information = False

    txt_file_names = glob.glob(folder_path + '*.txt')
    book_index = 0

    book_lines = []

    for txt_file_name in txt_file_names:

        with open(txt_file_name, 'r') as txt_file:
            for line in txt_file:

                if end_reading_line in line:
                    reading_line = False

                if prestart:
                    processed_line = preprocess_line(line)
                    if processed_line != '':
                        if not produce_information:
                            produce_information = True
                        else:
                            produce_information = False
                            prestart = False
                            reading_line = True

                if reading_line:
                    processed_line = preprocess_line(line)
                    if processed_line != '':
                        book_lines.append(processed_line)

                if start_reading_line in line:
                    prestart = True

        book_index += 1
        if book_index >= books_number:
            break

    return book_lines


def preprocess_line(line):
    return line.rstrip("\n").lower()


def get_urls_to_zip_files():
    urls_to_books = []
    if not os.path.exists(FILENAME):

        webpage_with_books_url = 'http://www.gutenberg.org/robot/harvest?filetypes[]=txt&langs[]=en'
        num_og_webpages_wth_books = 0

        while num_og_webpages_wth_books < 10:

            print('Reading webpage: ' + webpage_with_books_url)
            webpage_with_books = requests.get(webpage_with_books_url, timeout=20.0)
            if webpage_with_books:
                webpage_with_books = bs4.BeautifulSoup(webpage_with_books.text, "lxml")
                urls = [el.get('href') for el in
                        webpage_with_books.select('body > p > a[href^="http://aleph.gutenberg.org/"]')]
                url_to_next_page = webpage_with_books.find_all('a', string='Next Page')

                if len(urls) > 0:
                    urls_to_books.append(urls)

                    if url_to_next_page[0]:
                        webpage_with_books_url = "http://www.gutenberg.org/robot/" + url_to_next_page[0].get('href')
            num_og_webpages_wth_books = num_og_webpages_wth_books + 1
            print(num_og_webpages_wth_books)

        urls_to_books = [item for sublist in urls_to_books for item in sublist]

        # Backing up the list of URLs
        with open(FILENAME, 'w') as output:
            for u in urls_to_books:
                output.write('%s\n' % u)


def get_books_from_urls_to_zip_files():
    folder_path = 'books/'
    if len(glob.glob(folder_path)) == 0:
        os.makedirs(folder_path)
    url_to_zip_file_num = 0

    with open('urls_to_books.txt', 'r') as f:
        urls_to_books = f.read().splitlines()

    for url in urls_to_books[url_to_zip_file_num:]:

        dst = 'books/' + url.split('/')[-1].split('.')[0].split('-')[0]

        with open('logfile.log', 'w') as f:
            f.write('Unzipping file #' + str(url_to_zip_file_num) + ' ' + dst + '.zip' + '\n')

        if len(glob.glob(dst + '*')) == 0:
            urlretrieve(url, dst + '.zip')

            with zipfile.ZipFile(dst + '.zip', "r") as zip_ref:
                try:
                    zip_ref.extractall("books/")
                    print(str(url_to_zip_file_num) + ' ' + dst + '.zip ' + 'unzipped successfully!')
                except NotImplementedError:
                    print(str(url_to_zip_file_num) + ' Cannot unzip file:', dst)
            os.remove(dst + '.zip')
        url_to_zip_file_num += 1


if __name__ == '__main__':
    run()
