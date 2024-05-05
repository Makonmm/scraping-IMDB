import requests
import time
import csv
import random
from bs4 import BeautifulSoup
import threading

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'}

# Lista para armazenar os detalhes dos filmes
movies_data = []


def extract_movie_details(movie_link):
    time.sleep(random.uniform(0, 0.2))
    response = requests.get(movie_link, headers=headers)
    movie_soup = BeautifulSoup(response.content, 'html.parser')

    if movie_soup is not None:
        title_element = movie_soup.find(
            'h1', attrs={'data-testid': 'hero__pageTitle'}).find('span')
        title_text = title_element.get_text(
            strip=True) if title_element else None

        date_element = movie_soup.find('ul', class_='ipc-inline-list ipc-inline-list--show-dividers sc-d8941411-2 cdJsTz baseAlt').find(
            'li', class_='ipc-inline-list__item').find('a')
        date_text = date_element.get_text(strip=True) if date_element else None

        rating_element = movie_soup.find(
            'div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
        rating_text = rating_element.get_text(
            strip=True) if rating_element else None

        plot_element = movie_soup.find(
            'span', attrs={'data-testid': 'plot-xl'})
        plot_text = plot_element.get_text(strip=True) if plot_element else None

        return title_text, date_text, rating_text, plot_text


def extract_movies(movie_links):
    global movies_data
    for movie_link in movie_links:
        movie_details = extract_movie_details(movie_link)
        if movie_details:
            movies_data.append(movie_details)


def save_to_csv():
    global movies_data
    with open('movies.csv', mode='w', newline='', encoding='utf-8') as file:
        movie_writer = csv.writer(
            file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        movie_writer.writerow(['Title', 'Date', 'Rating', 'Plot'])
        for movie_data in movies_data:
            movie_writer.writerow(movie_data)


def main():
    start_time = time.time()

    # URL dos 100 filmes mais populares do IMDb
    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    movies_table = soup.find(
        'div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')
    movie_links = ['https://imdb.com' +
                   movie.find('a')['href'] for movie in movies_table_rows]

    # Dividindo a lista de links em partes iguais
    num_threads = 5
    links_per_thread = len(movie_links) // num_threads
    divided_links = [movie_links[i:i + links_per_thread]
                     for i in range(0, len(movie_links), links_per_thread)]

    # Criando e iniciando threads para extrair detalhes dos filmes
    threads = []
    for links_chunk in divided_links:
        thread = threading.Thread(target=extract_movies, args=(links_chunk,))
        thread.start()
        threads.append(thread)

    # Aguardando todas as threads terminarem
    for thread in threads:
        thread.join()

    # Salvando os detalhes dos filmes em um arquivo CSV
    save_to_csv()

    end_time = time.time()
    print('Total time taken: ', end_time - start_time)


if __name__ == '__main__':
    main()
