import os
import logging
import pandas as pd
from bs4 import BeautifulSoup

import common
from artsuebli_urbanart import credentials


def main():
    r = common.requests_get('https://www.artstuebli.ch/walls/')
    soup = BeautifulSoup(r.content, 'html.parser')
    div = soup.find('div', class_='image_list')
    df = pd.DataFrame()
    for row_num, ul in enumerate(div.find_all('ul')):
        for col_num, li in enumerate(ul.find_all('li')):
            index = 4 * row_num + col_num
            clickable_object = li.find_next('a')
            df.at[index, 'url_object'] = f"https://www.artstuebli.ch{clickable_object['href']}"
            img = li.find_next('img')
            # Extract from the string below the url for every resolution
            data_responsive_src = img['data-responsive-src']
            resolutions = ['345', '470', '650', '750', '850', '1000', '1200', '1400', '1600', '2400', '2600', '2800', '3000']
            for res in resolutions:
                df.at[index, f'img_url_{res}'] = data_responsive_src.split(f"'{res}': '")[1].split("'")[0]
            img_alt = img['alt']
            df.at[index, 'img_alt'] = img_alt
            correctly_capitalized = capitalize_except_abbreviations(img_alt)
            artists_origin, year_loc_desc = correctly_capitalized.split('|')
            if '(' in artists_origin:  # If there is an origin its in brackets
                artists, df.at[index, 'origin'] = artists_origin.split('(')
                df.at[index, 'origin'] = df.at[index, 'origin'].replace(')', '')
            else:
                artists = artists_origin
            if '&' in artists:
                df.at[index, 'artist'], df.at[index, 'further_artists'] = artists.split('&')
            else:
                df.at[index, 'artist'] = artists
            # The first four digits in the remaining description are the year
            df.at[index, 'year'] = year_loc_desc[:5].replace(' ', '')
            loc_desc = year_loc_desc[5:].strip()
            parts = loc_desc.split(' Auftraggeber: ')
            first_part = parts[0]

            if len(parts) > 1:
                principal_part = parts[1].split(' Principal: ')
                df.at[index, 'auftraggeber'] = principal_part[0]
                if len(principal_part) > 1:
                    df.at[index, 'principal'] = principal_part[1]

            events = first_part.split(' Event: ')
            if len(events) > 1:
                df.at[index, 'event_de'] = events[1]
                if len(events) > 2:
                    df.at[index, 'event_en'] = events[2]
                first_part = events[0]

            # Determine if the separator between street and municipality is a comma or space
            if ',' in first_part:
                street, municipality = first_part.split(',', 1)
            else:
                street, municipality = first_part.rsplit(' ', 1)

            df.at[index, 'street'] = street.strip()
            df.at[index, 'municipality'] = municipality.strip()
    # Column origin should be all upper letter
    df['origin'] = df['origin'].str.upper()
    df = df.replace('nan', '')
    df = df[['artist', 'further_artists', 'origin', 'year', 'street', 'municipality', 'auftraggeber', 'principal',
             'event_de', 'event_en', 'url_object', 'img_url_345', 'img_url_470', 'img_url_650', 'img_url_750',
             'img_url_850', 'img_url_1000', 'img_url_1200', 'img_url_1400', 'img_url_1600', 'img_url_2400',
             'img_url_2600', 'img_url_2800', 'img_url_3000']]

    # Export to Excel
    path_export = os.path.join(credentials.data_path, 'export', 'artsuebli_urbanart.xlsx')
    df.to_excel(path_export, index=False)


def capitalize_except_abbreviations(text):
    words = text.split()
    new_words = []
    for word in words:
        # Consider words with 4 or fewer alphabetic characters as abbreviations
        if len(word) <= 4:
            new_word = word.upper()
            new_words.append(new_word)
        else:
            # Capitalize in a way that first letter is uppercase and rest are lowercase
            new_word = word.capitalize()
            new_words.append(new_word)
    return ' '.join(new_words)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
