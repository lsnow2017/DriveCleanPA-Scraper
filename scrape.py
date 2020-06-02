#!/usr/bin/python3

import camelot
import pandas as pd
from bs4 import BeautifulSoup
import requests
import sys
import argparse
import re
import os
import errno
from argparse import ArgumentParser


VERSION = '0.0.1'
ROOT_DRIVECLEANPA_WEB_URL = 'http://www.drivecleanpa.state.pa.us/'
ROOT_FOLDER_NAME = 'pdf_sources'
DEFAULT_OUTPUT_FILENAME = 'output.csv'


def parse_page(url):
    r = requests.get(url)
    if r.status_code != 200:
        print("Error openning page " + url)
        sys.exit(1)

    return BeautifulSoup(r.text)


def get_list_of_regions():
    page = parse_page(ROOT_DRIVECLEANPA_WEB_URL)
    pattern = re.compile(r'^Regional Information')
    sidebar = page.find(lambda tag: tag.name ==
                        "span" and "Regional Information" in tag.text)
    children = sidebar.findChildren("a", recursive=False)
    regions = []

    for child in children:
        region = {}
        region['InfoFile'] = child['href']
        region['Name'] = ' '.join(child.find("u").get_text().split())
        regions.append(region)

    return regions


def download_pdf(folder_name, pdf_link):
    try:
        os.makedirs(ROOT_FOLDER_NAME + '/' + folder_name)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    pdf_name = ROOT_FOLDER_NAME + '/' + folder_name + '/' + pdf_link
    r = requests.get(ROOT_DRIVECLEANPA_WEB_URL +
                     'stations/' + pdf_link, allow_redirects=True)
    open(pdf_name, 'wb').write(r.content)

    return pdf_name


def download_region_pdfs(region):
    print('Downloading pdfs for region: ' + region['Name'])

    try:
        os.makedirs(ROOT_FOLDER_NAME)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    info_page = parse_page(ROOT_DRIVECLEANPA_WEB_URL + region['InfoFile'])
    list_link = info_page.find("a", string=re.compile(r'^List'))['href']
    list_page = parse_page(ROOT_DRIVECLEANPA_WEB_URL + list_link)
    pdf_link_elements = list_page.find_all("a", string=re.compile(r'PDF$'))
    pdf_files = []

    for element in pdf_link_elements:
        pdf_link = element['href']
        pdf_name = download_pdf(region['Name'], pdf_link)
        pdf_files.append(pdf_name)
        print('Downloaded: ' + pdf_name)

    return pdf_files


def download_all_region_pdfs():
    print('Downloading pdfs for all regions...')
    regions = get_list_of_regions()

    for region in regions:
        pdfs = download_region_pdfs(region)


def concatenate_all_pdfs():
    print('Concatenating all pdfs')

    subfolders = [f.path for f in os.scandir(ROOT_FOLDER_NAME) if f.is_dir()]
    master_dataframe = pd.DataFrame()

    for subfolder in subfolders:
        region_name = subfolder[subfolder.rindex('/')+1:]
        print('Concatenating pdfs in region folder ' + region_name)
        for root, dirs, files in os.walk(subfolder, topdown=False):
            for name in files:
                pdf = os.path.join(root, name)
                tables = camelot.read_pdf(pdf, pages='1-end')
                for table in tables:
                    table_df = table.df
                    table_df['Region'] = region_name
                    master_dataframe = pd.concat([master_dataframe, table_df])
                print('Concatenated ' + pdf)

    return master_dataframe


def main():
    print('Starting DriveCleanPA Station Scraper v' + VERSION)
    p = ArgumentParser()
    p.add_argument('-f', '--force', dest="force", action='store_true',
                   help="Force redownload of all PDFs (useful if previous execution was cancelled mid-download)")
    p.add_argument('-o', '--output', dest='output',
                   help="Output csv file name")
    args = p.parse_args(sys.argv[1:])

    created_root_folder = False
    try:
        os.makedirs(ROOT_FOLDER_NAME)
        created_root_folder = True
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    # if args.command and args.command == 'help':
    #     p.print_help()
    #     sys.exit(0)

    if created_root_folder or args.force:
        download_all_region_pdfs()

    df = concatenate_all_pdfs()

    output_file = DEFAULT_OUTPUT_FILENAME
    if args.output is not None:
        output_file = args.output

    df.to_csv(output_file)
    print('Output saved to ' + output_file)


if __name__ == "__main__":
    main()
