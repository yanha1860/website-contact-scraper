# -*- coding: utf-8 -*-

import re
import xlwt
import xlrd
import traceback
import requests
import os
import warnings

from urllib.parse import urlparse
from bs4 import BeautifulSoup
from urllib.request import urlopen
from concurrent.futures import ThreadPoolExecutor, wait

def _get_soup(url) -> BeautifulSoup:
    """
    Download html content by url,
    If a file with the same name exists locally, use the downloaded file to reduce the number of network IO
    """
    
    page_content = None;
    page_content_file_name = url.replace('://', '-').replace('/', '_');
    if os.path.exists('output/' + page_content_file_name):
        page_content = open('output/' + page_content_file_name, "r", encoding='utf-8').read();
    else:
        try:
            page = requests.get(url, verify=False, timeout = 10);
            page_content = page.text;
        except Exception as e:
            page_content = str(urlopen(url, timeout = 10));
        
        if len(page_content.strip()) > 0:
            page_content_file = open('output/' + page_content_file_name, 'w', encoding='utf-8');
            page_content_file.write(page_content);
            page_content_file.flush();
        
    soup = BeautifulSoup(page_content, 'html.parser', from_encoding='UTF-8', exclude_encodings='UTF-8');
    
    return soup;
    
def _dowload_html_text(url):
    soup = _get_soup(url);
    html = str(soup);
    
    return html;    

def _scrape_next_urls(url) -> set():
    soup = _get_soup(url);
    next_urls = set();
    base_url = '';
    if '/' in url[-1]:
        base_url = url[0:len(url) - 1];
    else:
        base_url = url;
    domain = urlparse(url).netloc;
    links = soup.findAll("a");
    for link in links:
        try :
            link_href = link.attrs['href'];
            if len(link_href) <= 1 :
                continue; 
            if 'http' in link_href and domain not in link_href:  
                continue;
            if ('contact' not in link_href 
                    and 'about' not in link_href):
                continue;
            elif 'http' in link_href and domain in link_href:
                next_urls.add(link_href);
            elif 'http' not in link_href and '/' in link_href:
                next_urls.add(base_url + link_href);
            
        except Exception as e:
            pass;
    
    return next_urls;
        
def _scrape_html_emails(html):
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,3}",html)
    if len(emails) == 0:
        emails = re.findall('''[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?''', html, flags=re.IGNORECASE)
    if len(emails) == 0:
        emails = re.findall(
            r"""(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"""
        ,html)
          
    return emails;

def _scrape_html_phones(html):
    phones = re.findall(r"((?:\d{3}|\(\d{3}\))?(?:\s|-|\.)?\d{3}(?:\s|-|\.)\d{4})",html)
    
    return phones;

def _do_scrape(data_index):
    _show_thread_state();
    
    data = datas[data_index];
    scraped_emails = set();
    scraped_phones = set();
    
    try :
        website = data['website'];
        html = _dowload_html_text(website);
        # scrape index page emails
        emails = _scrape_html_emails(html);
        scraped_emails = scraped_emails.union(set(emails));
        # scrape index page phones
        phones = _scrape_html_phones(html);
        scraped_phones = scraped_phones.union(set(phones));
        # scrape secondary page 
        next_urls = _scrape_next_urls(website);
        for next_url in next_urls:
            try:
                next_html = _dowload_html_text(next_url);
                next_emails = _scrape_html_emails(next_html);
                next_phone = _scrape_html_phones(next_html);
                if len(next_emails) > 0:
                    scraped_emails = scraped_emails.union(set(next_emails));
                if len(next_phone) > 0:
                    scraped_phones = scraped_phones.union(set(next_phone));
                
            except Exception as e:
                traceback.print_exc(limit=1);
                print('next_url : %s' % next_url);

    except Exception as e:
        traceback.print_exc(limit=1);
        print('data_index : %s' % data_index);
    
    data['emails'] = scraped_emails;
    data['phones'] = scraped_phones;
    datas[data_index] = data;

def _datas_filter():
    for data in datas:
        filtered_emails = set();
        # filter emails
        for email in data['emails']:
            # filter special characters
            # e.g: u003epointman26@hotmail.com -> pointman26@hotmail.com
            # e.g: //cdn.jsdelivr.net/npm/bootstrap@4.6.0 -> none
            email = email.replace('u003e', '').replace('%20', '').lower();
            is_do_filte = False;
            for filter_email_char in FILTER_EMAIL_CHARS:
                if filter_email_char.lower() in email:
                    is_do_filte = True;
            
            if not is_do_filte:
                filtered_emails.add(email);
          
        # filter phones 
        filtered_phones = set();  
        for phone in data['phones']:
            # filter special characters
            phone = phone.replace('-', '');
            is_do_filte = False;
            for filter_phone_char in FILTER_PHONE_CHARS:
                if filter_phone_char in phone:
                    is_do_filte = True;
            
            if not is_do_filte:
                filtered_phones.add(phone);
        
        data['emails'] = filtered_emails;
        data['phones'] = filtered_phones;

def _show_thread_state():
    print('--------work queue size : %s-------------' % multi_executor._work_queue.qsize());

def _extract_excel_data() -> list(dict()):
    workbook = xlrd.open_workbook(intput_file_path);
    sheet = workbook.sheet_by_name(workbook.sheet_names()[0]);
    excel_datas = list(dict());
    keys = list();
    for row_index in range(0, sheet.nrows):
        row_data = dict();
        for col_index in range(0, sheet.ncols):
            value = sheet.cell_value(row_index, col_index);
            if row_index == 0:
                keys.append(value);
            else:
                row_data[keys[col_index]] = value;
        
        if len(row_data) > 0:
            excel_datas.append(row_data);          
    return excel_datas;           

def _write_data_to_excel():
    wb = xlwt.Workbook();
    ws = wb.add_sheet('default');
    # write keys
    row_index = 0;
    col_index = 0;
    keys = datas[0].keys();
    for key in keys:
        ws.write(row_index, col_index, key);
        col_index += 1;
    row_index += 1;
    # write datas
    for data in datas: 
        col_index = 0;
        for key in keys:
            col_value = data[key];
            if isinstance(col_value, set):
                col_value = ','.join(col_value);
            ws.write(row_index, col_index, col_value);
            col_index += 1;
        row_index += 1;
    # save to file
    wb.save(out_put_file_path);
        
# configs
FILTER_EMAIL_CHARS = ["@yourdomain.com", "@mail.com", "@company.com", "@email.com", "@domain.com", 
                        "@example.com", "$", ".pdf", ".htm", ".svg", ".hei",
                        ".jpe", "^", "{", "@sentry", ".web", "/", ".gif", ".jpeg", 
                        "+", ".js", ".css", ".png", ".jpg", "=", ".1", "\"", "sentry.io", 
                        "@3x.web", "@2x.web", "@error-tracking", "@errors"];
FILTER_PHONE_CHARS = ["."];
# ignore ssl prompt
warnings.filterwarnings("ignore");
# excel file path
intput_file_path = r"data/websites.xls";
out_put_file_path = r'data/websites_out.xls';
# extract excel data to memory
datas = _extract_excel_data();
# Multi-threaded parallel processing to improve crawling performance
multi_executor = ThreadPoolExecutor(max_workers=2);
all_task = [multi_executor.submit(_do_scrape, data_index, ) for data_index in range(0, len(datas))];
wait(all_task);
# filter data
_datas_filter();
# write scraped data to file
_write_data_to_excel();
