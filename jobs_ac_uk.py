import pandas as pd
import requests     # for retrieving ID web pages
from bs4 import BeautifulSoup
from geosky import geo_plug

def clean_text(soup_element):
    cleaned = soup_element.text
    cleaned = cleaned.strip().replace('\n','')
    return cleaned

def salary_clean(salary_text):
    clean_salary = clean_text(salary_text)
    clean_salary = clean_salary.replace('Salary:','').replace('From ', '')
    
    return clean_salary

def make_url(prefix_url,term_list,suffix):
    term_str = '+'.join(term_list)
    url = prefix_url + term_str + suffix
    return url

def make_url_pages_list(prefix_url, term_list, start_val = 1, page_size = 10, end_val = 100):
    
    results_intervals = list(range(start_val, end_val, page_size))
    url_list = []
    
    for i in range(len(results_intervals) - 1):
        start_interval = results_intervals[i]
        suffix = f'&sortOrder=0&pageSize={page_size}&startIndex={start_interval}'
        url = make_url(prefix_url, term_list, suffix)
        url_list.append(url)
    
    return url_list

def exclude_irrelevant(columns, key_term, dataframe):
    df = dataframe.copy()
    clean_df_list = []
    
    for col in columns:
        clean_df = df[df[col].str.lower().str.contains(key_term.lower())]
        clean_df_list.append(clean_df)
    
    clean_df = pd.concat(clean_df_list, axis = 0).drop_duplicates()

    return clean_df

def exclude_specific(columns, exclusion_term, dataframe):
    df = dataframe.copy()
    for col in columns:
        df = df[~df[col].str.lower().str.contains(exclusion_term.lower())]
    return df

def filter_by_country(filter_col, dataframe, country):
    city_list = geo_plug.all_State_CityNames(country).split('"')[3::2]
    city_list = [city.lower() for city in city_list]
    filter_df = dataframe[dataframe[filter_col].apply(lambda r: any([city in r.lower() for city in city_list]))]
    return filter_df

def get_uk(loc_column, dataframe):
    cities_list = []
    uk_states = ['Scotland', 'England', 'Wales', 'Northern Ireland']
    for state in uk_states:
        cities_list = cities_list + geo_plug.all_State_CityNames(state).split('"')[3::2]
    
    cities_list = [city.lower() for city in cities_list]
    filter_df = dataframe[dataframe[loc_column].apply(lambda r: any([city in r.lower() for city in cities_list]))]
    return filter_df

def results_over_pages(search_terms, prefix = 'https://rest.jobs.ac.uk/search/?keywords='):
    url_list = make_url_pages_list(prefix, search_terms)

    results = []
    for url in url_list:
        try:
            site = requests.get(url)   # retrieve page with fasta
            soup = BeautifulSoup(site.content, 'html.parser')
            job_results = soup.find_all("div",class_="j-search-result__text")

            for job_element in job_results:
                title_element = job_element.find("a")
                
                job_page_link = 'https://www.jobs.ac.uk'+ str(title_element).split('"')[1]

                department_element = job_element.find("div", class_="j-search-result__department")
                employer_element = job_element.find("div", class_="j-search-result__employer")
                earnings_element = job_element.find(class_="j-search-result__info")

                location_element = job_element.find_all('div')
                location_element = [clean_text(loc) for loc in location_element]
                location_element = [loc.replace('Locations:','').strip() for loc in location_element if 'Locations:' in loc][0]

                closing_date = job_element.parent.find('span', class_="j-search-result__date-span j-search-result__date--blue")

                result_list = {'job_title':clean_text(title_element), 'Department' : clean_text(department_element), 
                                'Employer' : clean_text(employer_element),'Location': location_element, 
                                'Salary': salary_clean(earnings_element), 'Closing':clean_text(closing_date), 'Job_link' : job_page_link}
                results.append(result_list)
                
        except:
            break

    df = pd.DataFrame(results).drop_duplicates()
    return df