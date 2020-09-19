# Author: HZHtat
# Date: Jan-2020
# Version 1.0
'''
Utility functions to scrape/parse data from a website
'''

from bs4 import BeautifulSoup
import html5lib
import requests

def narrow_down(obj=None, tagOrAttr=None, identifier=None):
    '''
    Pass in the url you are intending to scrape or the bs4.BeautifulSoup object
    You need to know the structure of the webpage and contents you're after.
    Required params:
    * obj - either the URL or the returned BeautifulSoup object
    * tagOrAttr - HTML class name or ID name to uniquely identify needed tags.
    * identifier - primary search criteria e.g. div, table, p, section etc.
    Returns instance of bs4.BeautifulSoup - in order to chain funciton calls.
    '''
    if type(obj) == str: # url
        obj = BeautifulSoup(requests.get(obj).content, 'html5lib') # return raw

    if identifier != None: # class or ID 
        return obj.findAll(attrs = {tagOrAttr: identifier})
    else: # pure html tag
        return obj.findAll(name = tagOrAttr)

def strMth_to_numMth(data): # turns string format to number of month
    months = {
        'January': '01', 'Feburary': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12'
        }
    if data in months:
        return months[data]
    else:
        return data

def df(obj, sep, x): # x is index
    '''
    General tailorised function - an oxymoron - df (date formatter)
    obj arg is bs4.BeautifulSoup object
    sep arg is the separator character
    index arg is the index of the date string on the page (as per the obj)
    Turns 'Monday 28 Jan 2020' date format to 'YYYY-MM-DD'
    '''
    return sep.join(
        [strMth_to_numMth(i) for i in (obj[x].text.strip().split()[1:][::-1])]
        )
    
def termDates(obj):
    '''
    Returns a key/value dictionary of: 'Term1': [startDate, endDate], etc...
    '''
    container = dict()
    container['term1'] = [df(obj, '-', 4)]
    container['term1'].append(df(obj, '-', 5))
    container['autumnSH'] = [df(obj, '-', 7)]
    container['autumnSH'].append(df(obj, '-', 8))
    container['term2'] = [df(obj, '-', 10)]
    container['term2'].append(df(obj, '-', 11))
    container['winterSH'] = [df(obj, '-', 13)]
    container['winterSH'].append(df(obj, '-', 14))
    container['term3'] = [df(obj, '-', 16)]
    container['term3'].append(df(obj, '-', 17))
    container['springSH'] = [df(obj, '-', 19)]
    container['springSH'].append(df(obj, '-', 20))
    container['term4'] = [df(obj, '-', 22)]
    container['term4'].append(df(obj, '-', 23))
    return container

# mainline
u='https://www.aquaticcentre.com.au/Aquatic-Programs/Calendar-and-Office-Hours'
parse1 = narrow_down(obj=url, tagOrAttr='class', identifier='two-column-table')
parse2 = narrow_down(obj=parse1[0], tagOrAttr='td') # <td>
dates = termDates(obj=parse2)
