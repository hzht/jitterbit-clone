# Author: HZHtat
# Date: Aug-2019
# Version 0.6

import sys
import os
import shutil
import glob
import random
import pyodbc
import csv
from datetime import datetime as dt
from datetime import date as ymd
from dateutil import relativedelta as reldelt
from simple_salesforce import Salesforce
import emailalert

# Globals - set appropriate details
linksDBsvr = ''  # DB server name
linksDB = ''  # DB instance name
linksDBUName = ''  # DB username
linksDBUNamePw = ''  # DB pw
uName = ''  # Active Directory username - used for transform file creations
uPw = ''  # Active Directory pw
sfUname = ''  # Salesforce uname
sfPW = ''  # Salesforce pw
sfToken = ''  # Salesforce token

# Utility Functions (mostly SAP)


def sf_connection_obj(sfUname, sfPW, sfToken, test=False):
    '''
    Creates connectivity to either production or test / dev environment.
    Takes one arg 'test' either True or False. If true will connect to
    Development/sandbox. False will connect to production environment.

    Function returns the connectivity object that can be used for bulk
    upserts, queries and deletes of Salesforce records.
    '''
    if test == True:
        sf = Salesforce(username=sfUname, password=sfPW,
                        security_token=sfToken, domain='test')
        return sf
    else:
        sf = Salesforce(username=sfUname, password=sfPW,  # domain = None?
                        security_token=sfToken)
        return sf


def errorLog(p=None, **d):  # d is details
    '''
    Called when exception is raised. Appends to error.txt log file in
    sub directory to where the Python script runs. Ensure you create
    error_logs sub directory.

    Typical call errorLog(p='point: A', err=sys.exc_info(), m=mode ...)
    '''
    with open(r'.\error_logs\error.txt', 'a+') as f:
        f.write(p + '\n')
        f.write(dt.today().strftime('%Y-%m-%d-%H:%M:%S') + '\n')
        for i in d:
            f.write(str(i) + ':' + str(d[i]) + '\n')
        f.write('\n' * 3)


def hhmmss_to_secs(hhmmss):
    '''
    Converts hhmmss time format to equivalent seconds totality. E.g. SAP
    reports in the format of CSV contain time column in following format:
    hhmmss e.g. 164510. This function converts hhmmss into seconds so as to
    be load ready. I.e. Salesforce will only take integer format of time in
    seconds.

    This function is typically called by a transformation function e.g.
    transformCSV.
    '''
    h = hhmmss[:2]
    m = hhmmss[2:4]
    s = hhmmss[4:]

    return (int(h) * 3600 + int(m) * 60 + int(s)) * 1000  # 000 for ms


def mapSourceDestination(mode, source=None, target=None, user=None, pw=None,
                         emailPackage=None):
    '''
    Map / unmap network drives. This is a pre-step for copying of files to
    and from different network locations. This function only maps or unmaps
    network drives (source or staging locations) - nothing else.

    Example - SAP generates CSV reports on server X. Transformation
    of CSV fields and data occur to copied version of original CSV report
    in a 'staging area' i.e. server Y. Thus share on server X and server Y
    need to be mapped and unmapped.

    Available modes:

    'map_all' - will map both source and staging network shares.

    'unmap_all' - unmaps both source and staging network shares.

    'map_staging' - will only map staging network share.

    'unmap_staging' - will only unmap staging network share.
    '''
    try:
        if mode == 'map_all':
            # first map Y to the share where SAP report is located
            os.system(
                r'NET USE Y: {source} {pw} /USER:{user} /persistent:No'.format(
                    source=source, pw=pw, user=user))
            # second map Z to the staging directory
            os.system(
                r'NET USE Q: {target} {pw} /USER:{user} /persistent:No'.format(
                    target=target, pw=pw, user=user))
        elif mode == 'unmap_all':  # unmap both
            os.system(r'NET USE Y: /DELETE')
            os.system(r'NET USE Q: /DELETE')
        elif mode == 'map_staging':  # only destination
            os.system(
                r'NET USE Q: {target} {pw} /USER:{user} /persistent:No'.format(
                    target=target, pw=pw, user=user))
        elif mode == 'unmap_staging':
            os.system(r'NET USE Q: /DELETE')
    except Exception:
        if emailPackage:  # not None
            emailalert.alerter(emailPackage, mode='err', to='prim',
                               body='Error @ Point: A')
        errorLog(p='Point: A', mode=mode, error=str(sys.exc_info()))


def lastModifiedFile(src, extension=None, source=None, target=None, user=None,
                     pw=None, emailPackage=None):
    '''
    Finds the most recent modified file. In a scenario where a system such
    as SAP generates reports, function will identify the last created report
    file. After identifying the relevant file, will copy the file to staging
    network share for further manipulation / transformation.

    Function returns the filename of copied file to be used for further
    transformations or load.

    Takes arguments:

    'src' - takes string value of filename with wildcard e.g.
    'CARPARKSALES_DEV_*.xls'

    'extension' - either None or a string value. None by default will copy
    identified file to destination/staging area. Passing a string value of
    'csv' will change extension of copied file to .csv

    'source', 'target', 'user', 'pw' - these are to call mapSourceDestination
    '''
    targetFile = ''
    lastMod = {}  # a list of all files with similar name and file extension
    tformat = '%Y-%m-%d %H:%M:%S'
    sortedLastMod = []  # date/time of all files in lastMod dictionary

    mapSourceDestination('map_all', source=source, target=target, user=user,
                         pw=pw)  # oh well
    try:
        for file in glob.glob('Y:' + src):  # create list of all files with *
            lastMod[file] = os.path.getctime(file)

        for i in lastMod:
            sortedLastMod.append(dt.fromtimestamp(lastMod[i]).
                                 strftime(tformat))
    except Exception:
        if emailPackage:  # not None
            emailalert.alerter(emailPackage, mode='err', to='prim',
                               body='Error @ Point: B')
        errorLog(p='Point: B', source=src, fileExt=extension,
                 error=str(sys.exc_info()))

    sortedLastMod.sort()  # ascending i.e. last entry is newest date/time

    try:
        for j in lastMod:  # search & assign last modified file to targetFile
            if dt.fromtimestamp(lastMod[j]).strftime(tformat) == sortedLastMod[-1]:
                targetFile = j  # last in ascending order
        targetFile  # y:lastmodedfile.xls

        if extension == None:
            shutil.copy(targetFile, r'Q:' + targetFile)  # copy
        else:  # change extension
            shutil.copy(targetFile, r'Q:' + targetFile[2:-3] + extension)
    except Exception:
        if emailPackage:  # not None
            emailalert.alerter(emailPackage, mode='err', to='prim',
                               body='Error @ Point: C')
        errorLog(p='Point: C', source=src, fileExt=extension,
                 targFile=targetFile, error=str(sys.exc_info()))

    mapSourceDestination('unmap_all')

    if extension == None:
        return targetFile  # to be passed into preupload_prep
    else:  # change extension
        return targetFile[2:-3] + extension


def query_sf_custom(sfConn, soql_string, returnKey, purpose=None,
                    emailPackage=None, *args):
    '''
    Generalised query function. Cringe, but hey! E.g.
    query_sf_custom(sfConn, "SELECT Id, {0} from {1} WHERE {0} LIKE '{2}'",
            'Name', 'Name', 'Opportunity', 'PK2019%')
    OR

    query_sf_custom(sfConn, "SELECT Id, {0} FROM {1} WHERE {0} IN ({2})",
            'Email', 'Email', 'Contact', strung_emails)

    Where strung_emails references return of CSV_query function.

    Returns a dictionary of returnKey: Id mapping. E.g. pass in 'Email'
    or 'Name' when calling function.

    Function is typically used to query and get either email to SFID or
    Opportunity Name to opportunity ID mapping. This can then be used for
    either deleting in bulk or tacking onto CSV rows the SFIDs for each email.

    Better to just use query_salesforce as there are only two primary uses
    for querying Salesforce, i.e. to get Email to SFID pairs or to get
    Name to OpportunityId pairs.
    '''

    pairings = {}
    bulk_del = []

    arg_list = ['arg' + str(x) for x in range(len(args))]

    evaluate = 'query = sfConn.query(soql_string.format('

    for i in arg_list:
        evaluate += i + ', '

    evaluate[:-2] += '))'  # cap it off

    try:
        eval(evaluate)  # very very naughty! tsk tsk!

        size = query['totalSize']

        for j in range(size):
            pairings[query['records'][j][returnKey]] = query[
                'records'][j]['Id']  # e.g. returnKey = 'SFID'
            # e.g. {Email: 'SFID', n} OR {Name: 'OpportunityID'}

        if purpose == 'bulk_delete':  # query return to be used for bulk delete
            for k in pairings:
                bulk_del.append({'Id': pairings[k]})  # format Sf API consumes
            return bulk_del
        else:
            return pairings

    except Exception:
        if emailPackage:  # not None
            emailalert.alerter(emailPackage, mode='err', to='prim',
                               body='Error @ Point: D')
        errorLog(p='Point: D', soql_string=soql_string, return_key=return_key,
                 extension=extension, purpose=purpose, evaluate=evaluate,
                 error=str(sys.exc_info()))


def query_salesforce(sfConn, sObject, sObjectField, array=None, wCard=None,
                     purpose=None, switch=None, emailPackage=None):
    '''
    update 13 feb 2020 - this too needs to be modified. nope! all good! 
    update - 22 september - find out difference between bulk.query and normal
    query docstring needs to be fleshed out. perhaps add both?

    Used to query Salesforce. Two modes. First mode 'Contact' will to return
    Email to SFID pairs. This is needed for Opportunity upserts as
    Opportunities need to be upserted to SF using SFID as primary ID.

    Mode 'Contact' returns a dictionary in the following format:
    {'c@d.com': 'SFID20481AQaty20a', 'a@b.net': 'SFID427Sqhsyy', ...}

    Mode 'Opportunity' returns a dictionary in the following format:
    opportunity id value e.g. {'PK2019': 'A00000120313', ...}

    Mode is specified by the sObject arg, either 'Contact' or 'Opportunity'.

    sObjectField arg is either 'Email' or 'Name' - later is for
    Opportunities. Former for Contacts.

    array is typically the string of comma delimited emails generated from
    CSV_query function.

    wCard arg is required in 'Opportunity' mode, it is a string such as
    'PK2019%'.

    purpose - either None (default) or 'bulk_delete'. Changes data type
    and format of function return.

    switch - when True, the return will be a dictionary in 'Id': 'Email' or
    'Id': 'Name' pairs. Default is None, which will return 'Email': 'Id' or
    'Name': 'Id'. Why use this switch? On occasion you will need to get
    OpportunityIDs back for the multitude of opportunities that my infact have
    the same Name! So useful in Opportunity mode.

    Thus when function is called with 'Contact' as sObject the following SOQL
    runs:

    "SELECT Id, Email FROM Contact WHERE Email IN (comma delimit email string)"

    This can be assigned to a variable and be used to call transformCSV
    function in 'tack_sfid' mode to find in the transformed SAP dump file the
    corresponding Email of each row, finding a match it will then tack on as
    last column the related SFID.
    '''

    pairings = {}  # i am but a vessel
    bulk_del = []  # as am i

    try:
        if sObject == 'Contact':
            if type(array) == list:  # list_of_strs
                while len(array) != 0:
                    array_batch = array.pop()
                    qString = "SELECT Id, {1} FROM {0} WHERE {1} IN ({2})".format(
                        sObject, sObjectField, array_batch
                    )
                    query = sfConn.query(qString)  # < max_size of CSV_query

                    size = query['totalSize']  # number of pairs returned

                    for i in range(size):  # build it!
                        if switch == True:
                            pairings[query['records'][i]['Id']] = query[
                                'records'][i][sObjectField]
                        else:  # switch=None
                            pairings[query['records'][i][sObjectField]] = query[
                                'records'][i]['Id']  # e.g. {Email = 'SFID'}

        elif sObject == 'Opportunity':  # 'mode' - update 07022020 need to cater
          # for large queryset like 'contact' mode - not needed for the moment as
          # there is never a need to query for opportunities
            qString = "SELECT Id, {1}, FROM {0} WHERE {1} LIKE '{2}'".format(
                sObject, sObjectField, wCard
            )
            query = sfConn.query(qString)

            size = query['totalSize']

            for i in range(size):
                if switch == True:
                    pairings[query['records'][i]['Id']] = query[
                        'records'][i][sObjectField]
                else:  # switch=None
                    pairings[query['records'][i][sObjectField]] = query[
                        'records'][i]['Id']  # e.g. {Name = 'OpportunityId'}

        if purpose == 'bulk_delete':  # return to be used for bulk delete
            for j in pairings:
                bulk_del.append({'Id': pairings[j]})  # format API consumes
            return bulk_del
        else:
            return pairings
    except Exception:
        if emailPackage:  # not None
            emailalert.alerter(emailPackage, 'Error @ Point: Q')
        errorLog(p='Point: Q', source=sObject, qString=qString,
                 sObjectField=sObjectField, array=array, wCard=wCard,
                 purpose=purpose, switch=switch, querySize=size,
                 error=str(sys.exc_info()))


def looper(file, a_list):
    '''
    Writes contents of a Python list to a file.

    Takes two args. 'file' arg takes an open file in 'w' mode. Whereas 'a_list'
    arg takes a Python list.

    Loop will pop first item in the list and write it to file, until no more
    left to pop.

    Nothing to return. Used only in conjunction the various modes of
    transformCSV function.
    '''
    while len(a_list) != 0:
        file.write(str(a_list.pop(0)).strip())  # white spaces
        if len(a_list) == 0:
            file.write('\n')
            break
        else:
            file.write(',')


def transformCSV(mode, inFile, col=None, origTrue=None, origFalse=None,
                 newTrue=None, newFalse=None, fromX=None, toY=None, match=None,
                 mapping=None, source=None, target=None, user=None, pw=None,
                 emailPackage=None, colLength=None, purgeUniqueId=None):
    '''
    Function takes text/csv file passed through inFile arg, then transforms
    the file as per the selected mode, saving a new text/csv file with an
    appended random number postfix to original filename. Function then returns
    the newly generated text/csv file's name as a string to be used in further
    transformations etc.

    All modes except 'remove_header' require passing of args: mode (obviously)
    & inFile in addition to specific args based on mode selected.

    Specific modes as follows:

    'boolify' - changes specified inFile's col index from origTrue & origFalse
    vals to newTrue and newFalse vals. Requires passing of args:
        - col, origTrue, origFalse, newTrue (string), newFalse (string)

    'remove_header' - removes the first row in csv file. No args except mode &
    inFile required.

    'swap_columns' - swaps fromX with toY, requires passing of args:
        fromX, toY.

    'delete_column' - deletes a specified column, requires passing of arg: col

    'de-duplicate' - removes all duplicate rows based on value found in the
    particular value in the column of the row as identified by the col arg.
    Requires args: col. E.g. if there are 5 rows with X present in the 3 rows
    for the column index passed into 'col' arg, then those 3 rows will be
    collapsed into one row (the first row that has the value X in the specified
    column).

    'tack_sfid' - adds SFID as last column in CSV file - this function & mode
    to be used after calling query_salesforce. Requires arg: mapping - return
    value from query_salesforce i.e. a dictionary of email keys to SFID values.

    Assumption of 'tack_sfid' mode is that all emails will have corresponding
    SFID on the Contact records on Salesforce. Hence this mode is only run
    after upserting and re-downloading the contacts objects from original CSV.

    'tack_date_based_on_condition' - adds date based on value of a specified
    field. Used primarily with Health Club and Swim School whereby no expiry
    date is set for a given enrolment. col - specify the column to check -
    takes integer index value. match arg to specify value of the col value to
    find and if found take appropriate action on i.e. date to tack on to the
    end of the row. mapping arg integer value of years ahead based on
    conditional being true e.g. if col x value is as per that which is
    specified in value arg, tack on today plus increment of years specified in
    mapping. Else tack on value as specified in col. 

    'yyyymmdd_to_yyyy-mm-dd' - changes yyyymmdd to yyyy-mm-dd. This is the
    format Salesforce consumes. Requires args: col, pass in a tuple or list of
    column integers that have dates you want to convert. Even if only one
    column is to be converted, pass it in as a tuple or list e.g. col=(3,)

    'concat_n_tack' - an opportunity needs a name that is a string of
    concatenated field values for the particular record (row). Requires col
    arg. Pass a list of strings and or integers in the order in which the
    column indexes and or strings are to be concatenated.

    E.g. transformCSV('concat_n_tack', f, col=['AQ SS',1,3,5])

    This will tack on values from col 1, 3 and 5 to the string 'AQ SS' to
    the end of the row (record). Find out what columns hold what values first.

    'convert_time' - changes hhmmss (SAP report format) to equivalent seconds.
    Requires arg: col - column index containing the hhmmss time data. Returns
    new CSV file with converted time format that can be used with Salesforce.

    'tack_custom_val' - Opportunity records, unlike Contact records require
    'RecordTypeId' value when uploading. This will ensures Salesforce
    categorises the different types of opportunities properly, i.e. based on
    this unique ID (RecordTypeId). To get this log into Salesforce:
    > go to Object Manager
    > Opportunity
    > Record Types
    > select the opportunity e.g. Parking
    > copy the URL, in it is the RecordTypeId you need e.g. 0127F000001HyMzQAK
    ...RecordTypes/0127F000001HyMzQAK/view 

    Easiest way to find out is to log into Salesforce and obtain an Opportunity
    type's RecordTypeId. This can then be used to tack onto the rows of the CSV
    file.

    This mode 'tack_custom_val' is typically used to add RecordTypeId to rows
    of a CSV file. Nothing fancy - just bulk adding of a particular value as
    another column at the end of the row.

    Required args: mapping - how to obtain this for a given opportunity?
    Check on Salesforce for the given Opportunity's 'RecordTypeId' - it is
    unique to the Opportunity. Pass string value e.g. '1117F000001HAxzQAK'
    This value will then be tacked onto each row.

    'remove_missing_cols' - best to run this prior to preupload_prep function.
    Will remove any blank columns ,'', or ,'\n',. This will ensure there
    aren't any index errors when running preupload_prep function as the
    mappings inside that function are static, set and forget.

    This mode does not require any args apart from 'inFile'.

    Update - 9 March 2020 - this is no longer required as preupload_prep will
    just select the necessary fields, regardless of the holes inherent in the
    CSV file. 

    'de_dupe_remove_old_dates' - used primarily with Links extracts, in
    particular health club. Takes 3 args. 'col' - specify the column to check
    for identical values e.g. index of LinksID column. 'target' - specify the
    column to check for date values e.g. index of Date Started column. Logic as
    follows: 
    1. checks 'col' for duplicates. Duplicate values are inserted into dupes 
    list for further processing. Non duplicate rows are inserted into dedupes
    dict.
    2. with dupes collection, check the date value as per column index
    identified by 'target' arg. Remove from the dupes collection all duplicates
    except for the row with most recent date in 'target' column.

    Update - 9 March 2020 - no longer used / needed due to SQL query being
    modified to return non duplicate LinksIDs.

    'remove_row_based_on_val' - removes entire rows based on matching value
    as passed in via 'col' and 'match'. The later identifies what value to look
    for in the given col - if found to be true the row will be removed /
    omitted from being written to new file. Used mostly for 'blank' emails from
    Links. mapping arg is required - pass integer value of column/field of
    which its value will be used to send 'ter' emails - i.e. filling up
    row_errors set.

    'strip_time' - removes time from 'yyyy-mm-dd hh:mm:ss' to leave
    'yyyy-mm-dd' format. Requires args: col, pass in a tuple or list of
    column integers that have dates you want to convert. Even if only one
    column is to be converted, pass it in as a tuple or list e.g. col=(3,)

    Args 'source', 'target', 'user', 'pw' are a necessary evil - passes needed
    info for mapSourceDestination function.

    'purge' - used with Links data. Run this first and foremost with Links data
    it'll collate all the problematic rows with escape characters and multiple
    commas inside of address bar i.e. usually leading to index issues. the
    row_errors list will be then be emailed to helpdesk for fixing / notifying
    venues to edit the data accordingly on Links. Required args:

    purgeUniqueId - integer val indicating index of column that will be added
    as value to row_errors list e.g. LinksID or email address column.

    colLength - integer val indicating the number of rows each row is meant to
    have. Anything deviating from it, it's purgeUniqueId index value of the row
    will be added to row_errors.

    'join_dict_to_csv' - emulates a left join. Given input of CSV file, and
    a dictionary of key to value mappings - will tack on the dictionary to the
    CSV and spit out new file. Required args:

    mapping - the dictionary that has key to value mapping. Key being the
    primary key that will be used to 'join' to a specific column in the CSV.
    Value has to be a list of one or more primitives/objects. 

    match - integer index value on the CSV used in in the 'join' function, will
    be matched to the 'Key' of the mapping arg.

    col - integer index value to be tacked onto the CSV file from the
    dictionary value as identified by mapping arg. Note 'join_dict_to_csv' will
    only ever tack on one value / field / column per iteration through the
    function. As such multiple values to be 'joined' will need to be rerun with
    different col value set.

    E.g. tx = transformCSV(mode='join_dict_to_csv', inFile=raw_data[0],
                            mapping=sqlq, match=0, col=1)
    # sqlq = {20000001: [20017922, datetime.datetime(2020, 2, 13, 11, 22, 23)]
    # match = index of LinksID in CSV file
    # col = index of val to pull from key to value mapping as per sqlq
    '''
    randomAppend = str(random.randint(0, 99999))  # used as postfix.

    mapSourceDestination('map_staging', target=target, user=user, pw=pw)

    dedupes = dict()
    dupes = list()  # required for mode: de_dupe_split
    row_errors = set()  # collection of LinksIDs / identifier for helpdesk

    try:  # need to close at the end
        tempfile = open('Q:' + inFile[:-4] + '_' + randomAppend + '.csv', 'w')
        outFileName = inFile[:-4] + '_' + randomAppend + '.csv'  # just name
    except Exception:
        if emailPackage:  # not None
            emailalert.alerter(emailPackage, mode='err', to='prim',
                               body='Error @ Point: E')
        errorLog(p='Point: E', mode=mode, error=str(sys.exc_info()))
    with open('Q:' + inFile) as CSV:
        if mode == 'remove_header':
            CSV.readline()  # read header row - doesn't write to new file!
        elif mode == 'tack_date_based_on_condition':  # calc the date
            ddDate = str(
                dt.today().replace(year=dt.today().year + mapping).date()
            )
        for row in CSV:
            try:
                split = row.split(',')
                if mode == 'purge':  # specifically for Links dirty data
                    if len(split) != colLength:  # multiple commas inside""
                        row_errors.add(split[purgeUniqueId])  # add to list
                        continue  # skip writing it to file - not needed :|
                    else:
                        tempfile.write(row)
                elif mode == 'boolify':
                    if split[col] == origTrue:
                        split[col] = newTrue  # originally (newTrue)
                    elif split[col] == origFalse:
                        split[col] = newFalse  # originally (newFalse)
                    looper(tempfile, split)
                elif mode == 'remove_header':
                    tempfile.write(row)
                elif mode == 'swap_columns':
                    split[fromX], split[toY] = split[toY], split[fromX]
                    looper(tempfile, split)
                elif mode == 'delete_column':
                    split.pop(col)
                    looper(tempfile, split)
                elif mode == 'de-duplicate':
                    dedupes[split[col]] = split  # one row, dedupes['x@y.com']
                elif mode == 'tack_sfid':
                    sfid = ''
                    for field in split:  # add onto split list
                        # email in {'a@b.com': 'SF91941'}
                        if field.lower() in mapping:
                            sfid = mapping[field.lower()]
                            break
                    split.append(sfid)
                    for i in split:  # commit to file update 10 mar 2020 - i think this can be put into looper? looks like it'll work
                        tempfile.write(str(i).strip() + ',')
                    tempfile.write('\n')
                elif mode == 'tack_date_based_on_condition':  # used primarily for null/'' expiry dates for memberships
                    if split[col] in match:  # no date
                        split.append(ddDate)
                    else:  # has expiry date
                        split.append(split[col])
                    looper(tempfile, split)
                elif mode == 'yyyymmdd_to_yyyy-mm-dd':
                    for i in col:
                        if split[i] != '':  # always a chance there's no date!
                            ph = split[i]  # placeholder
                            split[i] = ph[:4] + '-' + ph[4:6] + '-' + ph[6:]
                    looper(tempfile, split)
                elif mode == 'concat_n_tack':
                    container = col[:]  # make a copy!
                    new_col = ''
                    while len(container) != 0:  # concatenate as per col index
                        item = container.pop(0)
                        if type(item) == str:
                            new_col += item + ' '
                        else:  # column indexes (int)
                            new_col += split[item] + ' '  # space
                    split.append(new_col)
                    looper(tempfile, split)
                elif mode == 'tack_custom_val':
                    split.append(mapping)  # 4 march 2020 - may break parking?
                    looper(tempfile, split)
                elif mode == 'convert_time':  # todo: cater for multiple col(s)
                    split[col] = hhmmss_to_secs(split[col])
                    looper(tempfile, split)
                elif mode == 'remove_missing_cols':
                    ph = [i.strip() for i in split]  # ph placeholder
                    ph = [i for i in ph if i != '']
                    looper(tempfile, ph)
                elif mode == 'remove_row_based_on_val':
                    if split[col].strip() != match:  # caters for '\n', ' ' & ''
                        tempfile.write(row)
                    else:  # dirty data email!
                        row_errors.add(split[mapping])
                elif mode == 'strip_time':
                    for i in col:
                        if split[i] != '':
                            split[i] = str(dt.strptime(
                                split[i], "%Y-%m-%d %H:%M:%S").date()
                            )
                    looper(tempfile, split)
                elif mode == 'de_dupe_remove_old_dates':
                    ...  # to be fleshed out for health club nightly
                elif mode == 'join_dict_to_csv':  # use in conjunction with loop_n_load of pull_SQL_data function
                    if split[match] in mapping:
                        split.append(str(mapping[split[match]][col]))
                        looper(tempfile, split)
            except Exception:
                if emailPackage:  # not None
                    emailalert.alerter(emailPackage, mode='err', to='prim',
                                       body='Error @ Point: F')
                errorLog(p='Point: F', mode=mode, inFile=inFile, col=col,
                         origTrue=origTrue, origFalse=origFalse,
                         newTrue=newTrue, newFalse=newFalse, fromX=fromX,
                         toY=toY, outFileName=outFileName, row=row,
                         split=split, error=str(sys.exc_info()))
    if mode == 'de-duplicate':
        for i in dedupes:
            try:
                templist = dedupes[i]
                while len(templist) != 1:
                    tempfile.write(str(templist.pop(0)).strip() + ',')
                tempfile.write(str(templist.pop()).strip() + '\n')
            except Exception:
                if emailPackage:  # not None
                    emailalert.alerter(emailPackage, mode='err', to='prim',
                                       body='Error @ Point: G')
                errorLog(p='Point: G', mode=mode, inFile=inFile, col=col,
                         outFileName=outFileName, error=str(sys.exc_info()))
    elif mode == 'de_dupe_remove_old_dates':
        ...  # to be fleshed out for health club nightly

    tempfile.close()
    mapSourceDestination('unmap_staging')  # first map destination drive

    if len(row_errors) != 0:  # some rows with mangled data!
        if emailPackage:  # not None
            row_errors = list(row_errors)
            row_errors.insert(0, 'Dirty data - skipped records:')  # 1st line
            emailalert.alerter(emailPackage, mode='info', to='sec',
                               body=''.join(
                                   [str(
                                       i) + '\n' for i in row_errors if i not in ['"', "'"]]
                               )
                               )
    return outFileName


def chunk_n_upload(mode, chunk_size, package, sfConnection,
                   primaryIDentifier=None, emailPackage=None):
    '''
    Breakup large reports/csv files into smaller chunks of chunk_size arg
    prior to initiating upload. All arguments are required.

    'mode' - either, 'Contact' or 'Opportunity', depending on what you're
    trying to upload/upsert.

    'chunk_size' - takes the maximum record size to upload/upsert per
    Salesforce API call.

    'package' - data structure holding the rows in memory. Typically this is
    an argument passed by preupload_prep function, and as such typically is
    a list of dictionaries e.g.

    [{'SF_field_name': split[2], ...}, {...}, ...]

    'sfConnection' - Salesforce connection object.

    Example call: chunk_n_upload('Contact', 500, entirePackage, sf, primaryID)
    '''
    try:  # check length & break up bulk upsert if necessary, refactor?
        while len(package) != 0:
            chunk = []  # new
            while len(chunk) < chunk_size:
                chunk.append(package.pop(0))
                if len(package) != 0:
                    continue
                else:
                    break
            if mode == 'Contact':
                if primaryIDentifier == None:  # create new records - assuming it will just update existing ones !
                    # update 4 Mar 2020 - test process, may never need this clause
                    print('here')
                    sfConnection.bulk.Contact.insert(chunk)
                    print('but not here')
                else:  # previously working condition
                    sfConnection.bulk.Contact.upsert(chunk, primaryIDentifier)
            elif mode == 'Opportunity':  # always an upsert i.e. only available contacts will get opportunities
                sfConnection.bulk.Opportunity.upsert(chunk, primaryIDentifier)
            if emailPackage:  # success
                sz = 'Upserted: ' + str(len(chunk)) + ' ' + mode + ' objects.'
                emailalert.alerter(emailPackage, mode='success', to='prim',
                                   body=sz)
            else:
                print('Upserted:', len(chunk), mode,
                      'primID:', primaryIDentifier)
    except Exception:
        if emailPackage:  # not None
            emailalert.alerter(emailPackage, mode='err', to='prim',
                               body='Error uploading to Salesforce')
        if len(package) != 0:
            errrow = package.pop()
        else:
            errrow = package
        errorLog(p='Error uploading to Salesforce', mode=mode,
                 chunk_size=chunk_size, last_item_in_package=errrow,
                 primID=primaryIDentifier, error=str(sys.exc_info()))  # only last item in package list


def stateConversion(mode, orig, new, emailPackage=None):
    '''
    Used to convert a collection of lists (usually an SQL query) to a CSV file
    OR used to read from a CSV file back into memory i.e. either a list of
    lists OR a dictionary of lists. If a dictionary of lists, then the column
    that will become the key will need to be specified.

    Used primarily to convert from CSV to a collection in memory so as to
    manipulate it further using tools in transformCollection function

    Mode: 'list_to_CSV' - will convert a list of lists (usually from
    pull_SQL_data) to a CSV file.

    Mode: 'CSV_to_list' - will convert a CSV file into a list of lists
    '''
    ...


def transformCollection(mode, source, col=None, emailPackage=None):
    '''
    To be fleshed out - the equivalent of transformCSV but in memory on
    data structure. Usually used straight after stateConversion function.
    As such most of the modes in this function are similar to that of
    transformCSV. Ideally the function would be function chainable and modular
    but as it is currently will not be as modular.

    Mandatory args: mode and source - later of which is the data collection
    that'll be manipulated. Typically in a list of list fashion straight
    from an SQL query via pull_SQL_data. source arg can also be a dictionary
    of lists as opposed to a list of lists.

    Mode: 'dictify' based on a col arg will turn source data structure
    typically a list of lists from an SQL query to a key/value pair. Typically
    used when a dictionary of lists is preferred to list of lists. Required

    Mode: 'split_dupes_nondupes' - specifically used with health club nightly
    runs to return a list of lists (of non duplicate rows with unique
    value in specified col arg) and a dictionary of lists of lists of duplicate
    rows with duplicate values in specified col arg. col arg usually specifying
    LinksID. Required args:

    * col - specify the column in the source list of list that is checked for
    processing. This column value is checked for duplication.
    Typically this is the LinksID as LinksID duplicates are returned for health
    club nightly runs.

    Example return: [[n...], [n...], n...]], {col: [[n...], [n...], [n...]]}
    # where frst list of list is non duplicate rows. And second dictionary of
    list of lists is a series of duplicate values found in col of each row. 
    '''

# Utility Functions for SQL connectivity


def pull_SQL_data(mode, sqlQuery, sqlSvr, sqlDB, sqlUname, sqlPw,
                  outFileName=None, loadIntoMem=False, loadIntoMemType=None,
                  key=None, iterable=None, target=None, user=None, pw=None, emailPackage=None):
    '''
    Connects & queries SQL database on specified server with passed in
    username / password. Dependnig on mode selected - will either save query
    as CSV file (outFile), return the cursor object for troubleshooting or
    do multiple procedural queries over a loop. All modes require all mandatory
    arguments (args without default values such as None or False). Three modes:

    Mode: 'loop_n_load' - loops over a collection/iterable and does a
    succession of many simple SQL queries and stores the results in memory to
    be later used. E.g. useful with Health Club whereby VisitSurveyView is
    queried and the most recent visitor visit is returned for the associated
    LinksID (key). Especially useful with 

    Requires args:
    * iterable, which takes an iterable such as a list or any other collection
    that can be iterated over.
    * key, which identifies the field in the returned row which will act as key
    in the dictionary ph2 (placeholder2).

    Mode: 'query_save' - save to disk some complex SQL query. Stores in CSV
    format. Most commonly used mode. Returns a list of two items. CSV output
    filename and ph (which is either empty or filled with data)

    Requires args:
    * outFileName, the name of the CSV file that will save the sqlQuery
    * loadIntoMem, either True or False, if True, will also save data of each
    returned SQL query row as identified by row[key]. I.e. in addition to
    saving the complex query as CSV, will also store and return a list of
    [outFileName, [row1[key], row2[key], ...]]
    * loadIntoMemType, pass either 'list' or 'set'. Based on whether the column
    or field you're wanting to collate has duplicates, whether you need dupes
    or whether you want non dupes.
    * key, integer value of the column / field that holds the data you're after
    e.g. LinksID. This can then be used to do another complex SQL query,
    or 'loop_n_load' mode to iterate over.

    Mode: 'list_of_lists' - will simply 'save' each row as a list of items
    inside another list and return it for manipulation in memory. Typically
    used in conjunction with function stateConversion which is used to commit
    what is in memory to CSV format for further manipulation.

    Required args:
    * just the mandatory arguments. Nothing more.

    Mode: 'return_cursor' - will just return the pyodbc connection object after
    doing running query as idenfied by the arg sqlQuery. Used for quick
    troubleshooting.
    '''
    ph = []
    ph2 = {}
    ph3 = set()

    try:
        conn = pyodbc.connect('DRIVER={SQL Server Native Client 10.0};SERVER=' +
                              sqlSvr + ';DATABASE=' + sqlDB + ';UID=' +
                              sqlUname + ';PWD=' + sqlPw)
        cursor = conn.cursor()
    except Exception:  # nested two trys may not be needed
        try:
            cursor.close()
        except Exception:
            print('cursor already closed!', sys.exc_info())
        try:
            conn.close()
        except Exception:
            print('conn already closed!', sys.exc_info())
        if emailPackage:  # not None
            emailalerter.alert(emailPackage, mode='err', to='prim',
                               body='Error @ Point: R')
        errorLog(p='Point: R', error=str(sys.exc_info()))

    if mode == 'loop_n_load':
        for i in iterable:
            try:
                q = sqlQuery.format(i)
                cursor.execute(q)
                row = cursor.fetchone()
                ph2[str(row[key])] = list(row)  # entire row
            except Exception:
                errorLog(p='Point: T', error=str(sys.exc_info()))
                print('debugging - row skipped')
        try:
            cursor.close()
        except Exception:
            print('cursor already closed!', sys.exc_info())
        try:
            conn.close()
        except Exception:
            print('conn already closed!', sys.exc_info())
        return ph2
    elif mode == 'query_save' or mode == 'list_of_lists':
        try:
            cursor.execute(sqlQuery)  # pull easy
            if mode == 'list_of_lists':
                while True:
                    row = cursor.fetchone()
                    ph.append(list(row))
                    if not row:  # no moar rows! :(
                        break
            elif mode == 'query_save':
                mapSourceDestination('map_staging', target=target, user=user,
                                     pw=pw)
                with open('Q:' + outFileName, 'w', newline='') as CSV:
                    wr = csv.writer(CSV)
                    while True:
                        row = cursor.fetchone()
                        wr.writerow(row)  # PROBLEM HERE!
                        # return [outFileName, [r[key], r2[key], n]
                        if loadIntoMem == True:
                            if loadIntoMemType == 'list':
                                ph.append(row[key])
                            elif loadIntoMemType == 'set':
                                ph3.add(row[key])
                        if not row:
                            break
        except Exception:  # no moar rows! :)
            cursor.close()  # amazin
            conn.close()  # amazin
            errtype, value, traceback = sys.exc_info()
            if str(value) == 'iterable expected, not NoneType':
                pass  # expected
            else:
                if emailPackage:  # not None
                    emailalert.alerter(emailPackage, mode='err', to='prim',
                                       body='Error @ Point: I')
                errorLog(p='Point: I', sqlQuery=sqlQuery, sqlSvr=sqlSvr,
                         sqlDB=sqlDB, sqlUname=sqlUname, outFile=outFileName,
                         error=str(sys.exc_info()))
        finally:  # double checks
            try:
                cursor.close()
                print('here???')
            except Exception:
                print('cursor already closed!', sys.exc_info())
            try:
                conn.close()
            except Exception:
                print('conn already closed!', sys.exc_info())
        if mode == 'list_of_lists':
            return ph
        elif mode == 'query_save':
            if loadIntoMemType == 'list':
                return [outFileName, ph]
            elif loadIntoMemType == 'set':
                return [outFileName, ph3]
    elif mode == 'return_cursor':
        cursor.execute(sqlQuery)  # pull easy
        return [conn, cursor]  # for direct work on SQL view


def sql_refTables(query, emailPackage=None):  # load tables into memory
    '''
    update - 22 september 2019 - docstring needs to be fleshed out.
    Load into memory reference Tables used for lookup, referencing
    and compiling new tables which can then be saved as CSVs.
    Used in conjunction with variables from sqlQueries module.
    Import ETLJitterbitClone in the mainline script then call
    sql_refTables with the variable from sqlQueries module e.g.
    membersTable = refTable(sqlQueries.membershiptTypes)
    '''
    referenceTable = {}  # a dictionary!
    conn, cursor = pull_SQL_data(query)  # [conn, cursor]
    try:
        while True:
            row = list(cursor.fetchone())  # see membershipTypes in sqlQueries
            key = row.pop(0)  # 0 is primary key or unique identifier of table
            referenceTable[key] = row
            if not row:
                break
    except Exception:
        cursor.close()  # amazing
        conn.close()  # grace
        if emailPackage:  # not None
            emailalert.alerter(emailPackage, mode='err', to='prim',
                               body='Error @ Point: J')
        errorLog(p='Point: J', query=query, error=str(sys.exc_info()))
    finally:
        try:
            cursor.close()
        except Exception:
            print('cursor already closed!', sys.exc_info())
        try:
            conn.close()
        except Exception:
            print('conn already closed!', sys.exc_info())
    return referenceTable

# Next two functions act as: for each parent, find associated child


def CSV_query(mode, csvfile, col=None, max_size=None, value=None,
              colFormat='string', source=None, target=None, user=None, pw=None,
              emailPackage=None):
    '''
    update - 22 september 2019 - docstring needs to be fleshed out.
    Function queries CSV files as T-SQL script queries SQL databases.
    Load item(s) or entire rows given conditions into memory to be used in
    further transforms, collation of data and or creation of CSVs in
    conjunction with other functions.

    Typical use of function is to extract emails from a SAP report to be used
    in calling query_salesforce function.

    All calls require args 'mode' and 'csvfile' to be passed.

    Specific modes as follows:

    'select_col' - will return as a list the values of a column of every row
    in csvfile. Similar to SQL query: 'SELECT col from csvfile'
    Both col(index) & colFormat args are required. 'string' passed as arg for
    colFormat will return comma delimited value of strings e.g.
    'x@y.com, a@b.com, me@my.net, nirav@knowssomething.com.au'
    If 'list' is passed, it'll return ['x@y.com', 'a@b.com', ...]

    'select_all' - will return a dictionary of lists. Similar to SQL query:
    'SELECT * from csvfile' # note will return in following format:
    {row[row.pop(col)]: [row[0], row[1], row[n]]}
    Only col arg is required.

    'find_value' - will find specific value passed to value arg. Will return
    row as a dictionary with a single list of values. Similar to SQL query:
    'SELECT * WHERE col = value' # note will return in following format:
    {value: [col0, col1, col2, coln]}
    Both col & value args are required.

    Functionality to be expanded as needed to replicate common SQL queries.

    'source', 'target', 'user', 'pw' are a necessary evil - passes needed info
    for mapSourceDestination function.
    '''

    mapSourceDestination('map_staging', target=target, user=user, pw=pw)
    # assumes CSV is already in target

    temp, temp2, list_of_lists, list_of_strs = [], {}, [], []  # placeholders

    with open('Q:' + csvfile) as CSV:
        try:
            if mode == 'select_col':
                for row in CSV:
                    split = row.split(',')
                    if len(temp) < max_size:
                        temp.append(split[col])  # e.g. (split[3])
                    else:
                        temp.append(split[col])  # for that one missing row! :)
                        list_of_lists.append(temp[:])  # copy ensures no ref
                        temp.clear()
                if len(temp) != 0:
                    list_of_lists.append(temp[:])
                    temp.clear()
            elif mode == 'select_all':
                for row in CSV:
                    split = row.split(',')
                    key = split.pop(col)
                    temp2[key] = split
            elif mode == 'find_value':
                for row in CSV:
                    split = row.split(',')
                    for i in split:
                        if i == value:
                            key = i
                            break
                    temp2[key] = split
                    index = temp2[key].index(i)
                    del temp2[key][index]  # remove key from list value
        except Exception:
            if emailPackage:  # not None
                emailalert.alerter(emailPackage, 'Error @ Point: K')
            errorLog(p='Point: K', mode=mode, csvfile=csvfile, col=col,
                     value=value, colFormat=colFormat,
                     error=str(sys.exc_info()))

    mapSourceDestination('unmap_staging')  # remove

    if mode == 'select_col':  # todo 10 feb - add exception handling like above
        try:
            if colFormat == 'string':
                while len(list_of_lists) != 0:
                    output = ''
                    temp = list_of_lists.pop()
                    for j in temp:
                        output += "'" + j + "'" + ','
                    list_of_strs.append(output[:-1])  # remove last ','
                return list_of_strs
            elif colFormat == 'list':
                return list_of_lists
        except Exception:
            if emailPackage:  # not None
                emailalert.alerter(emailPackage, 'Error @ Point: S')
            errorLog(p='Point: S', mode=mode, csvfile=csvfile, col=col,
                     value=value, colFormat=colFormat,
                     error=str(sys.exc_info()))
    else:  # caters both 'select_col' & 'find_value'
        return temp2  # no need to cater for large datasets

# Utility Function - Salesforce


def preupload_prep(mode, sfConn, csvfile, primaryID=None, select=None,
                   debug=False, source=None, target=None, user=None, pw=None,
                   emailPackage=None):
    '''
    Upsert a data collection to Salesforce object. Depending on the mode
    selected. Available modes:

    'Contact' - upserts to Contact object, sub modes are specified by the
    'select' argument. Either 'car_park_tickets', 'health_club', 'swim_school',
    or 'gymnastics'.

    'Contact_MailingPostalCode' - for whatever reason upserts to contact
    object with 'MailingPostalCode' as a key to value mapping will fail.
    Salesforce will spit out an error. However after having upserted new/old
    contact records, the same 'MailingPostalCode' can be reupserted with
    primaryID set as SFID. This will be successful. This is the only reason
    why this mode exists separately to the 'Contact' mode.

    'Opportunity' - upserts to Opportunity object, sub modes identical to that
    of 'Contact' mode.

    'sfConn' - pass in the Salesforce connection object.

    'csvfile' - pass in the name of the CSV file that holds the fields to be
    uploaded to salesforce.

    'primaryID' - Salesforce API requires this arg to be present. Typically
    for new or old 'Contacts', upserts can use 'Email' as primaryID. For
    Opportunity upserts, use 'Name'.

    Update 11 Feb 2020 - with primaryID set to None (default) it'll create new
    records using SimpleSalesforce API call of sf.bulk.Contact.insert(data)
    - the assumption here is that any existing records with matching pairs of
    FirstName + LastName + Email, will just update the existing record or if
    it doesn't exist will create a new record. 

    'debug' - when True is passed to this arg, function will return the mapping
    of what is to be upserted to Salesforce. When False (default), function
    will upsert mapping to Salesforce. True is useful when determining why
    upserts are failing.

    Note, this function is where the 'Load' component of ETL happens. As such
    Every type of upsert you are planning to do needs to be added here. I.e.
    once you have the shape of the CSV file (final transformed CSV file), then
    modify this function by adding a new 'mode' and specifying the mapping
    structure based on the CSV file.

    As an example, for 'gymnastics' mode, entirePackage.append(x)

    Where x is {'FirstName': split[a], 'Class': split[b], ...}

    'source', 'target', 'user', 'pw' - these are to call mapSourceDestination
    '''
    mapSourceDestination('map_staging', target=target, user=user, pw=pw)

    entirePackage = []  # load in memory items from CSV in destination

    if mode == 'Contact':
        try:
            with open('Q:' + csvfile) as CSV:
                for row in CSV:
                    split = row.split(',')  # make list of CSV param
                    if select == 'car_park_tickets':
                        entirePackage.append(  # todo: need to add -
                            # MailingStreet, MailingCity, MailingState,
                            # MailingPostalCode, Phone
                            {'FirstName': split[2],
                             'LastName': split[3],
                             'MobilePhone': split[4],
                             'Email': split[6],
                             'What_s_On__c': int(split[12])  # only takes ints
                             }
                        )
                    elif select == 'health_club_nomailing':
                        entirePackage.append(
                            {'FirstName': split[2],
                             'LastName': split[1],
                             'Phone': split[10],
                             'MobilePhone': split[12],
                             'Email': split[13],
                             'LINKS_CUSTID__c': split[0]
                             }
                        )
                    elif select == 'health_club':  # including mailing info
                        entirePackage.append(
                            {'Id': split[21],
                             'MailingStreet': split[6],
                             'MailingCity': split[7],
                             'MailingState': split[8],
                             'MailingPostalCode': split[9],
                             }
                        )
                    elif select == 'swim_school':
                        ...  # to be fleshed out
                    elif select == 'gymnastics':
                        ...
        except Exception:
            if emailPackage:  # not None
                emailalert.alerter(emailPackage, mode='err', to='prim',
                                   body='Error @ Point: L')
            if len(entirePackage) != 0:
                errrow = entirePackage.pop()
            else:
                errrow = entirePackage
            errorLog(p='Point: L', mode=mode, csvfile=csvfile,
                     primaryID=primaryID, select=select, debug=debug,
                     row=row, split=split, last_row=errrow,
                     error=str(sys.exc_info()))
        if debug == True:
            mapSourceDestination('unmap_staging')
            return entirePackage
        else:
            # caters for Fn+Ln+Email combo uses sf.bulk.Contact.insert(data) API call
            if primaryID == None:
                print('at least here!')
                chunk_n_upload('Contact', 500, entirePackage,
                               sfConn, emailPackage=emailPackage)
                print('if not??? here.')
            else:  # original upsert with primaryID specified by mainline script
                chunk_n_upload('Contact', 500, entirePackage,
                               sfConn, primaryIDentifier=primaryID,
                               emailPackage=emailPackage)
    # legacy for car parks - TODO: remove and add to the main IF clause like health_club_nomail
    elif mode == 'Contact_MailingPostalCode':
        try:
            with open('Q:' + csvfile) as CSV:
                for row in CSV:
                    split = row.split(',')  # make list of CSV param
                    entirePackage.append(
                        {'Id': split[14],  # check inFile for index of ID
                         # check inFile for index
                         'MailingPostalCode': split[5]
                         }
                    )
        except Exception:
            if emailPackage:  # not None
                emailalert.alerter(emailPackage, mode='err', to='prim',
                                   body='Error @ Point: M')
            if len(entirePackage) != 0:
                errrow = entirePackage.pop()
            else:
                errrow = entirePackage
            errorLog(p='Point: M', mode=mode, csvfile=csvfile,
                     primaryID=primaryID, select=select, debug=debug,
                     row=row, split=split, last_row=errrow,
                     error=str(sys.exc_info()))
        if debug == True:
            mapSourceDestination('unmap_staging')
            return entirePackage
        else:
            chunk_n_upload('Contact', 500, entirePackage, sfConn, primaryID,
                           emailPackage=emailPackage)
    elif mode == 'Opportunity':
        try:
            with open('Q:' + csvfile) as CSV:
                for row in CSV:
                    split = row.split(',')
                    if select == 'car_park_tickets':
                        entirePackage.append(
                            {'Ticket_Number__c': int(split[8]),  # 'Ticket No'
                             'Park_Date__c': split[0],  # 'Car Park date'
                             'Pay_Time__c': int(split[11]),  # 'Pay Time'
                             'Promo_codes__c': split[13],  # 'Promo Code'
                             'Amount': split[7],  # 'Pay Amount'
                             'Pay_Date__c': split[10],  # 'Pay Date'
                             'Car_Park__c': split[1],  # 'Car Park'
                             # 'TotalOpportunityQuantity': format(int(split[9]), '.2f'), #'Tickets'
                             'CloseDate': split[10],  # 'Pay Date'
                             'StageName': 'Closed Won',
                             'Contact__c': split[14],  # SFID of asso'd Contact
                             'Name': split[16],
                             'RecordTypeId': split[17]  # RecordTypeId on Sf
                             }
                        )
                    elif select == 'health_club':
                        entirePackage.append(
                            {'Student_DOB__c': split[14],
                             'Aquatic_Health_Club__c': 1,
                             'Opportunity_type__c': 'Health Club',
                             'Start_Date__c': split[4],
                             'Status__c': split[16],
                             'Membership_Type__c': split[3],
                             'Current_Expiry__c': split[24].strip(),
                             'Last_Visit__c': split[20][:10],  # date only
                             # null? use ddDate otherwise use Current_Expiry__c
                             'CloseDate': split[24].strip(),
                             'StageName': 'Closed Won',
                             'Contact__c': split[21],
                             'Name': split[23],
                             'RecordTypeId': ''  # insert record type e.g. 0125D0000000
                             }
                        )
                    elif select == 'swim_school':
                        ...  # to be fleshed out
                    elif select == 'gymnastics':
                        ...
        except Exception:
            if emailPackage:  # not None
                emailalert.alerter(emailPackage, mode='err', to='prim',
                                   body='Error @ Point: N')
            if len(entirePackage) != 0:
                errrow = entirePackage.pop()
            else:
                errrow = entirePackage
            errorLog(p='Point: N', mode=mode, csvfile=csvfile,
                     primaryID=primaryID, select=select, debug=debug,
                     row=row, split=split, last_row=errrow,
                     error=str(sys.exc_info()))
        if debug == True:
            mapSourceDestination('unmap_staging')
            return entirePackage
        else:  # todo 11 feb 2020 - need to add clause for when primaryID is None
            if primaryID == None:
                ...
            else:
                chunk_n_upload('Opportunity', 500, entirePackage,
                               sfConn, primaryIDentifier=primaryID,
                               emailPackage=emailPackage)

    mapSourceDestination('unmap_staging')  # unmap drive


def delete_sf_records(mode, obj, sfConn, records, emailPackage=None):
    '''
    Just that, bulk deletes either Contact records or Opportunity records
    depending on mode and obj selected. Saves having to log into SF and doing
    so with a zillion clicks. Use with care on production environment.
    Soft delete is good enough for 99% of the time. It goes to recycle bin
    that stays on Salesforce for 15 days by default, before being emptied.

    mode - either 'soft' delete or 'hard' delete (see Salesforce doc on diff.

    obj - either 'contact' or 'opportunity'.

    sfConn - Salesforce connection object.

    records - a list of dictionaries e.g. [{'Id': '0000000000AAAAA'}]. This
    can be obtained from return value of query_salesforce function.
    '''
    if obj == 'contact':
        try:
            if mode == 'soft':
                sfConn.bulk.Contact.delete(records)
            elif mode == 'hard':
                sfConn.bulk.Contact.hard_delete(records)
        except Exception:
            if emailPackage:  # not None
                emailalert.alerter(emailPackage, mode='err', to='prim',
                                   body='Error @ Point: O')
            if len(records) != 0:
                errrow = records.pop()
            else:
                errrow = records
            errorLog(p='Point: O', mode=mode, last_record=errrow,
                     error=str(sys.exc_info()))
    elif obj == 'opportunity':
        try:
            if mode == 'soft':
                sfConn.bulk.Opportunity.delete(records)
            elif mode == 'hard':
                sfConn.bulk.Opportunity.hard_delete(records)
        except Exception:
            if emailPackage:  # not None
                emailalert.alerter(emailPackage, mode='err', to='prim',
                                   body='Error @ Point: P')
            if len(records) != 0:
                errrow = records.pop()
            else:
                errrow = records
            errorLog(p='Point: P', mode=mode, last_record=errrow,
                     error=str(sys.exc_info()))
