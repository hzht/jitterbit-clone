# Author: HZHtat
# Date: Jan-2020
# Version: 0.8

import smtplib
from email.message import EmailMessage


def alerter(emailpackage, mode, to, body):
    '''
    emailpackage is a list:
    [sender, [receiver(s),], error_subject, success_subject, mailsvr]
    Pass through a list of following structure. e.g.
    ([
    'from',
    {'prim': x, 'sec': y, 'ter': z},
    {'err': ..., 'success': ..., 'info': ...},
    mailsvr
    ],
    'mode',
    'to',
    'body')
    Arg exception is either True or False
    '''
    msg = EmailMessage()
    msg['From'] = emailpackage[0]

    if to == 'prim':
        msg['To'] = emailpackage[1]['prim']
    elif to == 'sec':
        msg['To'] = emailpackage[1]['sec']
    elif to == 'ter':  # tertiary
        msg['To'] = emailpackage[1]['ter']

    msg.set_content(body)

    # subject lines
    if mode == 'err':
        msg['Subject'] = emailpackage[2]['err']
    elif mode == 'success':
        msg['Subject'] = emailpackage[2]['success']
    elif mode == 'info':
        msg['Subject'] = emailpackage[2]['info']

    s = smtplib.SMTP(emailpackage[3])  # mailserver
    s.send_message(msg)
    s.quit()
