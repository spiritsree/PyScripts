#!/usr/bin/env python3

'''
The script will send csv data to provided email.

Usage:
    emailCSVContent.py <csv_path> <email>

Requirement:
    any mail server should be running with standard SMTP port.
'''

import os
import csv
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import re

def send_email(csv_file, email):
    '''
    To email csv data.
    First row will be taken as header.
    Mail will be sent as MIME text with both plaintext and html support.
    '''
    from_email = "info@example.com"
    try:
        with open(csv_file, 'r') as csvfile:
            table_string = ''
            text_string = ''
            header_string = ''
            reader = csv.reader(csvfile)
            header = next(reader, None)
            header_string += ','.join(header) + "\n"
            table_string += "<tr>" + \
                                "<th>" + \
                                    "</th><th>".join(header) + \
                                "</th>" + \
                            "</tr>\n"
            for row in reader:
                text_string += ','.join(row) + "\n"
                table_string += "<tr>" + \
                                    "<td>" + \
                                        "</td><td>".join(row) + \
                                    "</td>" + \
                                "</tr>\n"

            print(table_string)
    except IOError as e:
        print('I/O error in reading file ' + csv_file + "with error " + str(e))
        return 'Error'

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "CSV Data"
    msg['From'] = from_email
    msg['To'] = email

    # Create the body of the message (a plain-text and an HTML version).
    text = "Hi, \n\nPlease find the attached data:\n\n" + text_string + "\n\nCheers,\nXXXXXXX\n"
    html = "<html><head></head><body><p>Hi there !<br><br>Please find the tabular data below.<br><br>" + table_string + "<br><br></p><p>Cheers,<br>XXXXXXX<br><br></p></body></html>"

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    msg.attach(part1)
    msg.attach(part2)

    try:
        s = smtplib.SMTP('localhost')
        print('Sending email....')
        s.sendmail(from_email, email, msg.as_string())
        s.quit()
    except:
        print('Failed smtp connection with error ' + str(sys.exc_info()[1]))
        return 'Error'

    return 'Success'

def main():
    '''
    Main function.
    '''
    if len(sys.argv) != 3:
        print('\nUsage: {} <csv_file_path> <email_id>\n'.format(sys.argv[0]))
        sys.exit(1)
    csv_file = sys.argv[1]
    if not os.path.isfile(csv_file):
        print('\n{}: Not a valid file\n'.format(csv_file))
        sys.exit(1)
    email = sys.argv[2]
    if not re.match('^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', email):
        print('\n{}: Not a valid email\n'.format(email))
        sys.exit(1)
    email_status = send_email(csv_file, email)
    if email_status == 'Error':
        print('Failed to sent email.')

if __name__ == '__main__':
    main()
