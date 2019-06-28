import pyodbc, sys, logging, configparser
import smtplib, ssl
import threading
import pandas as pd
import os, schedule, time
from datetime import datetime, timedelta
from openpyxl import Workbook
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase


try :


    def main():
        logging.basicConfig(filename=f'Log/app_{datetime.now().strftime("%Y%m%d%H%M%S")}.log', filemode='w',
                            format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

        logging.warning('Job Started')
        createExcel()
        sendEmail()
        logging.warning('Job Completed')

    def sendEmail():
        logging.warning('Sending Email...')
        global fileName

        #print(config['USER']['Recipient'])

        report_name =  config['EMAIL']['Subject'] +' ('+ datetime.today().date().strftime('%d-%b-%Y') + ')'
        sender_email = config['USER']['Sender']
        if int(config['MODE']['DEBUG']) == 1:
            receiver_email = config['USER']['TestRecipient']
        else:
            receiver_email = config['USER']['Recipient']
        cc_email = config['USER']['CC']
        password = config['SERVER']['Password']
        smtp_server = config['SERVER']['Smtp_Server']
        port = config['SERVER']['Port']  # For SSL

        message = MIMEMultipart()
        message["Subject"] = report_name
        message["From"] = sender_email
        message["To"] = receiver_email
        message["CC"] = cc_email

        html = f"""\
               <html>
                 <body>
                   <p>Hi,<br><br>
                      Attached please find the {report_name}<br>
                   </p>
                 </body>
               </html>
               """

        # Turn these into plain/html MIMEText objects
        # part1 = MIMEText(text, "plain")

        body = MIMEText(html, "html")

        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        # message.attach(part1)
        message.attach(body)

        fileName = 'File/' + fileName

        attachment = MIMEBase('application', "octet-stream")
        attachment.set_payload(open(fileName, "rb").read())
        encoders.encode_base64(attachment)
        #fileName = 'D:/Users/User/PycharmProjects/Scheduler/venv/File/Unsuccessful-2019-06-26.xlsx'
        attachment.add_header('Content-Disposition', f'attachment; filename={os.path.basename(fileName)}''')

        message.attach(attachment)

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        # with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        #    server.login(sender_email, password)
        #    server.sendmail(
        #        sender_email, receiver_email, message.as_string()
        #    )

        try:
            server = smtplib.SMTP(smtp_server, port)
            # server.ehlo()  # Can be omitted
            server.starttls(context=context)
            # server.ehlo()  # Can be omitted
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        except Exception as e:
            logging.exception(e)
        finally:
            server.quit()

    def createExcel():
        logging.warning('Creating Excel...')
        global fileName

        dDate = datetime.today().date()+ timedelta(int(config['DATA']['TimeDelta']))
        print(f"Run date is: {dDate}")

        conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=' + config['SERVER']['Sql_Server'] + ';'
                          'Trusted_Connection=yes;'
                          'Database=Jeunesse_Back;')

        cursor = conn.cursor()
        sSql = f"""         
                select 
            ma.country,
            ma.siteurl,
            mo.mainorderspk,
            mo.orderdate,
            WASiteurl=isnull(mMA.siteurl,''), 
            WA_Amount=isnull(MAO.amount,0),
            Refund_WA_Siteurl=isnull(mMAO.siteurl,isnull(mMAORefund.siteurl,'')),
            Refund_WA_Amount=isnull(MAORe.amount,isnull(MAORefund.amount,0)),
            BC_Siteurl=isnull(m.siteurl,''), 
            BC_Amount=isnull(MBC.amount,0),
            Refund_BC_Siteurl=isnull(mRe.siteurl,isnull(mRefund.siteurl,'')), 	
            Refund_BC_Amount=isnull(MBCRe.amount,isnull(MBCRefund.amount,0))
        from mainorders mo with (nolock)
        left join main ma on mo.mainfk=ma.mainpk
        left join  [dbo].[Main_BonusCredit] MBC with (nolock)　on mo.mainorderspk=MBC.MainordersFk
        left join  [dbo].[Main_BonusCredit] MBCRe with (nolock)　on mo.mainorderspk=MBCRe.TriggerMainordersFk
        left join  [dbo].[MainOrderRefunds] MBCRefund with (nolock)　on mo.mainorderspk=MBCRefund.MainordersFk  and MBC.mainfk=MBCRefund.mainfk
        left join main m with (nolock) on MBC.mainfk=m.mainpk
        left join main mRe with (nolock) on MBCRe.mainfk=mRe.mainpk
        left join main mRefund with (nolock) on MBCRefund.mainfk=mRefund.mainpk
        left join [dbo].[MainAccount_Order] MAO with (nolock) on mo.mainorderspk=MAO.mainordersfk
        left join [dbo].[MainAccount_Order] MAORe with (nolock) on mo.mainorderspk=MAORe.REFUndmainordersfk
        left join [dbo].[MainOrderRefunds] MAORefund with (nolock) on mo.mainorderspk=MAORefund.MainordersFk and MAO.mainfk=MAORefund.mainfk
        left join main mMA with (nolock)　on MAO.mainfk=mMA.mainpk
        left join main mMAO with (nolock)　on MAORe.mainfk=mMAO.mainpk
        left join main mMAORefund with (nolock)　on MAORefund.mainfk=mMAORefund.mainpk
        where mo.orderdate　>= '{dDate}' 
        and mo.orderdate <= dateadd(hour,-24,getdate())
        and ma.country in ('CN','HK','TW','MO') and mo.paidstatus=0 
        and  ( 
            (isnull(mMA.siteurl,'')<>'') and (isnull(mMAO.siteurl,isnull(mMAORefund.siteurl,''))='')
            or 	
            (isnull(m.siteurl,'')<>'') and (isnull(mRe.siteurl,isnull(mRefund.siteurl,''))='')
        )
        order by ma.country, mo.mainorderspk
        """

        #print(sSql)
        cursor.execute(sSql)

        if cursor.rowcount != 0:
            #print("Create Excel!")

            #Create Excel
            wb = Workbook()
            ws = wb.active
            #Header
            ws.append(['Country', 'User Name', 'Order Number', 'Order Date', 'Wallet Paid Siteurl', 'Amount', 'Refunded Wallet Siteurl', 'Refunded Wallet Amount',
                       'Bonus Credit Paid Sisteurl', 'Amount','Refunded Bonus Credit Siteurl', 'Refunded Bonus Credit Amount'])

            for row in cursor.fetchall():
                ws.append([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10] , row[11]  ])

            #Adjust Width
            for col in ws.columns:
                max_length = 0
                column = col[0].column  # Get the column name
                colLetter = chr(ord('A') + col[0].column-1)
                #print(colLetter)

                # Since Openpyxl 2.6, the column name is  ".column_letter" as .column became the column number (1-based)
                for cell in col:
                    try:  # Necessary to avoid error on empty cells
                        if len(str(cell.value)) > max_length:
                            max_length = len(cell.value)
                    except:
                        pass
                    adjusted_width = (max_length + 2) * 1.2
                    #print(int(adjusted_width))
                    ws.column_dimensions[colLetter].width = adjusted_width

            ws.column_dimensions['D'].width = ws.column_dimensions['D'].width * 1.5
            fileName=f'Unsuccessful-{datetime.today().date()}.xlsx'
            wb.save('File/'+fileName)
            wb.close()
        else:
            logging.error("NO send email!!!")

        conn.close()

    #Call main module
    config = configparser.ConfigParser()
    config.read('config.ini')

    if int(config['MODE']['DEBUG']) == 1:
        schedule.every(1).minutes.do(main)
    else:
        schedule.every().monday.at("14:30").do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)

except Exception as e:
    logging.exception(e)

finally:
    print("Finished")