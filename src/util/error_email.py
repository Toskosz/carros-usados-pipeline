import smtplib
import email.message
import os

def notify_erro(ex):
    texto = "<p>Erro durante pipeline. "+ str(ex) +" </p>"
    msg = email.message.Message()
    msg['Subject'] = "Email automático used-cars pipeline"
    
    msg['From'] = os.getenv('EMAIL_SENDER', '')
    password = os.getenv('EMAIL_PASSWORD', '')
    msg['To'] = os.getenv('EMAIL_RECEIVER', '')

    msg.add_header('Content-Type', 'text/html')
    msg.set_payload(texto)

    s = smtplib.SMTP('smtp.gmail.com: 587')
    s.starttls()
    s.login(msg['From'], password)
    s.sendmail(msg['From'], [msg['To']], msg.as_string().encode('utf-8'))
    
    return