import pyodbc
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import sqlalchemy

#%% Configura de conexao antiga
# Configuração da conexão com o banco de dados
server = ''
database = ''
username = ''
password = ''
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Estabeleça a conexão com o banco de dados
# conn = pyodbc.connect(conn_str)
#%%
def alchConn(banco):
    st = "/"+\
        banco+"?driver=ODBC+Driver+18+for+SQL+Server&ENCRYPT=no"
    return sqlalchemy.create_engine(st)

conn = alchConn('CLIENTE_PRUMO')


# Consulta SQL 1
query1 = """
SELECT
nm_colaborador
,cd_placa
,ds_obra
,id_rastreador
,id_ibutton
,dh_excesso_velocidade
,hr_duracao
,fl_distancia
,nu_velocidade
,ds_endereco
,nm_cidade
,nm_uf
,ds_categoria
,Dia_da_semana
FROM 
(
SELECT
 IIF(a.nm_colaborador is null or a.nm_colaborador = '',B.nm_colaborador,a.nm_colaborador) as nm_colaborador,
 a.cd_placa,
 a.id_rastreador,
 IIF(a.id_ibutton is null or a.id_ibutton = '',B.cd_ibutton,a.id_ibutton) as id_ibutton,
 a.dh_excesso_velocidade,
 a.hr_duracao,
 a.fl_distancia,
 a.nu_velocidade,
 a.ds_endereco,
 a.nm_cidade,
 a.nm_uf,
 a.ds_categoria,
 a.ds_obra,
 case
    when (ds_categoria LIKE '%LEVE%' OR ds_categoria LIKE '%PICKUP MEDIA%') AND nu_velocidade >= 121 THEN 'EXCESSO'
    when nu_velocidade >= 88 AND ds_categoria IN (SELECT DISTINCT(ds_categoria) FROM CLIENTE_PRUMO.relatorio.implantacao_pbi_historico WHERE (ds_categoria NOT LIKE '%LEVE%' AND ds_categoria NOT LIKE '%PICKUP MEDIA%')) THEN 'EXCESSO'
    else 'AVALIAR'
 end as excesso_tratado,
 CASE DATEPART(dw, dh_excesso_velocidade)
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Segunda-feira'
    WHEN 3 THEN 'Terça-feira'
    WHEN 4 THEN 'Quarta-feira'
    WHEN 5 THEN 'Quinta-feira'
    WHEN 6 THEN 'Sexta-feira'
    WHEN 7 THEN 'Sábado'
 END AS 'Dia_da_semana'
FROM CLIENTE_PRUMO.relatorio.vw_excesso_velocidade AS A
LEFT JOIN CLIENTE_PRUMO.relatorio.ibutton_ok AS B ON A.cd_placa = B.cd_placa AND A.dh_excesso_velocidade BETWEEN B.dh_ini_mov AND B.dh_ult_mov
WHERE nu_velocidade >= 88
and hr_duracao >= '00:00:30'
and CAST(a.dh_excesso_velocidade AS DATE) BETWEEN CAST(dateadd(day,-7,GETDATE()) AS DATE) AND CAST(dateadd(day,-1,GETDATE()) AS DATE)
) A 
WHERE excesso_tratado <> 'AVALIAR'
ORDER BY A.dh_excesso_velocidade DESC
""" 

# Consulta SQL 2
query2 = """
SELECT top 20 a.nm_colaborador,a.ds_obra, COUNT(*) as qtd_excesso_semana
FROM 
(
SELECT
 a.cd_ano_mes
,a.id_colaborador
,IIF(a.nm_colaborador is null or a.nm_colaborador = '',B.nm_colaborador,a.nm_colaborador) as nm_colaborador
,A.ds_obra
,a.cd_placa
,a.id_rastreador
,IIF(a.id_ibutton is null or a.id_ibutton = '',B.cd_ibutton,a.id_ibutton) as id_ibutton
,a.dh_excesso_velocidade
,a.hr_duracao
,a.fl_distancia
,a.nu_velocidade
,a.ds_endereco
,a.nm_cidade
,a.nm_uf
,a.ds_categoria
,a.cd_filial
,a.centrodecusto
,a.id_superior
,a.nm_gestor
, convert(time,dh_excesso_velocidade) as hora,
case
    when (ds_categoria LIKE '%LEVE%' OR ds_categoria LIKE '%PICKUP MEDIA%') AND nu_velocidade >= 121 THEN 'EXCESSO'
    when nu_velocidade >= 88 AND ds_categoria IN (SELECT DISTINCT(ds_categoria) FROM CLIENTE_PRUMO.relatorio.implantacao_pbi_historico WHERE (ds_categoria NOT LIKE '%LEVE%' AND ds_categoria NOT LIKE '%PICKUP MEDIA%')) THEN 'EXCESSO'
    else 'AVALIAR'
end as excesso_tratado,
case
    when convert(time,dh_excesso_velocidade) >= '03:01' and convert(time,dh_excesso_velocidade) <= '6:00' then '03:00 - 06:00'
    when convert(time,dh_excesso_velocidade) >= '6:01' and convert(time,dh_excesso_velocidade) <= '9:00' then '06:00 - 09:00'
    when convert(time,dh_excesso_velocidade) >= '9:01' and convert(time,dh_excesso_velocidade) <= '12:00' then '09:00 - 12:00'
    when convert(time,dh_excesso_velocidade) >= '12:00' and convert(time,dh_excesso_velocidade) <= '15:00' then '12:00 - 15:00'
    when convert(time,dh_excesso_velocidade) >= '15:01' and convert(time,dh_excesso_velocidade) <= '18:00' then '15:00 - 18:00'
    when convert(time,dh_excesso_velocidade) >= '18:01' and convert(time,dh_excesso_velocidade) <= '21:00' then '18:00 - 21:00'
    when convert(time,dh_excesso_velocidade) >= '21:01' and convert(time,dh_excesso_velocidade) <= '23:59' then '21:00 - 00:00'
    when convert(time,dh_excesso_velocidade) > ='00:00' and convert(time,dh_excesso_velocidade) <= '3:00' then '00:00 - 03:00'
else '0'
end as faixa_hora
, CASE DATEPART(dw, dh_excesso_velocidade)
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Segunda-feira'
    WHEN 3 THEN 'Terça-feira'
    WHEN 4 THEN 'Quarta-feira'
    WHEN 5 THEN 'Quinta-feira'
    WHEN 6 THEN 'Sexta-feira'
    WHEN 7 THEN 'Sábado'
END AS 'Dia da semana'
FROM CLIENTE_PRUMO.relatorio.vw_excesso_velocidade AS A
LEFT JOIN CLIENTE_PRUMO.relatorio.ibutton_ok AS B ON A.cd_placa = B.cd_placa AND A.dh_excesso_velocidade BETWEEN B.dh_ini_mov AND B.dh_ult_mov
WHERE nu_velocidade >= 88
and hr_duracao >= '00:00:30'
)  a
where CAST(a.dh_excesso_velocidade AS DATE) BETWEEN CAST(dateadd(day,-7,GETDATE()) AS DATE) AND CAST(dateadd(day,-1,GETDATE()) AS DATE)
and nm_colaborador <> ''
AND excesso_tratado <> 'AVALIAR'
group by a.nm_colaborador, a.ds_obra
order by COUNT(*) desc
""" 

df_excesso = pd.read_sql(query1, conn)
df_agrupado = pd.read_sql(query2, conn)

excel_file = 'excesso_velocidade.xlsx'
df_excesso.to_excel(excel_file, index=False)

# Configurações de email
from_email = '' 
to_emails = ['','','','','','','','','', '', '']  # Substitua pelo endereço de e-mail do destinatário
subject = 'PRUMO :: Excesso de Velocidade'

# Configuração do servidor SMTP do Outlook
smtp_server = 'smtp.office365.com'
smtp_port = 587
smtp_username = '' 
smtp_password = ''  

# Criação da mensagem
message = MIMEMultipart()
message['From'] = from_email
message['To'] = ', '.join(to_emails)
message['Subject'] = subject

style = """
<style>
  table {
    border: 1px solid black;
    background-color: white;
    width: 80%;
    text-align: center;
    border-collapse: collapse;
  }
  table td, table th {
    border: 1px solid #AAAAAA;
    padding: 3px 2px;
    white-space: nowrap; 
  }
  table tbody td {
    font-size: 13px;
  }
  table thead {
    background: #ED7D31;
    border-bottom: 2px solid #444444;
  }
  table thead th {
    font-size: 13px;
    font-weight: bold;
    color: #FFFFFF;
    border-left: 2px solid #D0E4F5;
  }
  table thead th:first-child {
    border-left: none;
  }
  .th{
    color: black;
    font-weight: bold;
  }
  .inc{
    color: #3B90CE;
    font-weight: bold;
  }
  </style>
"""

# Corpo do e-mail em HTML
body = f"""
<html>
  <body>
  
    {style}
  
    <p>Olá,</p>
    <p>Segue em anexo o analitico semanal de excesso de velocidade e abaixo os 20 colaboradores que tiveram mais excessos na semana:<br><br>
    Para mais informações acesse o relatório <a href = ''>PRUMO - Diário - Relatório de Rastreamento e Excesso de Velocidade</a>
    </p>
    {df_agrupado.to_html(index=False)}
    <br>
    Atenciosamente,<br>
    <br>

  </body>
</html>
"""
message.attach(MIMEText(body, 'html'))

# Anexa o arquivo Excel à mensagem
with open(excel_file, 'rb') as file:
    attachment = MIMEApplication(file.read(), Name=excel_file)
    attachment['Content-Disposition'] = f'attachment; filename={excel_file}'
    message.attach(attachment)

# Conecta-se ao servidor SMTP do Outlook
server = smtplib.SMTP(smtp_server, smtp_port)
server.starttls()
server.login(smtp_username, smtp_password)

# Envia a mensagem
server.sendmail(from_email, to_emails, message.as_string())

# Fecha a conexão com o servidor SMTP
server.quit()

# Limpa o arquivo Excel após o envio
import os
os.remove(excel_file)


print("Consulta concluída. Resultado Geral enviado por email.")


# df_emails = pd.read_excel('emails_prumo.xlsx')
# e-mails lidos via Query
query_mails = "SELECT * FROM CLIENTE_PRUMO.origem.email_obra"
df_emails = pd.read_sql(query_mails, conn)
df_emails.columns = ['CENTRO_DE_CUSTO','REGIONAL', 'EMAIL1', 'GERENTE', 'EMAIL2',
                     'TRANSPORTE','EMAIL3']


# Fecha a conexão com o banco de dados
conn.dispose()
#conn.close()


lista_emails = []

for index, row in df_emails.iterrows():
    obra = row['CENTRO_DE_CUSTO']
    emails = [row['EMAIL1'], row['EMAIL2'], row['EMAIL3']]

    for email in emails:
        if obra in [item['obra'] for item in lista_emails]:
            # Se a obra já existe na lista, verifica se o e-mail já existe
            if email not in [item['email'] for item in lista_emails if item['obra'] == obra]:
                lista_emails.append({'obra': obra, 'email': email})
        else:
            # Se a obra não existe na lista, adiciona a obra e o e-mail
            lista_emails.append({'obra': obra, 'email': email})

lista_emails = [item for item in lista_emails if not pd.isna(item['email'])]

df_lista_email = pd.DataFrame(lista_emails)

# Agrupar e-mails por obra, separando por vírgula
df_lista_email_final = df_lista_email.groupby('obra')['email'].agg(lambda x: ';'.join(x)).reset_index()

# Renomear colunas
df_lista_email_final.columns = ['obra', 'emails']


# Iterar sobre as linhas do DataFrame
for index, row in df_lista_email_final.iterrows():
    nome_obra = row['obra']
    lista_emails = row['emails']  # Separar a lista de e-mails por vírgula
    
    # Agora você pode realizar o seu processo para cada obra
    print(f"Nome da obra: {nome_obra}")
    print(f"Lista de e-mails:{lista_emails}")
    
    
    # Consulta SQL 1
    query1 = f"""
    SELECT
    nm_colaborador
    ,cd_placa
    ,ds_obra
    ,id_rastreador
    ,id_ibutton
    ,dh_excesso_velocidade
    ,hr_duracao
    ,fl_distancia
    ,nu_velocidade
    ,ds_endereco
    ,nm_cidade
    ,nm_uf
    ,ds_categoria
    ,Dia_da_semana
    FROM 
    (
    SELECT
      IIF(a.nm_colaborador is null or a.nm_colaborador = '',B.nm_colaborador,a.nm_colaborador) as nm_colaborador,
      a.cd_placa,
      a.id_rastreador,
      IIF(a.id_ibutton is null or a.id_ibutton = '',B.cd_ibutton,a.id_ibutton) as id_ibutton,
      a.dh_excesso_velocidade,
      a.hr_duracao,
      a.fl_distancia,
      a.nu_velocidade,
      a.ds_endereco,
      a.nm_cidade,
      a.nm_uf,
      a.ds_categoria,
      a.ds_obra,
      case
        when (ds_categoria LIKE '%LEVE%' OR ds_categoria LIKE '%PICKUP MEDIA%') AND nu_velocidade >= 121 THEN 'EXCESSO'
        when nu_velocidade >= 88 AND ds_categoria IN (SELECT DISTINCT(ds_categoria) FROM CLIENTE_PRUMO.relatorio.implantacao_pbi_historico WHERE (ds_categoria NOT LIKE '%LEVE%' AND ds_categoria NOT LIKE '%PICKUP MEDIA%')) THEN 'EXCESSO'
        else 'AVALIAR'
      end as excesso_tratado,
      CASE DATEPART(dw, dh_excesso_velocidade)
        WHEN 1 THEN 'Domingo'
        WHEN 2 THEN 'Segunda-feira'
        WHEN 3 THEN 'Terça-feira'
        WHEN 4 THEN 'Quarta-feira'
        WHEN 5 THEN 'Quinta-feira'
        WHEN 6 THEN 'Sexta-feira'
        WHEN 7 THEN 'Sábado'
      END AS 'Dia_da_semana'
    FROM CLIENTE_PRUMO.relatorio.vw_excesso_velocidade AS A
    LEFT JOIN CLIENTE_PRUMO.relatorio.ibutton_ok AS B ON A.cd_placa = B.cd_placa AND A.dh_excesso_velocidade BETWEEN B.dh_ini_mov AND B.dh_ult_mov
    WHERE nu_velocidade >= 88
    and CAST(a.dh_excesso_velocidade AS DATE) BETWEEN CAST(dateadd(day,-7,GETDATE()) AS DATE) AND CAST(dateadd(day,-1,GETDATE()) AS DATE)
    ) A 
    WHERE excesso_tratado <> 'AVALIAR'
    AND DS_OBRA = '{nome_obra}'
    ORDER BY A.dh_excesso_velocidade DESC
    """ 

    # Consulta SQL 2
    query2 = f"""
    SELECT top 20 a.nm_colaborador,a.ds_obra, COUNT(*) as qtd_excesso_semana
    FROM 
    (
    SELECT
      a.cd_ano_mes
    ,a.id_colaborador
    ,IIF(a.nm_colaborador is null or a.nm_colaborador = '',B.nm_colaborador,a.nm_colaborador) as nm_colaborador
    ,A.ds_obra
    ,a.cd_placa
    ,a.id_rastreador
    ,IIF(a.id_ibutton is null or a.id_ibutton = '',B.cd_ibutton,a.id_ibutton) as id_ibutton
    ,a.dh_excesso_velocidade
    ,a.hr_duracao
    ,a.fl_distancia
    ,a.nu_velocidade
    ,a.ds_endereco
    ,a.nm_cidade
    ,a.nm_uf
    ,a.ds_categoria
    ,a.cd_filial
    ,a.centrodecusto
    ,a.id_superior
    ,a.nm_gestor
    , convert(time,dh_excesso_velocidade) as hora,
    case
        when (ds_categoria LIKE '%LEVE%' OR ds_categoria LIKE '%PICKUP MEDIA%') AND nu_velocidade >= 121 THEN 'EXCESSO'
        when nu_velocidade >= 88 AND ds_categoria IN (SELECT DISTINCT(ds_categoria) FROM CLIENTE_PRUMO.relatorio.implantacao_pbi_historico WHERE (ds_categoria NOT LIKE '%LEVE%' AND ds_categoria NOT LIKE '%PICKUP MEDIA%')) THEN 'EXCESSO'
        else 'AVALIAR'
    end as excesso_tratado,
    case
        when convert(time,dh_excesso_velocidade) >= '03:01' and convert(time,dh_excesso_velocidade) <= '6:00' then '03:00 - 06:00'
        when convert(time,dh_excesso_velocidade) >= '6:01' and convert(time,dh_excesso_velocidade) <= '9:00' then '06:00 - 09:00'
        when convert(time,dh_excesso_velocidade) >= '9:01' and convert(time,dh_excesso_velocidade) <= '12:00' then '09:00 - 12:00'
        when convert(time,dh_excesso_velocidade) >= '12:00' and convert(time,dh_excesso_velocidade) <= '15:00' then '12:00 - 15:00'
        when convert(time,dh_excesso_velocidade) >= '15:01' and convert(time,dh_excesso_velocidade) <= '18:00' then '15:00 - 18:00'
        when convert(time,dh_excesso_velocidade) >= '18:01' and convert(time,dh_excesso_velocidade) <= '21:00' then '18:00 - 21:00'
        when convert(time,dh_excesso_velocidade) >= '21:01' and convert(time,dh_excesso_velocidade) <= '23:59' then '21:00 - 00:00'
        when convert(time,dh_excesso_velocidade) > ='00:00' and convert(time,dh_excesso_velocidade) <= '3:00' then '00:00 - 03:00'
    else '0'
    end as faixa_hora
    , CASE DATEPART(dw, dh_excesso_velocidade)
        WHEN 1 THEN 'Domingo'
        WHEN 2 THEN 'Segunda-feira'
        WHEN 3 THEN 'Terça-feira'
        WHEN 4 THEN 'Quarta-feira'
        WHEN 5 THEN 'Quinta-feira'
        WHEN 6 THEN 'Sexta-feira'
        WHEN 7 THEN 'Sábado'
    END AS 'Dia da semana'
    FROM CLIENTE_PRUMO.relatorio.vw_excesso_velocidade AS A
    LEFT JOIN CLIENTE_PRUMO.relatorio.ibutton_ok AS B ON A.cd_placa = B.cd_placa AND A.dh_excesso_velocidade BETWEEN B.dh_ini_mov AND B.dh_ult_mov
    WHERE nu_velocidade >= 88
    )  a
    where CAST(a.dh_excesso_velocidade AS DATE) BETWEEN CAST(dateadd(day,-7,GETDATE()) AS DATE) AND CAST(dateadd(day,-1,GETDATE()) AS DATE)
    and nm_colaborador <> ''
    AND excesso_tratado <> 'AVALIAR'
    AND a.DS_OBRA = '{nome_obra}'
    group by a.nm_colaborador, a.ds_obra
    order by COUNT(*) desc
    """ 
    conn = alchConn('CLIENTE_PRUMO')
    # conn = pyodbc.connect(conn_str)
    df_excesso = pd.read_sql(query1, conn)
    df_agrupado = pd.read_sql(query2, conn)

    if not df_excesso.empty:
        excel_file = f"excesso_velocidade_{nome_obra}.xlsx"
        df_excesso.to_excel(excel_file, index=False)
    
        # Configurações de email
        from_email = '' 
        to_emails = [lista_emails]
        to_emails = [email.strip() for email in lista_emails.replace(';', ',').split(',')]# Substitua pelo endereço de e-mail do destinatário
        subject = f"PRUMO :: Excesso de Velocidade - {nome_obra}"
        
        # Configuração do servidor SMTP do Outlook
        smtp_server = 'smtp.office365.com'
        smtp_port = 587
        smtp_username = '' 
        smtp_password = ''  
    
        # Criação da mensagem
        message = MIMEMultipart()
        message['From'] = from_email
        message['To'] = ', '.join(to_emails)
        message['Subject'] = subject
    
        style = """
        <style>
          table {
            border: 1px solid black;
            background-color: white;
            width: 80%;
            text-align: center;
            border-collapse: collapse;
          }
          table td, table th {
            border: 1px solid #AAAAAA;
            padding: 3px 2px;
            white-space: nowrap; 
          }
          table tbody td {
            font-size: 13px;
          }
          table thead {
            background: #ED7D31;
            border-bottom: 2px solid #444444;
          }
          table thead th {
            font-size: 13px;
            font-weight: bold;
            color: #FFFFFF;
            border-left: 2px solid #D0E4F5;
          }
          table thead th:first-child {
            border-left: none;
          }
          .th{
            color: black;
            font-weight: bold;
          }
          .inc{
            color: #3B90CE;
            font-weight: bold;
          }
          </style>
        """
    
        # Corpo do e-mail em HTML
        body = f"""
        <html>
          <body>
          
            {style}
          
            <p>Olá,</p>
            <p>Segue em anexo o analitico semanal de excesso de velocidade e abaixo os 20 colaboradores que tiveram mais excessos na semana:<br><br>
            Para mais informações acesse o relatório <a href = ''>PRUMO - Diário - Relatório de Rastreamento e Excesso de Velocidade</a>
            </p>
            {df_agrupado.to_html(index=False)}
            <br>
            Atenciosamente,<br>
            <br>

          </body>
        </html>
        """
        message.attach(MIMEText(body, 'html'))
    
        # Anexa o arquivo Excel à mensagem
        with open(excel_file, 'rb') as file:
            attachment = MIMEApplication(file.read(), Name=excel_file)
            attachment['Content-Disposition'] = f'attachment; filename={excel_file}'
            message.attach(attachment)
    
        # Conecta-se ao servidor SMTP do Outlook
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
    
        # Envia a mensagem
        server.sendmail(from_email, to_emails, message.as_string())
    
        # Fecha a conexão com o servidor SMTP
        server.quit()
    
        os.remove(excel_file)
    
        # Fecha a conexão com o banco de dados
        conn.dispose()
        #conn.close()
        print("\n")  # Apenas para separar as saídas no console

print("Emails enviados")
