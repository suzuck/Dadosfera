import boto3
from boto3.s3.transfer import TransferConfig
import json
import pandas as pd
import db_instruction
from io import BytesIO

# s3 parameters
S3_ID = 'AKIASHEMMY5CBBUIZU5T'
S3_PASS = 'A9xX7KQ/JUMxh42F589IDxhuAITlP6+0zlPZRAjD'
S3_BUCKET = "dados-fera"
S3_TRIP = "raw/trip"
S3_VENDOR = "raw/vendor"
S3_PAYMENT = "raw/payment"

def read_folder(s3_files):
    zip_files = []  
    print('Criando Array dos Json para input em banco...')
    for f in s3_files["Contents"]:
        if f["Key"].upper().find(".JSON") != -1 or f["Key"].upper().find(".CSV") != -1:
            f_name = f["Key"].split('/')[len(f["Key"].split('/'))-1]
            diretorio = f["Key"][0:f["Key"].find(f_name)] 
            zip_files.append([f_name,diretorio])
    
    return zip_files

#Recria ou cria tabela TB_TRIP
db_instruction.create_tb_trip()
print('Tabela TB_TRIP criada')

#Captura arquivos da pasta TRIP
s3 = boto3.client('s3',aws_access_key_id = S3_ID , aws_secret_access_key = S3_PASS)
s3_files = s3.list_objects(Bucket=S3_BUCKET,Prefix=S3_TRIP)

for f in read_folder(s3_files):
    dev_resource = boto3.resource('s3',aws_access_key_id = S3_ID , aws_secret_access_key = S3_PASS)
    obj = dev_resource.Object(bucket_name=S3_BUCKET, key=(f[1]+f[0]))
    
    print("open zip_obj = ",obj)

    file = obj.get()['Body'].read().decode('utf-8')

    file = '['+file.replace('\n',',')
    
    file = file[0:len(file)-1] +']'

    file = json.loads(file)

    fl = pd.DataFrame(file)
    
    db_instruction.insert('TB_TRIP',fl)

print('Input de dados finalizado TRIP')


#Recria ou cria tabela TB_VENDOR
db_instruction.create_tb_vendor()
print('Tabela TB_VENDOR criada')

#Captura arquivos da pasta vendor
s3 = boto3.client('s3',aws_access_key_id = S3_ID , aws_secret_access_key = S3_PASS)
s3_files = s3.list_objects(Bucket=S3_BUCKET,Prefix=S3_VENDOR)

for f in read_folder(s3_files):
    dev_resource = boto3.resource('s3',aws_access_key_id = S3_ID , aws_secret_access_key = S3_PASS)
    obj = dev_resource.Object(bucket_name=S3_BUCKET, key=(f[1]+f[0]))
    
    print("open zip_obj = ",obj)

    with BytesIO(obj.get()['Body'].read()) as bio:
        df = pd.read_csv(bio)
    
    db_instruction.insert('TB_VENDOR',df)

print('Input de dados finalizado VENDOR')


#Recria ou cria tabela TB_PAYMENT
db_instruction.create_tb_payment()
print('Tabela TB_PAYMENT criada')

#Captura arquivos da pasta payment
s3 = boto3.client('s3',aws_access_key_id = S3_ID , aws_secret_access_key = S3_PASS)
s3_files = s3.list_objects(Bucket=S3_BUCKET,Prefix=S3_PAYMENT)

for f in read_folder(s3_files):
    dev_resource = boto3.resource('s3',aws_access_key_id = S3_ID , aws_secret_access_key = S3_PASS)
    obj = dev_resource.Object(bucket_name=S3_BUCKET, key=(f[1]+f[0]))
    
    print("open zip_obj = ",obj)

    with BytesIO(obj.get()['Body'].read()) as bio:
        df = pd.read_csv(bio)
    
    #Tornando linha como como header
    df.columns = df.iloc[0]
    df = df[1:]

    db_instruction.insert('TB_PAYMENT',df)

print('Input de dados finalizado PAYMENT')