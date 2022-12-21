import boto3
from boto3.s3.transfer import TransferConfig
import json
import pandas as pd
import db_instruction

def lambda_handler(event, context):

    # s3 parameters
    S3_ID = 'AKIASHEMMY5CCHM45LL5'
    S3_PASS = 'fCOhM2mQnzbqOkCwvnO47tGVH6sibfccI/nHk5Nn'
    S3_BUCKET = "teste-dados-fera"
    S3_DIRETORIO = "raw/"
    s3 = boto3.client('s3',aws_access_key_id = S3_ID , aws_secret_access_key = S3_PASS)
    s3_files = s3.list_objects(Bucket=S3_BUCKET,Prefix=S3_DIRETORIO)
    
    def read_folder():
        zip_files = []  
        print('Criando Array dos Json para input em banco...')
        for f in s3_files["Contents"]:
            if f["Key"].upper().find(".JSON") != -1:
                f_name = f["Key"].split('/')[len(f["Key"].split('/'))-1]
                diretorio = f["Key"][0:f["Key"].find(f_name)] 
                zip_files.append([f_name,diretorio])
        
        return zip_files

    db_instruction.create_tables()
    print('Tabela TX_TRIP criada')

    for f in read_folder():
        dev_resource = boto3.resource('s3',aws_access_key_id = S3_ID , aws_secret_access_key = S3_PASS)
        obj = dev_resource.Object(bucket_name=S3_BUCKET, key=(f[1]+f[0]))
        array_correcao = []

        print("open zip_obj = ",obj)

        file = obj.get()['Body'].read().decode('utf-8')

        file = '['+file.replace('\n',',')+']'

        file = json.loads(file)

        fl = pd.DataFrame(file)
        
        db_instruction.insert('TX_TRIP',fl)

    print('Input de dados finalizado')

lambda_handler(1,1)