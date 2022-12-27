Dados Fera

    Para execuitar este desafio foi necessário executar os pontos abaixo

    1-Criar instancia de banco RD Cloud PostgreSQL, para armazenamento dos dados
    2-Criar maquina EC2 para executar scripts Python de Etl
    3-Criar bucket S3 para recebimento de arquivos e realizar input na base de dados

📦 Estrutura de pastas
 
    Dadosfera
        ├───Excell
        │       Extratificação.xlsx --> Planilha contendo extratifições e graficos dos desafios)
        │
        └───Script
                db_instruction.py --> Script Py com metodos e dados de acesso a banco
                main.py --> Script main com processo de ingestão de dados

📋 Pré-requisitos
    
    Antes de mais nada recomendamos seguir os passos abaixo:
    
    1-Fazer uso do pg-admin ou qualquer outro SGBD que utilize o motor do PostgreSQL, pois iremos utilizar uma instancia RDS com motor PostgreSQL 
    2-Uso do aplicativo Cyberduck para acesso ao S3 bucket facilitando gerencia de dados do S3
    3-Utilizaçào do python3

⚙️ Configurando ambiente
    
    Quanto acesso ao S3 Bucket Segue dados de acesso
        ID: AKIASHEMMY5CBBUIZU5T
        Key: A9xX7KQ/JUMxh42F589IDxhuAITlP6+0zlPZRAjD

    Acesso Banco de Dados RDS PostgreSQL
        dbname: db1 
        host: database-2.chaeu1yz7n3h.us-east-1.rds.amazonaws.com
        port: 5432 
        user: postgres 
        password: 123central

🔧 Instalação

    O Projeto utiliza as libs psycopg2, boto3 e pandas
    Para instalar todas é necessário executar o comando a baixo

    pip install psycopg2 boto3 pandas


🚀 Começando
    
    Todo o processo de ingestão é automatizado e para iniciar o mesmo basta executar o comando 
    
    python main.py

    O Script main.py inicialmente cria as tabelas TB_TRIP, TB_VENDOR e TB_PAYMENT, através das funções create_tb_trip, create_tb_vendor e create_tb_payment, todas estas funções estão contidas no arquivo db_instructions.py

    Logo em seguida efetua a leitura dos arquivos relativos as trip`s contidos na pasta do S3, através da função read_folder presente no arquivo main.py

    Apos a leitura é reaizado o input de dados através da função insert presente no arquivo bd_instruction.py

🛠️ Qry de extração

    Qry extração média corridas até 2 passageiros

        AVG é a função responsavel por calcular a média

        select avg(trip_distance), passenger_count from public.tb_trip
        where passenger_count <= 2 
        group by passenger_count

    Qry extração Vendors por faturamento
        
        SUM é a função responsavel por calcular a soma
        
        select v.name, sum(t.total_amount)::NUMERIC::MONEY FROM public.tb_trip t
        left join public.tb_vendor v on t.vendor_id = v.vendor_id
        group by v.name

    QRY extração Historiograma 4 anos

        Necessário subselect para calcular dados mensais, caso onctrario teriamos agrupamentos diarios

        SELECT NAME, DT, payment_type, SUM(total_amount)::NUMERIC::MONEY
        from(
            select 
            to_char(T.pickup_datetime,'MM/YYYY') as DT, 
            T.pickup_datetime,
            T.payment_type, 
            T.total_amount,
            V.NAME
            from public.tb_trip T
            LEFT JOIN public.tb_vendor V ON T.VENDOR_ID = V.VENDOR_ID 
        ) BS
        GROUP BY DT,payment_type, NAME

    QRY extração Historiograma gorjetas 3 ultimos meses 2011

        SELECT DT, SUM(tip_amount)::NUMERIC::MONEY
        from(
            select 
            TO_CHAR(T.pickup_datetime::DATE,'DD/MM/YYYY') as DT, 
            T.tip_amount
            from public.tb_trip T
            WHERE 
            EXTRACT(MONTH FROM T.pickup_datetime) >= 10 AND
            EXTRACT(MONTH FROM T.pickup_datetime) <= 12 AND
            EXTRACT(YEAR FROM T.pickup_datetime) = 2011
        ) BS
        GROUP BY DT


    QRY calculo media de tempo corridas por semana
        
        SELECT to_char(AVG(TEMPO),'HH24:MI:SS'),WEEK, NAME FROM (
            select 
                dropoff_datetime - T.pickup_datetime AS TEMPO,
                (
                    case
                        when TO_CHAR(dropoff_datetime,'D') = '1' then 'DOM'
                        when TO_CHAR(dropoff_datetime,'D') = '2' then 'SEG'
                        when TO_CHAR(dropoff_datetime,'D') = '3' then 'TER'
                        when TO_CHAR(dropoff_datetime,'D') = '4' then 'QUA'
                        when TO_CHAR(dropoff_datetime,'D') = '5' then 'QUI'
                        when TO_CHAR(dropoff_datetime,'D') = '6' then 'SEX'
                        when TO_CHAR(dropoff_datetime,'D') = '7' then 'SAB'
                    END
                ) AS WEEK,
                V.NAME
                from public.tb_trip T
                LEFT JOIN public.tb_vendor V ON T.VENDOR_ID = V.VENDOR_ID 
        ) BS
        GROUP BY WEEK, NAME 
        LIMIT 100

    QRY Extração latitute e longitude 2010

        select pickup_latitude||','||pickup_longitude
        from public.tb_trip
        where extract(year from dropoff_datetime) = 2010