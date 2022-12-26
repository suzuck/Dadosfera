# Dadosfera
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