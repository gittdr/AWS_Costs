import boto3
import json
import pyodbc
import os
from dotenv import load_dotenv
from datetime import datetime, date

load_dotenv()

ce = boto3.client('ce')
start = date.today().replace(month=1, day=1).isoformat()
end = date.today().isoformat()

def get_connection():
    """
    Establece la conexion con la base de datos y devuelve un objeto de conexión a SQL usando variables de entorno
    """
    try:
        return pyodbc.connect(
            f'DRIVER={os.getenv("BD_DRIVER")};'
            f'SERVER={os.getenv("BD_SERVER")};'
            f'DATABASE={os.getenv("BD_DATABASE")};'
            f'UID={os.getenv("BD_USERNAME")};'
            f'PWD={os.getenv("BD_PSWRD")}'
        )
    except Exception as e:
        print("Error al conectar con SQL Server: ", e)
        raise
    

def save_to_database(data, query):
    """
    Save the fetched data to a database.
    :param data: The data to save.
    """
    conn = None
    try:
        # Establecer conexión a SQL Server
        conn = get_connection()
        cursor = conn.cursor()
        # Insertar los registros
        cursor.execute(F"TRUNCATE TABLE {os.getenv("TABLE")};")
        for registro in data:
            cursor.execute(query, tuple(registro.values()))
        # Confirmar cambios
        conn.commit()
        print("Datos insertados correctamente en SQL Server.")

    except Exception as e:
        print(f"Error al guardar en SQL Server con query {query}:\n{e}")

    finally:
        # Cerrar conexión
        conn.close()

def obtain_aws_cost():
    cost = ce.get_cost_and_usage(
        TimePeriod={
            'Start': start,
            'End': end
        },
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[
            {
                'Type':'DIMENSION',
                'Key':'SERVICE'
            }
        ]
    )
    results = cost.get('ResultsByTime')

    final_cost = []

    for monthly_result in results:
        period = datetime.strptime(monthly_result.get('TimePeriod').get('Start'), "%Y-%m-%d")
        period_date = period.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        for group in monthly_result.get('Groups'):
            data_group = {
                'TimePeriod' : period_date,
                'Service': group.get('Keys')[0],
                'Amount' : group.get('Metrics').get('UnblendedCost').get('Amount'),
                'Unit' : group.get('Metrics').get('UnblendedCost').get('Unit')
            }
            final_cost.append(data_group)
    return final_cost

query = os.getenv('QUERY')
        
save_to_database(obtain_aws_cost(), query)
