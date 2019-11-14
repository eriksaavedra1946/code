import boto3
import csv

def csv_reader(event, context):
    region='us-east-1'
    recList=[]
    
    try:            
        s3=boto3.client('s3')            
        dyndb = boto3.client('dynamodb', region_name=region)
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        s3_file_name = event['Records'][0]['s3']['object']['key']
        confile= s3.get_object(Bucket=bucket_name,Key=s3_file_name)
        recList = confile['Body'].read().decode('latin-1').split('\n')
        firstrecord=True
        csv_reader = csv.reader(recList, delimiter=';')
        for row in csv_reader:
            if (firstrecord):
                firstrecord=False
                continue
            Id = row[0]
            Year = row[1].replace(',','') if row[1] else '-'
            Value = row[2].replace(',','') if row[2] else '-'
            Cod = row[3].replace(',','') if row[3] else '-'
            Codigo = row[4].replace(',','') if row[4] else '-'
            AgrupadorCluster = row[5].replace(',','') if row[5] else '-'
            Agrupador = row[6].replace(',','') if row[6] else '-'
            Sistema = row[7].replace(',','') if row[7] else '-'
            Marca = row[8].replace(',','') if row[8] else '-'
            Clase = row[9].replace(',','') if row[9] else '-'
            ReferenciaGeneral = row[10].replace(',','') if row[10] else '-'
            Referencia1 = row[11].replace(',','') if row[11] else '-'
            Referencia2 = row[12].replace(',','') if row[12] else '-'
            Referencia3 = row[13].replace(',','') if row[13] else '-'
            Peso = row[14].replace(',','') if row[14] else '-'
            IdServicio = row[15].replace(',','') if row[15] else '-'
            Servicio = row[16].replace(',','') if row[16] else '-'
            Bcpp = row[17].replace(',','') if row[17] else '-'
            Importado = row[18].replace(',','') if row[18] else '-'
            Potencia = row[19].replace(',','') if row[19] else '-'
            TipoCaja = row[20].replace(',','') if row[20] else '-'
            Cilindraje = row[21].replace(',','') if row[21] else '-'
            Nacionalidad = row[22].replace(',','') if row[22] else '-'
            CapacidadPasajeros = row[23].replace(',','') if row[23] else '-'
            CapacidadCarga = row[24].replace(',','') if row[24] else '-'
            Puertas = row[25].replace(',','') if row[25] else '-'
            AireAcondicionado = row[26].replace(',','') if row[26] else '-'
            Ejes = row[27].replace(',','') if row[27] else '-'
            Estado = row[28].replace(',','') if row[28] else '-'
            Combustible = row[29].replace(',','') if row[29] else '-'
            Transmision = row[30].replace(',','') if row[30] else '-'
            Um = row[31].replace(',','') if row[31] else '-'
            PesoCategoria = row[32].replace(',','') if row[32] else '-'
            Restricted = row[33].replace(',','') if row[33] else '-'
            CombustibleCode = row[34].replace(',','') if row[34] else '-'
            TransmissionCode = row[35].replace(',','') if row[35] else '-'
            VehicleInclude = row[36].replace(',','') if row[36] else '-'
            TextSearch = row[37].replace(',','') if row[37] else '-'
            response = dyndb.put_item(
                TableName='table-fasecolda-dev',
                Item={
                'Id'  :  {'S' :str(Id)},
                'Year'  :  {'S' :str(Year)},
                'Value'  :  {'S' :str(Value)},
                'Cod'  :  {'S' :str(Cod)},
                'Codigo'  :  {'S' :str(Codigo)},
                'AgrupadorCluster'  :  {'S' :str(AgrupadorCluster)},
                'Agrupador'  :  {'S' :str(Agrupador)},
                'Sistema'  :  {'S' :str(Sistema)},
                'Marca'  :  {'S' :str(Marca)},
                'Clase'  :  {'S' :str(Clase)},
                'ReferenciaGeneral'  :  {'S' :str(ReferenciaGeneral)},
                'Referencia1'  :  {'S' :str(Referencia1)},
                'Referencia2'  :  {'S' :str(Referencia2)},
                'Referencia3'  :  {'S' :str(Referencia3)},
                'Peso'  :  {'S' :str(Peso)},
                'IdServicio'  :  {'S' :str(IdServicio)},
                'Servicio'  :  {'S' :str(Servicio)},
                'Bcpp'  :  {'S' :str(Bcpp)},
                'Importado'  :  {'S' :str(Importado)},
                'Potencia'  :  {'S' :str(Potencia)},
                'TipoCaja'  :  {'S' :str(TipoCaja)},
                'Cilindraje'  :  {'S' :str(Cilindraje)},
                'Nacionalidad'  :  {'S' :str(Nacionalidad)},
                'CapacidadPasajeros'  :  {'S' :str(CapacidadPasajeros)},
                'CapacidadCarga'  :  {'S' :str(CapacidadCarga)},
                'Puertas'  :  {'S' :str(Puertas)},
                'AireAcondicionado'  :  {'S' :str(AireAcondicionado)},
                'Ejes'  :  {'S' :str(Ejes)},
                'Estado'  :  {'S' :str(Estado)},
                'Combustible'  :  {'S' :str(Combustible)},
                'Transmision'  :  {'S' :str(Transmision)},
                'Um'  :  {'S' :str(Um)},
                'PesoCategoria'  :  {'S' :str(PesoCategoria)},
                'Restricted'  :  {'S' :str(Restricted)},
                'CombustibleCode'  :  {'S' :str(CombustibleCode)},
                'TransmissionCode'  :  {'S' :str(TransmissionCode)},
                'VehicleInclude'  :  {'S' :str(VehicleInclude)},
                'TextSearch'  :  {'S' :str(TextSearch)}

                }
            )
        print('Put succeeded:')
    except Exception, e:
        print (str(e))
