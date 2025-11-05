import boto3
import uuid
import os
import json   # ✅ agregado para parsear el body

def lambda_handler(event, context):
    # Entrada (json)
    print(event)

    # ✅ Manejar body tanto si viene como string o como dict
    body = event.get("body")
    if isinstance(body, str):
        data = json.loads(body)
    else:
        data = body

    tenant_id = data['tenant_id']     # ✅ antes event['body']['tenant_id']
    texto = data['texto']             # ✅ antes event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]

    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }

    # ✅ Guardar en DynamoDB (igual que antes)
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)

    # ✅ NUEVO: guardar en S3 para ingesta push
    bucket_name = os.environ["S3_BUCKET_INGESTA"]
    s3 = boto3.client('s3')
    archivo_s3 = f"{tenant_id}/{uuidv1}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=archivo_s3,
        Body=json.dumps(comentario),
        ContentType='application/json'
    )

    print(f"Archivo guardado en S3 -> {bucket_name}/{archivo_s3}")

    # Salida (json)
    print(comentario)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'comentario': comentario,
            'uuid': uuidv1,
            's3': f"{bucket_name}/{archivo_s3}"
        })
    }
