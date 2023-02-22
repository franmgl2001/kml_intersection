import kml2geojson
import json
from shapely.geometry import Polygon
import mysql.connector
import boto3

bucket = 'geojsonfastfarm'
s3 = boto3.client('s3')

def get_secret():
    secret_name = ""
    region_name = "us-west-2"
    secretsession = boto3.session.Session()
    secretclient = secretsession.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_response = secretclient.get_secret_value(
        SecretId=secret_name
    )
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    return json.loads(secret)


def getkmldata(kml):
    coors = []
    names = []
    geojson = kml2geojson.convert(kml)
    for i in range(len(geojson[0]["features"])):
        coors += [geojson[0]["features"][i]["geometry"]["coordinates"][0]]
        names += [geojson[0]["features"][i]["properties"]["name"]]
    
    return[names, coors]




def getfields():
    secret = get_secret()
    HOST = secret['host']
    DB = secret['dbname']
    USER = secret['username']
    USER_PWD = secret['password']

    conn = mysql.connector.connect(host=HOST, database=DB, user=USER, password=USER_PWD)
    cursor = conn.cursor()
    producer_names = (49, 67, 72, 73, 74, 75, 101)

    q = f"""SELECT * FROM fields f WHERE is_active = 1;"""
    cursor.execute(q)
    fields = cursor.fetchall()
    conn.close()
    paths = []
    field_ids = []
    for field in fields:
        paths += [field[4]]
        field_ids += [field[0]]
    print(field_ids)
    return [paths, field_ids]


def writecoordinates():
    f = open('coordinatesall.txt', 'a')
    [paths, field_ids] = getfields()
    for i in range(len(paths)):
        data = s3.get_object(Bucket=bucket, Key=paths[i])
        contents = data['Body'].read().decode("utf-8")
        print(field_ids[i])
        print(paths[i])
        print(contents + "\n")
        f.write(str(field_ids[i])+"\n")
        f.write(paths[i]+"\n")
        f.write(contents+"\n")
    f.close
    

def checkIntersection(cord1, cord2):
    p1 = Polygon(cord1)
    p2 = Polygon(cord2)
    res = p1.intersects(p2)
    print(res)
    return res


def opencordsfile():
    f = open('coordinates4.txt', 'r')
    lines = len(f.readlines())

    f = open('coordinates4.txt', 'r')
    field_ids = []
    paths = []
    coordinates = []
    
    for i in range(int(lines/3)):
        field_ids += [f.readline().strip('\n')]
        paths += [f.readline().strip("\n")]
        coordinates += json.loads(f.readline().strip('\n'))["coordinates"]
    
    f = open('intersections_withGivenIds.txt', 'w')
    
    for i in range(len(coordinates)):
        for j in range(i,len(coordinates)):
            print(field_ids[i], field_ids[j])
            if checkIntersection(coordinates[i], coordinates[j]) and field_ids[i] != field_ids[j]:
                f.write( field_ids[i] + '-' + field_ids[j] +  '\n')
    f.close()
    print(len(coordinates))
#writecoordinates()
opencordsfile()

