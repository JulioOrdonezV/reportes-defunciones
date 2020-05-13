import requests
from pymongo import MongoClient, DESCENDING, ASCENDING
from pymongo.write_concern import WriteConcern
from pymongo.errors import DuplicateKeyError, OperationFailure
from pymongo.read_concern import ReadConcern

MONGODB_URI = "mongodb+srv://usuario_citas_defunciones:libro_horse_blau_key@cluster0-zaajb.mongodb.net/test"


def load_data():
    db = MongoClient(
        MONGODB_URI,
        maxPoolSize=50,
        wtimeout=5000
        )['defuncionesCR']

    citas = []
    with open("data/def_abril2020_27_30/MOVWEBDEF.txt",'r', encoding='latin_1') as data_file:
        for line in data_file:
            citas.append(line[2:14])


    for numero_cita in citas:
        data = {"identificacion": numero_cita, "tiponacionalidad": 0 }
        response_json = requests.get("https://www.consulta.tse.go.cr/appcdi/home/WSDefuncion", data=data).json()
        if response_json.get('personaDifunta').get('citaDefuncion') :
            db.citas_defunciones.insert_one(response_json)

if __name__ == '__main__':
    load_data()
