import requests
from settings import MONGO_USER, MONGO_PASSW
from pymongo import MongoClient, DESCENDING, ASCENDING
from pymongo.write_concern import WriteConcern
from pymongo.errors import DuplicateKeyError, OperationFailure
from pymongo.read_concern import ReadConcern

MONGODB_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASSW}@cluster0-zaajb.mongodb.net/test"

db = MongoClient(
    MONGODB_URI,
    maxPoolSize=50,
    wtimeout=5000
    )['defuncionesCR']

def load_data():

    citas = []
    with open("data/def_febrero2020_24_29/MOVWEBDEF.txt",'r', encoding='latin_1') as data_file:
        for line in data_file:
            citas.append(line[2:14])

    for numero_cita in citas:
        data = {"identificacion": numero_cita, "tiponacionalidad": 0 }
        response_json = requests.get("https://www.consulta.tse.go.cr/appcdi/home/WSDefuncion", data=data).json()
        try:
            if response_json.get('personaDifunta').get('citaDefuncion'):
                    db.citas_defunciones_febrero.insert_one(response_json)
        except:
            print(data)
            print(response_json)

if __name__ == '__main__':
    load_data()
