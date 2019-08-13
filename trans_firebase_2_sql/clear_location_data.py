import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import time

def getData(db):
    docs = db.where(u'createdTime', u'>', 0).order_by(u'createdTime').get()
    #order_by(
    #            u'name', direction=firestore.Query.DESCENDING).limit(3)
#    for doc in docs:
#        print(u' {}'.format(doc.to_dict()))
            
    return docs


def clearData(db):
    docs = db.where(u'createdTime', u'<', int(round(time.time() * 1000))).get()
    for doc in docs:
        db.document(doc.id).delete()


def addData(db):
   data = {
           u'uid':0000,
           u'createdTime':0,
           u'lat':0,
           u'lon':0,
           u'speed':0
           }
   db.add(data)


if __name__ == '__main__':
    # Use the application default credentials
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, {
      'projectId': 'pedestriandetectdemo',
      })

    db = firestore.client()
    location_ref = db.collection(u'location')
    clearData(location_ref)

