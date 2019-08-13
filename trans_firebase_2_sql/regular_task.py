import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime
import time
import sched
import multiprocessing

Base = declarative_base()

import json
import urllib2

cloudSqlIp = '10.33.1.3'
firebaseDBURL = 'https://pedestriandetectdemo.firebaseio.com/Data/LaneData.json'

class LaneData(Base):
    __tablename__ = 'lane_data'
    id = Column(Integer, primary_key=True)
    userId = Column(Integer)
    distanceLeft = Column(Float)
    distanceRight = Column(Float)
    createdDate = Column(DateTime, default=datetime.datetime.utcnow)    
    def __repr__(self):
        return "<User(index='%d', distanceLeft='%f', distanceRight='%f')>" % (self.index, self.distanceLeft, self.distanceRight)


class LocationData(Base):
    __tablename__ = 'location_data'
    
    id = Column(Integer, primary_key=True)
    uid = Column(String(40))
    lat = Column(Float(precision='15,12'))
    lon = Column(Float(precision='15,12'))
    speed = Column(Float)
    createdTime = Column(DateTime, default=datetime.datetime.utcnow)    
    def __repr__(self):
        return "<User(index='%d')>" % (self.index)


def getDataFromFirebase():
    response = urllib2.urlopen(firebaseDBURL)
    raw_data = response.read()
    data = json.loads(raw_data)
    
    return data


def transFromFirestore(data):
    engine = create_engine('mysql://admin:admin@' + cloudSqlIp + ':3306/test', echo=False)
    Base.metadata.create_all(engine)
    Base.metadata.bind = engine
     
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    index, base = 0,1538811385 
    for doc in data:
        value = doc.to_dict()
        newLocation = LocationData(uid=value['uid'], lat=value['lat'], lon=value['lon'],
                speed=value['speed'])
        newLocation.createdTime = datetime.datetime.fromtimestamp(value['createdTime']/1000).strftime('%Y-%m-%d %H:%M:%S') 
        session.add(newLocation)
        index += 1

    session.commit()
#    persons = session.query(LocationData)
#    for person in persons:
#        print(person.lat)


def transformSql(data):
    engine = create_engine('mysql://admin:admin@' + cloudSqlIp + ':3306/test', echo=True)
    Base.metadata.create_all(engine)
    Base.metadata.bind = engine
     
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    index, base = 0,1538811385 
    for key, value in data.iteritems():
        newLaneData = LaneData(distanceLeft=value['distanceLeft'], distanceRight=value['distanceRight'])
        newLaneData.userId = 10001
        newLaneData.createdDate = datetime.datetime.fromtimestamp(base + index*300).strftime('%Y-%m-%d %H:%M:%S') 
        session.add(newLaneData)
        index += 1

    session.commit()
    print(time.time())
#    persons = session.query(LaneData)
#    for person in persons:
#        print(person.distanceLeft, person.distanceRight)


def clockTasks(sqlSchedule):
    data = getDataFromFirebase()
    transformSql(data)
    sqlSchedule.enter(24*60*60, 1, clockTasks, argument=(sqlSchedule,))
#    sqlSchedule.enter(6, 1, transformSql, argument=('test',))

def runClockTasks():
    sqlSchedule = sched.scheduler(time.time, time.sleep)
    sqlSchedule.enter(10, 1, clockTasks, argument=(sqlSchedule,))
    sqlSchedule.run()
#


if __name__ == '__main__':
    background_process = multiprocessing.Process(name='background_process',target=runClockTasks)
    background_process.daemon = True
    background_process.start()

