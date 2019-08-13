import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime
import time

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


if __name__ == '__main__':

    response = urllib2.urlopen(firebaseDBURL)
    raw_data = response.read()
    data = json.loads(raw_data)

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
    persons = session.query(LaneData)
    for person in persons:
        print(person.distanceLeft, person.distanceRight)


