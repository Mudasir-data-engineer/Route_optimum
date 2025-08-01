# db_schema/models/user.py
from sqlalchemy import Column, Integer, String
from .base import Base  # relative import

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    name = Column(String)
